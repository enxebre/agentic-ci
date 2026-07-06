"""OTLP collector lifecycle and post-hoc metrics parsing.

Manages an OpenTelemetry Collector (otelcol-contrib) subprocess that receives
OTLP exports from Claude Code, writes them to a JSONL file, and provides
post-hoc parsing for token/cost summaries.

Falls back to a lightweight built-in Python HTTP collector when otelcol-contrib
is not installed.
"""

import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import textwrap
import time
from collections import defaultdict
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

_token_samples: list[tuple[float, int]] = []
_WINDOW_SECS = 60

MAX_BODY_SIZE = 1_048_576


# ---------------------------------------------------------------------------
# Signal-type detection for OTLP JSONL records
# ---------------------------------------------------------------------------

def _signal_type(rec):
    """Return 'metrics', 'logs', or 'traces' for a JSONL record.

    Supports both the legacy {ts, path, payload} wrapper format and raw
    OTLP JSON where each line is {resourceMetrics: ...} etc.
    """
    path = rec.get("path", "")
    if path:
        if "/v1/metrics" in path:
            return "metrics"
        if "/v1/logs" in path:
            return "logs"
        if "/v1/traces" in path:
            return "traces"
        return ""

    if "resourceMetrics" in rec:
        return "metrics"
    if "resourceLogs" in rec:
        return "logs"
    if "resourceSpans" in rec:
        return "traces"
    return ""


def _payload(rec):
    """Extract the OTLP payload from a record (legacy wrapper or raw)."""
    if "payload" in rec:
        return rec["payload"]
    return rec


# ---------------------------------------------------------------------------
# otelcol-contrib collector
# ---------------------------------------------------------------------------

def _find_free_port(bind_addr="127.0.0.1"):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((bind_addr, 0))
        return s.getsockname()[1]


def _write_config(run_dir, http_port, grpc_port, otel_log, bind_addr="127.0.0.1"):
    config_path = os.path.join(run_dir, "otelcol.yaml")
    config = textwrap.dedent(f"""\
        receivers:
          otlp:
            protocols:
              http:
                endpoint: "{bind_addr}:{http_port}"
              grpc:
                endpoint: "{bind_addr}:{grpc_port}"

        processors:
          batch:
            timeout: 2s
            send_batch_size: 256

        exporters:
          file:
            path: "{otel_log}"
            append: true

        service:
          telemetry:
            logs:
              level: warn
          pipelines:
            traces:
              receivers: [otlp]
              processors: [batch]
              exporters: [file]
            metrics:
              receivers: [otlp]
              processors: [batch]
              exporters: [file]
            logs:
              receivers: [otlp]
              processors: [batch]
              exporters: [file]
    """)
    with open(config_path, "w") as f:
        f.write(config)
    return config_path


def _start_otelcol(run_dir, bind_addr="127.0.0.1"):
    os.makedirs(run_dir, exist_ok=True)
    otel_log = os.path.join(run_dir, "claude-otel.jsonl")
    otel_rate = os.path.join(run_dir, "claude-otel-rate.json")

    for f in [otel_log]:
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass

    http_port = _find_free_port(bind_addr)
    grpc_port = _find_free_port(bind_addr)
    config_path = _write_config(run_dir, http_port, grpc_port, otel_log, bind_addr)

    otelcol = shutil.which("otelcol-contrib")
    if not otelcol:
        raise FileNotFoundError("otelcol-contrib not found in PATH")

    proc = subprocess.Popen(
        [otelcol, "--config", config_path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Wait for the HTTP endpoint to accept connections
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((bind_addr, http_port), timeout=0.5):
                break
        except (ConnectionRefusedError, OSError):
            if proc.poll() is not None:
                raise RuntimeError(
                    f"otelcol-contrib exited with code {proc.returncode}"
                )
            time.sleep(0.2)
    else:
        proc.kill()
        raise RuntimeError("otelcol-contrib did not start within 10 seconds")

    return proc, http_port, otel_log, otel_rate


# ---------------------------------------------------------------------------
# Legacy built-in Python collector (fallback)
# ---------------------------------------------------------------------------

class _LegacyOTLPHandler(BaseHTTPRequestHandler):
    def _read_chunked(self):
        chunks = []
        total = 0
        while True:
            line = self.rfile.readline()
            if not line:
                break
            try:
                chunk_size = int(line.split(b";")[0].strip(), 16)
            except ValueError:
                break
            if chunk_size == 0:
                while True:
                    trailer = self.rfile.readline()
                    if not trailer or trailer in (b"\r\n", b"\n"):
                        break
                break
            if chunk_size > MAX_BODY_SIZE:
                return None
            chunk = self.rfile.read(chunk_size)
            self.rfile.readline()
            total += len(chunk)
            if total > MAX_BODY_SIZE:
                return None
            chunks.append(chunk)
        return b"".join(chunks)

    def do_POST(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = self._read_chunked()
            if body is None:
                self.send_error(413, "Payload Too Large")
                return
        else:
            try:
                length = int(self.headers.get("Content-Length", 0))
            except ValueError:
                self.send_error(400, "Invalid Content-Length")
                return
            if length > MAX_BODY_SIZE:
                self.send_error(413, "Payload Too Large")
                return
            body = self.rfile.read(length) if length else b""
        try:
            payload = json.loads(body) if body else {}
        except json.JSONDecodeError:
            payload = {"raw": body.decode("utf-8", errors="replace")}

        record = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "path": self.path,
            "payload": payload,
        }
        log_file = os.environ.get("OTEL_LOG_FILE", "/tmp/claude-otel.jsonl")
        with open(log_file, "a") as f:
            f.write(json.dumps(record) + "\n")

        if "/v1/metrics" in self.path:
            _update_token_rate(payload)

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"partialSuccess":{}}')

    def log_message(self, format, *args):
        pass


def _update_token_rate(payload):
    global _token_samples
    now = time.monotonic()
    total = 0
    for rm in payload.get("resourceMetrics", []):
        for sm in rm.get("scopeMetrics", []):
            for metric in sm.get("metrics", []):
                if metric.get("name") == "claude_code.token.usage":
                    data = metric.get("sum", metric.get("gauge", {}))
                    for dp in data.get("dataPoints", []):
                        total += dp.get("asDouble", dp.get("asInt", 0))
    if total <= 0:
        return

    _token_samples.append((now, total))
    cutoff = now - _WINDOW_SECS
    _token_samples = [(t, v) for t, v in _token_samples if t >= cutoff]

    rate = 0.0
    if len(_token_samples) >= 2:
        dt = _token_samples[-1][0] - _token_samples[0][0]
        dv = _token_samples[-1][1] - _token_samples[0][1]
        if dt > 0:
            rate = dv / dt

    rate_file = os.environ.get("OTEL_RATE_FILE", "/tmp/claude-otel-rate.json")
    tmp = rate_file + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"total": total, "rate": rate, "ts": time.time()}, f)
    os.replace(tmp, rate_file)


def _start_legacy(run_dir, bind_addr="127.0.0.1"):
    otel_log = os.path.join(run_dir, "claude-otel.jsonl")
    otel_rate = os.path.join(run_dir, "claude-otel-rate.json")
    port_file = os.path.join(run_dir, "otel-port")

    for f in [otel_log, port_file]:
        try:
            os.unlink(f)
        except FileNotFoundError:
            pass

    env = {
        **os.environ,
        "OTEL_LOG_FILE": otel_log,
        "OTEL_RATE_FILE": otel_rate,
        "OTEL_COLLECTOR_PORT": "0",
        "OTEL_PORT_FILE": port_file,
        "OTEL_BIND_ADDR": bind_addr,
    }
    proc = subprocess.Popen(
        [sys.executable, "-m", "agentic_ci.otel"],
        env=env,
        stderr=subprocess.DEVNULL,
    )

    for _ in range(50):
        if os.path.exists(port_file):
            break
        time.sleep(0.1)
    else:
        proc.kill()
        raise RuntimeError("Legacy OTEL collector did not write port file")

    with open(port_file) as f:
        port = int(f.read().strip())

    return proc, port, otel_log, otel_rate


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def start_collector(run_dir, bind_addr="127.0.0.1"):
    """Start an OTEL collector. Prefers otelcol-contrib, falls back to built-in."""
    if shutil.which("otelcol-contrib"):
        return _start_otelcol(run_dir, bind_addr)
    return _start_legacy(run_dir, bind_addr)


def stop_collector(proc):
    """Stop the OTEL collector subprocess."""
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def parse_metrics(records):
    """Parse OTLP JSONL records into structured token/cost data.

    Supports both legacy {ts, path, payload} wrapper format and raw OTLP JSON.
    """
    token_totals = defaultdict(float)
    cost_totals = defaultdict(float)
    api_requests = []
    active_time = defaultdict(float)

    for rec in records:
        sig = _signal_type(rec)
        payload = _payload(rec)

        if sig == "metrics":
            for rm in payload.get("resourceMetrics", []):
                for sm in rm.get("scopeMetrics", []):
                    for metric in sm.get("metrics", []):
                        name = metric.get("name", "")
                        data = metric.get("sum", metric.get("gauge", metric.get("histogram", {})))
                        for dp in data.get("dataPoints", []):
                            attrs = {
                                a["key"]: a["value"].get(
                                    "stringValue",
                                    a["value"].get("intValue", a["value"].get("doubleValue")),
                                )
                                for a in dp.get("attributes", [])
                            }
                            raw = dp.get("asDouble", dp.get("asInt", 0))
                            value = float(raw) if isinstance(raw, str) else raw

                            if name == "claude_code.token.usage":
                                model = attrs.get("model", "unknown")
                                token_type = attrs.get("type", "unknown")
                                token_totals[(model, token_type)] += value
                            elif name == "claude_code.cost.usage":
                                model = attrs.get("model", "unknown")
                                cost_totals[model] += value
                            elif name == "claude_code.active_time.total":
                                time_type = attrs.get("type", "unknown")
                                active_time[time_type] += value

        elif sig == "logs":
            for rl in payload.get("resourceLogs", []):
                for sl in rl.get("scopeLogs", []):
                    for lr in sl.get("logRecords", []):
                        event_name = ""
                        event_attrs = {}
                        for a in lr.get("attributes", []):
                            key = a["key"]
                            val = a["value"]
                            v = val.get("stringValue", val.get("intValue", val.get("doubleValue")))
                            event_attrs[key] = v
                            if key == "event.name":
                                event_name = v
                        if event_name == "claude_code.api_request":
                            api_requests.append(event_attrs)

    return token_totals, cost_totals, api_requests, active_time


def print_summary(log_file):
    """Print a human-readable token/cost summary from an OTEL JSONL log."""
    records = []
    try:
        with open(log_file) as f:
            for line in f:
                line = line.strip()
                if line:
                    records.append(json.loads(line))
    except FileNotFoundError:
        print("No OTEL data collected (log file not found).")
        return
    except json.JSONDecodeError as e:
        print(f"Error parsing OTEL log: {e}")
        return

    if not records:
        print("No OTEL data collected.")
        return

    token_totals, cost_totals, api_requests, active_time = parse_metrics(records)

    if token_totals:
        models = sorted(set(m for m, _ in token_totals.keys()))
        for model in models:
            print(f"\n  Model: {model}")
            print(f"  {'Token Type':<20} {'Count':>12}")
            print(f"  {'-' * 20} {'-' * 12}")
            model_tokens = {t: c for (m, t), c in token_totals.items() if m == model}
            for token_type in ["input", "cacheRead", "cacheCreation", "output"]:
                if token_type in model_tokens:
                    print(f"  {token_type:<20} {model_tokens[token_type]:>12,.0f}")
            total = sum(model_tokens.values())
            print(f"  {'TOTAL':<20} {total:>12,.0f}")

    if cost_totals:
        print(f"\n  {'Model':<30} {'Cost (USD)':>12}")
        print(f"  {'-' * 30} {'-' * 12}")
        grand_total = 0.0
        for model in sorted(cost_totals.keys()):
            cost = cost_totals[model]
            grand_total += cost
            print(f"  {model:<30} ${cost:>11.4f}")
        if len(cost_totals) > 1:
            print(f"  {'TOTAL':<30} ${grand_total:>11.4f}")

    if active_time:
        print("\n  Active Time:")
        for time_type, seconds in sorted(active_time.items()):
            mins, secs = divmod(int(seconds), 60)
            print(f"    {time_type}: {mins}m {secs}s")

    if api_requests:
        print(f"\n  API Requests: {len(api_requests)}")
        total_duration = sum(float(r.get("duration_ms", 0)) for r in api_requests)
        if total_duration:
            print(f"  Total API time: {total_duration / 1000:.1f}s")


# ---------------------------------------------------------------------------
# Legacy __main__ entry point (used by _start_legacy fallback)
# ---------------------------------------------------------------------------

def main():
    """Run the legacy built-in OTEL collector server."""
    port = int(os.environ.get("OTEL_COLLECTOR_PORT", "4318"))
    bind_addr = os.environ.get("OTEL_BIND_ADDR", "127.0.0.1")
    server = HTTPServer((bind_addr, port), _LegacyOTLPHandler)
    actual_port = server.server_address[1]
    port_file = os.environ.get("OTEL_PORT_FILE")
    if port_file:
        with open(port_file, "w") as f:
            f.write(str(actual_port))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    log_file = os.environ.get("OTEL_LOG_FILE", "/tmp/claude-otel.jsonl")
    print(
        f"OTLP collector listening on {bind_addr}:{actual_port}, writing to {log_file}",
        file=sys.stderr,
    )
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    server.server_close()


if __name__ == "__main__":
    main()
