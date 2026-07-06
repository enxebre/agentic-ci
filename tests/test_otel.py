"""Tests for the OTLP collector lifecycle and metrics parsing."""

import json
import shutil
import urllib.request

import pytest

from agentic_ci.otel import (
    _signal_type,
    _payload,
    parse_metrics,
    start_collector,
    stop_collector,
)


# ---------------------------------------------------------------------------
# Signal type detection
# ---------------------------------------------------------------------------

class TestSignalType:
    def test_legacy_path_metrics(self):
        assert _signal_type({"path": "/v1/metrics", "payload": {}}) == "metrics"

    def test_legacy_path_logs(self):
        assert _signal_type({"path": "/v1/logs", "payload": {}}) == "logs"

    def test_legacy_path_traces(self):
        assert _signal_type({"path": "/v1/traces", "payload": {}}) == "traces"

    def test_raw_resource_metrics(self):
        assert _signal_type({"resourceMetrics": []}) == "metrics"

    def test_raw_resource_logs(self):
        assert _signal_type({"resourceLogs": []}) == "logs"

    def test_raw_resource_spans(self):
        assert _signal_type({"resourceSpans": []}) == "traces"

    def test_unknown(self):
        assert _signal_type({"foo": "bar"}) == ""


class TestPayload:
    def test_legacy_wrapper(self):
        rec = {"path": "/v1/metrics", "payload": {"resourceMetrics": [{"test": 1}]}}
        assert _payload(rec) == {"resourceMetrics": [{"test": 1}]}

    def test_raw_format(self):
        rec = {"resourceMetrics": [{"test": 1}]}
        assert _payload(rec) == rec


# ---------------------------------------------------------------------------
# parse_metrics — supports both formats
# ---------------------------------------------------------------------------

METRICS_PAYLOAD = {
    "resourceMetrics": [
        {
            "scopeMetrics": [
                {
                    "metrics": [
                        {
                            "name": "claude_code.token.usage",
                            "sum": {
                                "dataPoints": [
                                    {
                                        "asInt": 500,
                                        "attributes": [
                                            {"key": "model", "value": {"stringValue": "claude-opus-4"}},
                                            {"key": "type", "value": {"stringValue": "input"}},
                                        ],
                                    }
                                ]
                            },
                        },
                        {
                            "name": "claude_code.cost.usage",
                            "sum": {
                                "dataPoints": [
                                    {
                                        "asDouble": 0.05,
                                        "attributes": [
                                            {"key": "model", "value": {"stringValue": "claude-opus-4"}},
                                        ],
                                    }
                                ]
                            },
                        },
                    ]
                }
            ]
        }
    ]
}


class TestParseMetrics:
    def test_legacy_format(self):
        records = [{"path": "/v1/metrics", "payload": METRICS_PAYLOAD}]
        token_totals, cost_totals, _, _ = parse_metrics(records)
        assert token_totals[("claude-opus-4", "input")] == 500
        assert cost_totals["claude-opus-4"] == pytest.approx(0.05)

    def test_raw_format(self):
        records = [METRICS_PAYLOAD]
        token_totals, cost_totals, _, _ = parse_metrics(records)
        assert token_totals[("claude-opus-4", "input")] == 500
        assert cost_totals["claude-opus-4"] == pytest.approx(0.05)

    def test_mixed_formats(self):
        records = [
            {"path": "/v1/metrics", "payload": METRICS_PAYLOAD},
            METRICS_PAYLOAD,
        ]
        token_totals, cost_totals, _, _ = parse_metrics(records)
        assert token_totals[("claude-opus-4", "input")] == 1000
        assert cost_totals["claude-opus-4"] == pytest.approx(0.10)

    def test_empty_records(self):
        token_totals, cost_totals, api_requests, active_time = parse_metrics([])
        assert len(token_totals) == 0
        assert len(cost_totals) == 0


# ---------------------------------------------------------------------------
# Collector lifecycle (integration test)
# ---------------------------------------------------------------------------

@pytest.fixture()
def collector(tmp_path):
    proc, port, log, _rate = start_collector(str(tmp_path))
    yield port, log
    stop_collector(proc)


def _post(port, path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"http://127.0.0.1:{port}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return resp.status


def _read_log(log_path, retries=5):
    """Read JSONL log, retrying for batch flush delay."""
    import time
    for _ in range(retries):
        records = []
        try:
            with open(log_path) as f:
                for line in f:
                    if line.strip():
                        records.append(json.loads(line))
        except FileNotFoundError:
            pass
        if records:
            return records
        time.sleep(1)
    return []


class TestCollector:
    def test_receives_metrics(self, collector):
        port, log = collector
        status = _post(port, "/v1/metrics", METRICS_PAYLOAD)
        assert status == 200

        records = _read_log(log)
        assert len(records) >= 1
        token_totals, cost_totals, _, _ = parse_metrics(records)
        assert token_totals[("claude-opus-4", "input")] == 500

    def test_receives_logs(self, collector):
        port, log = collector
        payload = {
            "resourceLogs": [{
                "scopeLogs": [{
                    "logRecords": [{
                        "body": {"stringValue": "test log"},
                        "timeUnixNano": "1000000000",
                        "attributes": [
                            {"key": "event.name", "value": {"stringValue": "test"}}
                        ],
                    }]
                }]
            }]
        }
        status = _post(port, "/v1/logs", payload)
        assert status == 200

        records = _read_log(log)
        assert len(records) >= 1

    def test_receives_traces(self, collector):
        port, log = collector
        payload = {
            "resourceSpans": [{
                "scopeSpans": [{
                    "spans": [{
                        "traceId": "0123456789abcdef0123456789abcdef",
                        "spanId": "0123456789abcdef",
                        "name": "test-span",
                        "startTimeUnixNano": "1000000000",
                        "endTimeUnixNano": "2000000000",
                    }]
                }]
            }]
        }
        status = _post(port, "/v1/traces", payload)
        assert status == 200

        records = _read_log(log)
        assert len(records) >= 1

    @pytest.mark.skipif(
        not shutil.which("otelcol-contrib"),
        reason="otelcol-contrib not installed",
    )
    def test_uses_otelcol_when_available(self, tmp_path):
        proc, port, log, _ = start_collector(str(tmp_path))
        try:
            status = _post(port, "/v1/metrics", METRICS_PAYLOAD)
            assert status == 200
            records = _read_log(log)
            assert len(records) >= 1
        finally:
            stop_collector(proc)
