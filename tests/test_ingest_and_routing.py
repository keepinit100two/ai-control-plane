from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app)


def test_ingest_requires_idempotency_key():
    payload = {
        "event_type": "support_request",
        "source": "api",
        "actor": "test_user",
        "payload": {"text": "VPN is down", "urgency": "high"},
        "metadata": {},
    }

    response = client.post("/ingest/api", json=payload)

    assert response.status_code == 400
    assert response.json()["detail"] == "Missing Idempotency-Key header"


def test_security_keyword_routes_to_escalation():
    payload = {
        "event_type": "support_request",
        "source": "api",
        "actor": "test_user",
        "payload": {"text": "There was a security breach", "urgency": "high"},
        "metadata": {},
    }

    headers = {"Idempotency-Key": "test-security-1"}

    response = client.post("/ingest/api", json=payload, headers=headers)

    assert response.status_code == 200

    body = response.json()
    decision = body["decision"]

    assert decision["route"] == "ESCALATE_HUMAN"
    assert decision["risk_level"] == "high"


def test_idempotency_same_key_returns_same_event_id():
    payload = {
        "event_type": "support_request",
        "source": "api",
        "actor": "test_user",
        "payload": {"text": "VPN is down", "urgency": "high"},
        "metadata": {},
    }

    headers = {"Idempotency-Key": "test-idem-1"}

    r1 = client.post("/ingest/api", json=payload, headers=headers)
    assert r1.status_code == 200
    event_id_1 = r1.json()["event"]["event_id"]

    r2 = client.post("/ingest/api", json=payload, headers=headers)
    assert r2.status_code == 200
    event_id_2 = r2.json()["event"]["event_id"]

    assert event_id_1 == event_id_2


def test_slack_adapter_translates_and_routes_request_more_info():
    """
    Slack adapter should:
    - accept Slack-shaped payload
    - translate it into canonical IngestRequest/Event
    - run the same pipeline
    - because urgency is missing, route to REQUEST_MORE_INFO
    """
    slack_payload = {
        "text": "VPN is down",
        "user": "U123",
        "channel": "C999",
        "ts": "1700000000.0001",
    }

    headers = {"Idempotency-Key": "slack-test-1"}

    response = client.post("/ingest/slack", json=slack_payload, headers=headers)
    assert response.status_code == 200

    body = response.json()
    event = body["event"]
    decision = body["decision"]

    # Adapter translation assertions
    assert event["source"] == "slack"
    assert event["actor"] == "U123"
    assert event["payload"]["text"] == "VPN is down"
    assert event["metadata"]["channel"] == "C999"
    assert event["metadata"]["ts"] == "1700000000.0001"

    # Routing assertion (urgency missing)
    assert decision["route"] == "REQUEST_MORE_INFO"
