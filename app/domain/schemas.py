from pydantic import BaseModel, Field
from typing import Any, Dict, Optional
from datetime import datetime
import uuid


class IngestRequest(BaseModel):
    event_type: str = Field(..., description="Type of event, e.g. support_request, user_message, task_requested")
    source: str = Field("api", description="Where this event came from, e.g. api, slack, telegram")
    actor: Optional[str] = Field(None, description="Who initiated the event (user id/email), if available")
    payload: Dict[str, Any] = Field(..., description="The core content of the event")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional extra context for debugging/routing")


class Event(BaseModel):
    event_id: str = Field(..., description="Unique idempotency anchor for this event")
    event_type: str
    source: str
    timestamp: datetime
    actor: Optional[str] = None
    payload: Dict[str, Any]
    metadata: Dict[str, Any]


class Decision(BaseModel):
    decision_id: str = Field(..., description="Unique identifier for this decision")
    event_id: str = Field(..., description="The event this decision was derived from")
    route: str = Field(
        ...,
        description="Chosen route, e.g. ESCALATE_HUMAN, REQUEST_MORE_INFO, CREATE_DRAFT_TICKET",
    )
    reason: str = Field(..., description="Human-readable reason for this decision")
    risk_level: str = Field("low", description="Risk level: low, medium, high")
    proposed_action: Dict[str, Any] = Field(
        default_factory=dict,
        description="Optional structured action request",
    )


class IngestResponse(BaseModel):
    event: Event
    decision: Decision


class ActionResult(BaseModel):
    action_id: str = Field(..., description="Unique identifier for this action execution")
    event_id: str = Field(..., description="Event the action corresponds to")
    decision_id: str = Field(..., description="Decision that triggered this action")
    action_type: str = Field(..., description="Type of action executed (or attempted)")
    status: str = Field(..., description="Outcome: executed, skipped, noop, failed")
    artifact_path: Optional[str] = Field(None, description="Where a draft artifact was stored (if any)")
    reason: str = Field(..., description="Human-readable explanation of what happened")

class SlackIngestRequest(BaseModel):
    text: str
    user: Optional[str] = None
    channel: Optional[str] = None
    ts: Optional[str] = None
