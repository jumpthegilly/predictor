"""
FastAPI application — HTTP layer for the MMA predictor platform.

Endpoints
---------
GET  /health                     — Health check
POST /pipeline/fighter           — Run signal pipeline for one fighter
POST /pipeline/event             — Run signal pipeline for all fighters in an event
POST /reports/bout               — Generate a pre-fight bout report
POST /reports/card               — Generate a full card report
GET  /reports/bout/{bout_id}     — Fetch a stored bout report
GET  /reports/event/{event_id}   — Fetch all stored bout reports for an event

Usage
-----
    uvicorn src.api.main:app --reload
"""
from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from src.generators.card_report import generate_card_report
from src.generators.report_generator import generate_bout_report
from src.pipeline.event_runner import run_event_pipeline
from src.pipeline.signal_pipeline import run_signal_pipeline
from src.storage.report_storage import get_bout_report, get_event_reports

app = FastAPI(
    title="MMA Predictor API",
    description="Pre-fight intelligence signals and bout predictions for MMA events.",
    version="1.0.0",
)


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class FighterPipelineRequest(BaseModel):
    fighter_name: str
    fighter_id: str
    event_id: str


class EventPipelineRequest(BaseModel):
    event_id: str


class BoutReportRequest(BaseModel):
    bout_id: str


class CardReportRequest(BaseModel):
    event_id: str


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@app.get("/health")
def health_check() -> dict:
    """Simple liveness check."""
    return {"status": "ok"}


@app.post("/pipeline/fighter")
def fighter_pipeline(request: FighterPipelineRequest) -> dict:
    """
    Run the full signal harvesting pipeline for one fighter and event.

    Fetches news, Reddit, YouTube, and prediction market signals,
    processes them with Claude, and stores results to Supabase.
    """
    result = run_signal_pipeline(
        fighter_name=request.fighter_name,
        fighter_id=request.fighter_id,
        event_id=request.event_id,
    )
    return result


@app.post("/pipeline/event")
def event_pipeline(request: EventPipelineRequest) -> dict:
    """
    Run the signal pipeline for all fighters on an event card.

    Processes each fighter in the event sequentially.
    """
    result = run_event_pipeline(event_id=request.event_id)
    return result


@app.post("/reports/bout")
def bout_report(request: BoutReportRequest) -> dict:
    """
    Generate a pre-fight bout report using Claude Sonnet.

    Fetches fighter data and signals from Supabase, calls Claude
    to produce a full prediction report, and stores it.
    """
    result = generate_bout_report(bout_id=request.bout_id)
    if result.get("error"):
        raise HTTPException(status_code=500, detail="Report generation failed.")
    return result


@app.post("/reports/card")
def card_report(request: CardReportRequest) -> dict:
    """
    Generate a card-level narrative report for an event.

    Generates missing bout reports, then synthesises a card summary.
    """
    result = generate_card_report(event_id=request.event_id)
    return result


@app.get("/reports/bout/{bout_id}")
def fetch_bout_report(bout_id: str) -> dict:
    """
    Fetch the most recent stored report for a bout.
    """
    report = get_bout_report(bout_id=bout_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found.")
    return report


@app.get("/reports/event/{event_id}")
def fetch_event_reports(event_id: str) -> list:
    """
    Fetch all stored bout reports for an event.
    """
    return get_event_reports(event_id=event_id)
