import os
import time
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="MeritFlow API", version="0.2.0")

# Required env vars
CLOUDANT_URL = os.environ["CLOUDANT_URL"].rstrip("/")
CLOUDANT_APIKEY = os.environ["CLOUDANT_APIKEY"]

# DB names (defaults match what you created)
DB_COURSE = os.environ.get("DB_COURSE_CATALOG", "course_catalog")
DB_EVENTS = os.environ.get("DB_WORK_EVENTS", "work_events")
DB_KUDOS = os.environ.get("DB_KUDOS_LOG", "kudos_log")
DB_GROWTH = os.environ.get("DB_GROWTH_RECOS", "growth_recos_log")
DB_PULSE = os.environ.get("DB_PULSE", "pulse_aggregates")

IAM_TOKEN_URL = "https://iam.cloud.ibm.com/identity/token"


async def get_iam_token() -> str:
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            IAM_TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data=f"grant_type=urn:ibm:params:oauth:grant-type:apikey&apikey={CLOUDANT_APIKEY}",
        )
        resp.raise_for_status()
        return resp.json()["access_token"]


async def cloudant_find(db: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    token = await get_iam_token()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(
            f"{CLOUDANT_URL}/{db}/_find",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


async def cloudant_put(db: str, doc_id: str, doc: Dict[str, Any]) -> Dict[str, Any]:
    token = await get_iam_token()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.put(
            f"{CLOUDANT_URL}/{db}/{doc_id}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=doc,
        )
        if resp.status_code not in (200, 201, 202):
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return resp.json()


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/cloudant/ping")
async def cloudant_ping():
    token = await get_iam_token()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{CLOUDANT_URL}/_all_dbs", headers={"Authorization": f"Bearer {token}"})
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return {"ok": True, "dbs": resp.json()}


# -----------------------
# Growth workflow endpoints
# -----------------------

@app.get("/events/recent")
async def recent_events(employee_id: str, limit: int = 10):
    payload = {
        "selector": {"employee_id": {"$eq": employee_id}},
        "sort": [{"timestamp": "desc"}],
        "limit": min(limit, 50),
    }
    return await cloudant_find(DB_EVENTS, payload)


@app.get("/courses/search")
async def search_courses(skill_tag: str, limit: int = 10):
    payload = {
        "selector": {"skill_tags_normalized": {"$elemMatch": {"$eq": skill_tag}}},
        "limit": min(limit, 50),
    }
    return await cloudant_find(DB_COURSE, payload)


# -----------------------
# Recognition workflow endpoints
# -----------------------

class KudosCreate(BaseModel):
    from_employee_id: str
    to_employee_id: str
    manager_id: str
    team_id: str
    message: str
    values_tags: Optional[List[str]] = None
    related_event_id: Optional[str] = None


@app.post("/kudos/create")
async def create_kudos(req: KudosCreate):
    kudos_id = f"kudos_{int(time.time())}"
    doc: Dict[str, Any] = {
        "_id": kudos_id,
        "kudos_id": kudos_id,
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "approval_status": "pending",
        "from_employee_id": req.from_employee_id,
        "to_employee_id": req.to_employee_id,
        "manager_id": req.manager_id,
        "team_id": req.team_id,
        "message": req.message,
        "values_tags": req.values_tags or [],
        "related_event_id": req.related_event_id,
    }
    result = await cloudant_put(DB_KUDOS, kudos_id, doc)
    return {"ok": True, "result": result, "kudos": doc}


@app.get("/kudos/pending")
async def pending_kudos(manager_id: str, limit: int = 20):
    payload = {
        "selector": {
            "manager_id": {"$eq": manager_id},
            "approval_status": {"$eq": "pending"},
        },
        "sort": [{"created_at": "desc"}],
        "limit": min(limit, 50),
    }
    return await cloudant_find(DB_KUDOS, payload)


# -----------------------
# Culture workflow endpoint (aggregated only)
# -----------------------

@app.get("/pulse/team")
async def pulse_team(team_id: str, limit: int = 8):
    payload = {
        "selector": {"team_id": {"$eq": team_id}},
        "sort": [{"week_start": "desc"}],
        "limit": min(limit, 52),
    }
    return await cloudant_find(DB_PULSE, payload)
