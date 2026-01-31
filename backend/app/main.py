import os
import httpx
from fastapi import FastAPI, HTTPException

app = FastAPI(title="MeritFlow API", version="0.1.0")

CLOUDANT_URL = os.environ["CLOUDANT_URL"].rstrip("/")
CLOUDANT_APIKEY = os.environ["CLOUDANT_APIKEY"]

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


@app.get("/health")
def health():
    return {"ok": True}


@app.get("/cloudant/ping")
async def cloudant_ping():
    token = await get_iam_token()
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{CLOUDANT_URL}/_all_dbs",
            headers={"Authorization": f"Bearer {token}"},
        )
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail=resp.text)
        return {"ok": True, "dbs": resp.json()}
