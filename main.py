import os
import json
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()


@app.get("/health")
def health():
    return {"status": "ok"}


def _get_drive_service():
    sa_json = os.environ.get("GCP_SA_JSON")
    if not sa_json:
        raise RuntimeError("Missing env: GCP_SA_JSON")

    sa_info = json.loads(sa_json)

    credentials = service_account.Credentials.from_service_account_info(
        sa_info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )

    return build("drive", "v3", credentials=credentials, cache_discovery=False)


def _list_children(drive, folder_id: str, page_size: int = 3) -> List[Dict[str, Any]]:
    q = f"'{folder_id}' in parents and trashed = false"
    res = drive.files().list(
        q=q,
        fields="files(id,name,mimeType,modifiedTime)",
        pageSize=page_size,
        orderBy="modifiedTime desc",
    ).execute()
    return res.g
