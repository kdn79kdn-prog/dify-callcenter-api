import os
import json
from typing import Any, Dict, List

from fastapi import FastAPI, HTTPException
from google.oauth2 import service_account
from googleapiclient.discovery import build

app = FastAPI()


@app.get("/health")
def health():
    # 反映確認用versionは残してOK
    return {"status": "ok", "version": "2026-02-19-01"}


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
    return res.get("files", [])


@app.post("/run_daily_close")
def run_daily_close():
    """
    Drive疎通テスト用:
    INPUTフォルダの子要素を最大3件返す
    """
    input_folder_id = os.environ.get("DRIVE_INPUT_FOLDER_ID")
    if not input_folder_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_INPUT_FOLDER_ID")

    try:
        drive = _get_drive_service()
        children = _list_children(drive, input_folder_id, page_size=3)
        return {
            "status": "ok",
            "message": "Drive connection OK",
            "input_folder_id": input_folder_id,
            "children_preview": children,
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Drive test failed: {type(e).__name__}: {e}",
        )
