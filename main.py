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


from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

REQUIRED_FILES = [
    "CPH.xlsx",
    "AHT.xlsx",
    "ATT.xlsx",
    "ACW.xlsx",
    "CPD.xlsx",
    "着座比率.xlsx",
    "稼働率.xlsx",
]

def _find_child_folder_by_name(drive, parent_folder_id: str, folder_name: str):
    q = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"name = '{folder_name}' and trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id,name)", pageSize=10).execute()
    files = res.get("files", [])
    return files[0] if files else None

def _list_child_files(drive, parent_folder_id: str, page_size: int = 200):
    q = f"'{parent_folder_id}' in parents and trashed = false"
    res = drive.files().list(
        q=q,
        fields="files(id,name,mimeType,modifiedTime)",
        pageSize=page_size,
        orderBy="name",
    ).execute()
    return res.get("files", [])

@app.post("/run_daily_close")
def run_daily_close():
    """
    Phase1:
    - JST前日フォルダを探す
    - 必須7ファイルが揃ってるか検証する
    """
    input_folder_id = os.environ.get("DRIVE_INPUT_FOLDER_ID")
    if not input_folder_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_INPUT_FOLDER_ID")

    # 1) JST基準で前日(as_of_date)を算出
    jst = ZoneInfo("Asia/Tokyo")
    as_of_date = (datetime.now(jst).date() - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        drive = _get_drive_service()

        # 2) INPUT配下から前日フォルダを取得
        daily_folder = _find_child_folder_by_name(drive, input_folder_id, as_of_date)
        if not daily_folder:
            raise HTTPException(
                status_code=409,
                detail=f"INPUT_NOT_READY: daily folder not found: {as_of_date}",
            )

        daily_folder_id = daily_folder["id"]

        # 3) フォルダ配下のファイル一覧取得
        children = _list_child_files(drive, daily_folder_id, page_size=200)

        # フォルダは除外して「ファイル名」だけ集める
        found_file_names = sorted([
            f["name"] for f in children
            if f.get("mimeType") != "application/vnd.google-apps.folder"
        ])

        # 4) 必須7ファイルが揃っているか検証
        found_set = set(found_file_names)
        missing_files = [name for name in REQUIRED_FILES if name not in found_set]

        return {
            "status": "ok" if not missing_files else "error",
            "phase": "phase1_validate_inputs",
            "as_of_date": as_of_date,
            "input_folder_id": input_folder_id,
            "daily_folder_id": daily_folder_id,
            "found_files": found_file_names,
            "missing_files": missing_files,
            "extra_files": sorted(list(found_set - set(REQUIRED_FILES))),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Phase1 failed: {type(e).__name__}: {e}")

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
