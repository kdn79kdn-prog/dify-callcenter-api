import os
import json
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List

import pandas as pd
from fastapi import FastAPI, HTTPException
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from zoneinfo import ZoneInfo

app = FastAPI()

# ---- 設定 ----
REQUIRED_FILES = [
    "CPH.xlsx",
    "AHT.xlsx",
    "ATT.xlsx",
    "ACW.xlsx",
    "CPD.xlsx",
    "着座比率.xlsx",
    "稼働率.xlsx",
]

METRIC_BY_FILENAME = {
    "CPH.xlsx": "CPH",
    "AHT.xlsx": "AHT",
    "ATT.xlsx": "ATT",
    "ACW.xlsx": "ACW",
    "CPD.xlsx": "CPD",
    "着座比率.xlsx": "着座比率",
    "稼働率.xlsx": "稼働率",
}


@app.get("/health")
def health():
    return {"status": "ok", "version": "2026-02-19-01"}


# ---- Drive ----
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


def _find_child_folder_by_name(drive, parent_folder_id: str, folder_name: str):
    q = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"name = '{folder_name}' and trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id,name)", pageSize=10).execute()
    files = res.get("files", [])
    return files[0] if files else None


def _list_child_files(drive, parent_folder_id: str, page_size: int = 200) -> List[Dict[str, Any]]:
    q = f"'{parent_folder_id}' in parents and trashed = false"
    res = drive.files().list(
        q=q,
        fields="files(id,name,mimeType,modifiedTime)",
        pageSize=page_size,
        orderBy="name",
    ).execute()
    return res.get("files", [])


def _download_file_bytes(drive, file_id: str) -> bytes:
    fh = BytesIO()
    request = drive.files().get_media(fileId=file_id)
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    return fh.getvalue()


# ---- Excel ----
def _read_excel_from_bytes(xbytes: bytes) -> pd.DataFrame:
    # openpyxl が requirements に入っていればこれで読める
    return pd.read_excel(BytesIO(xbytes))


def _normalize_to_agent_value(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("　", " ") for c in df.columns]

    rename_map = {}
    for c in df.columns:
        c2 = c.strip()
        if c2 in ["エージェントID", "エージェントId", "agent_id", "AgentID", "AGENT_ID", "ID"]:
            rename_map[c] = "agent_id"
        if c2 in ["値", "value", "Value", "VAL", "数値"]:
            rename_map[c] = "value"

    df = df.rename(columns=rename_map)

    if "agent_id" not in df.columns or "value" not in df.columns:
        raise ValueError(f"Excel columns must include agent_id & value. found={list(df.columns)}")

    out = df[["agent_id", "value"]].copy()
    out["agent_id"] = out["agent_id"].astype(str).str.strip()

    out["value"] = (
        out["value"]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace(",", "", regex=False)
        .str.strip()
    )
    out["value"] = pd.to_numeric(out["value"], errors="coerce")

    out = out.dropna(subset=["agent_id", "value"], how="any")
    return out


@app.post("/run_daily_close")
def run_daily_close():
    input_folder_id = os.environ.get("DRIVE_INPUT_FOLDER_ID")
    if not input_folder_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_INPUT_FOLDER_ID")

    jst = ZoneInfo("Asia/Tokyo")
    as_of_date = (datetime.now(jst).date() - timedelta(days=1)).strftime("%Y-%m-%d")

    try:
        drive = _get_drive_service()

        # Phase1: 前日フォルダ取得
        daily_folder = _find_child_folder_by_name(drive, input_folder_id, as_of_date)
        if not daily_folder:
            raise HTTPException(
                status_code=409,
                detail=f"INPUT_NOT_READY: daily folder not found: {as_of_date}",
            )
        daily_folder_id = daily_folder["id"]

        children = _list_child_files(drive, daily_folder_id, page_size=200)

        found_file_names = sorted([
            f["name"] for f in children
            if f.get("mimeType") != "application/vnd.google-apps.folder"
        ])

        found_set = set(found_file_names)
        missing_files = [name for name in REQUIRED_FILES if name not in found_set]

        if missing_files:
            return {
                "status": "error",
                "phase": "phase1_validate_inputs",
                "as_of_date": as_of_date,
                "daily_folder_id": daily_folder_id,
                "found_files": found_file_names,
                "missing_files": missing_files,
                "extra_files": sorted(list(found_set - set(REQUIRED_FILES))),
            }

        # Phase2: Fact_Long生成
        file_id_by_name = {
            f["name"]: f["id"]
            for f in children
            if f.get("mimeType") != "application/vnd.google-apps.folder"
        }

        frames = []
        for filename in REQUIRED_FILES:
            file_id = file_id_by_name[filename]
            metric = METRIC_BY_FILENAME[filename]

            xbytes = _download_file_bytes(drive, file_id)
            df = _read_excel_from_bytes(xbytes)
            df = _normalize_to_agent_value(df)

            df["as_of_date"] = as_of_date
            df["metric"] = metric
            frames.append(df)

        fact_long = pd.concat(frames, ignore_index=True)
        preview = fact_long.head(5).to_dict(orient="records")

        return {
            "status": "ok",
            "phase": "phase2_build_fact_long",
            "as_of_date": as_of_date,
            "daily_folder_id": daily_folder_id,
            "fact_long_rows": int(len(fact_long)),
            "fact_long_preview": preview,
            "extra_files": sorted(list(found_set - set(REQUIRED_FILES))),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Phase2 failed: {type(e).__name__}: {e}")
