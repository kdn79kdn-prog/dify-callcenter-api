import os
import json
from datetime import datetime, timedelta
from io import BytesIO
from typing import Any, Dict, List, Tuple, Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from zoneinfo import ZoneInfo

from openpyxl import load_workbook

app = FastAPI()

# ----------------------------
# 設定
# ----------------------------
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

# INPUT共通列（実データ前提）
BASE_COLS = ["日付", "agent_id", "氏名", "勤務区分", "実働時間(h)", "CPD目標"]

# テンプレシート名（違うならここだけ変える）
SHEET_FACT_DAILY = "Fact_Daily"
SHEET_FACT_LONG = "Fact_Long"


# ----------------------------
# API
# ----------------------------
@app.get("/health")
def health():
    return {"status": "ok", "version": "2026-02-21-Phase3-MonthOverwrite-01"}


# ----------------------------
# Drive
# ----------------------------
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


def _find_child_folder_by_name(drive, parent_folder_id: str, folder_name: str) -> Optional[Dict[str, str]]:
    q = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType = 'application/vnd.google-apps.folder' and "
        f"name = '{folder_name}' and trashed = false"
    )
    res = drive.files().list(q=q, fields="files(id,name)", pageSize=10).execute()
    files = res.get("files", [])
    return files[0] if files else None


def _ensure_child_folder(drive, parent_folder_id: str, folder_name: str) -> Dict[str, str]:
    existing = _find_child_folder_by_name(drive, parent_folder_id, folder_name)
    if existing:
        return existing

    metadata = {
        "name": folder_name,
        "mimeType": "application/vnd.google-apps.folder",
        "parents": [parent_folder_id],
    }
    created = drive.files().create(body=metadata, fields="id,name").execute()
    return {"id": created["id"], "name": created["name"]}


def _find_child_file_by_name_latest(drive, parent_folder_id: str, filename: str) -> Optional[Dict[str, str]]:
    """
    フォルダ直下の同名ファイルを探し、modifiedTimeが新しいものを返す。
    """
    # nameにシングルクォートがあると壊れるので簡易エスケープ
    safe_name = filename.replace("'", "\\'")
    q = (
        f"'{parent_folder_id}' in parents and "
        f"mimeType != 'application/vnd.google-apps.folder' and "
        f"name = '{safe_name}' and trashed = false"
    )
    res = drive.files().list(
        q=q,
        fields="files(id,name,modifiedTime)",
        pageSize=10,
        orderBy="modifiedTime desc",
    ).execute()
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
        _, done = downloader.next_chunk()
    return fh.getvalue()


def _upsert_bytes_to_drive(
    drive,
    parent_folder_id: str,
    filename: str,
    content: bytes,
    mimetype: str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
) -> Dict[str, Any]:
    """
    同名ファイルがあれば update（上書き）、なければ create。
    """
    existing = _find_child_file_by_name_latest(drive, parent_folder_id, filename)

    media = MediaIoBaseUpload(BytesIO(content), mimetype=mimetype, resumable=False)

    if existing:
        updated = drive.files().update(
            fileId=existing["id"],
            media_body=media,
            body={"name": filename},
            fields="id,name,modifiedTime",
        ).execute()
        return {"mode": "updated", "id": updated["id"], "name": updated["name"], "modifiedTime": updated.get("modifiedTime")}
    else:
        created = drive.files().create(
            body={"name": filename, "parents": [parent_folder_id]},
            media_body=media,
            fields="id,name,modifiedTime",
        ).execute()
        return {"mode": "created", "id": created["id"], "name": created["name"], "modifiedTime": created.get("modifiedTime")}


# ----------------------------
# Excel -> DataFrame（Phase2）
# ----------------------------
def _read_excel_from_bytes(xbytes: bytes, sheet_name: str = "Data") -> pd.DataFrame:
    bio = BytesIO(xbytes)
    try:
        return pd.read_excel(bio, sheet_name=sheet_name)
    except ValueError:
        bio.seek(0)
        return pd.read_excel(bio)  # fallback


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().replace("　", " ") for c in df.columns]
    return df


def _normalize_common_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = _clean_columns(df)

    rename_map = {}
    for c in df.columns:
        c2 = str(c).strip()
        if c2 in ["エージェントID", "エージェントId", "agent_id", "AgentID", "AGENT_ID", "ID"]:
            rename_map[c] = "agent_id"

    df = df.rename(columns=rename_map)

    missing = [c for c in BASE_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing base columns: {missing}. found={list(df.columns)}")

    df["agent_id"] = df["agent_id"].astype(str).str.strip()
    return df


def _to_numeric_series(s: pd.Series) -> pd.Series:
    x = s.astype(str)
    x = x.str.replace("%", "", regex=False).str.replace(",", "", regex=False).str.strip()
    return pd.to_numeric(x, errors="coerce")


def _extract_metric_series(raw_df: pd.DataFrame, metric_name: str) -> pd.DataFrame:
    df = _normalize_common_columns(raw_df)

    candidates = [c for c in df.columns if c not in BASE_COLS]

    if metric_name in df.columns:
        metric_col = metric_name
    else:
        if len(candidates) != 1:
            raise ValueError(
                f"Cannot uniquely detect metric column for metric={metric_name}. "
                f"candidates={candidates}"
            )
        metric_col = candidates[0]

    out = df[["日付", "agent_id", metric_col]].copy()
    out = out.rename(columns={metric_col: metric_name})
    out[metric_name] = _to_numeric_series(out[metric_name])

    dup = out.duplicated(subset=["日付", "agent_id"], keep=False)
    if dup.any():
        sample = out.loc[dup, ["日付", "agent_id"]].head(5).to_dict(orient="records")
        raise ValueError(f"Duplicate keys in metric={metric_name} on (日付,agent_id). sample={sample}")

    return out


def _build_fact_daily_and_long(
    raw_by_metric: Dict[str, pd.DataFrame],
    as_of_date: str,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if "CPD" not in raw_by_metric:
        raise ValueError("CPD.xlsx is required as base but not provided.")

    base = _normalize_common_columns(raw_by_metric["CPD"])
    fact_daily = base[BASE_COLS].copy()

    metric_names = list(raw_by_metric.keys())

    for metric in metric_names:
        metric_df = _extract_metric_series(raw_by_metric[metric], metric)
        fact_daily = fact_daily.merge(
            metric_df,
            on=["日付", "agent_id"],
            how="left",
            validate="one_to_one",
        )

    fact_long = fact_daily.melt(
        id_vars=BASE_COLS,
        value_vars=metric_names,
        var_name="metric",
        value_name="actual",
    )
    fact_long["as_of_date"] = as_of_date
    fact_long["work_flag"] = (fact_long["勤務区分"].astype(str).str.strip() != "休み").astype(int)

    return fact_daily, fact_long


# ----------------------------
# openpyxl（Phase3）
# ----------------------------
def _clear_worksheet(ws):
    mr = ws.max_row
    mc = ws.max_column
    if mr < 1 or mc < 1:
        return
    for r in range(1, mr + 1):
        for c in range(1, mc + 1):
            ws.cell(row=r, column=c).value = None


def _write_df_to_sheet(ws, df: pd.DataFrame):
    cols = list(df.columns)
    for c, name in enumerate(cols, start=1):
        ws.cell(row=1, column=c).value = name

    values = df.where(pd.notnull(df), None).values.tolist()
    for r_idx, row in enumerate(values, start=2):
        for c_idx, v in enumerate(row, start=1):
            ws.cell(row=r_idx, column=c_idx).value = v


def _build_output_excel_bytes(template_bytes: bytes, fact_daily: pd.DataFrame, fact_long: pd.DataFrame) -> bytes:
    wb = load_workbook(BytesIO(template_bytes))

    if SHEET_FACT_DAILY not in wb.sheetnames:
        raise ValueError(f"Template missing sheet: {SHEET_FACT_DAILY}. sheets={wb.sheetnames}")
    if SHEET_FACT_LONG not in wb.sheetnames:
        raise ValueError(f"Template missing sheet: {SHEET_FACT_LONG}. sheets={wb.sheetnames}")

    ws_daily = wb[SHEET_FACT_DAILY]
    ws_long = wb[SHEET_FACT_LONG]

    _clear_worksheet(ws_daily)
    _write_df_to_sheet(ws_daily, fact_daily)

    _clear_worksheet(ws_long)
    _write_df_to_sheet(ws_long, fact_long)

    out = BytesIO()
    wb.save(out)
    return out.getvalue()


# ----------------------------
# Main Endpoint（Phase3: 月フォルダ + 上書き）
# ----------------------------
@app.post("/run_daily_close")
def run_daily_close():
    input_folder_id = os.environ.get("DRIVE_INPUT_FOLDER_ID")
    output_root_folder_id = os.environ.get("DRIVE_OUTPUT_FOLDER_ID")
    template_file_id = os.environ.get("DRIVE_TEMPLATE_FILE_ID")

    if not input_folder_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_INPUT_FOLDER_ID")
    if not output_root_folder_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_OUTPUT_FOLDER_ID")
    if not template_file_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_TEMPLATE_FILE_ID")

    jst = ZoneInfo("Asia/Tokyo")
    as_of_date_dt = datetime.now(jst).date() - timedelta(days=1)
    as_of_date = as_of_date_dt.strftime("%Y-%m-%d")
    month_key = as_of_date_dt.strftime("%Y-%m")  # ★月フォルダ

    try:
        drive = _get_drive_service()

        # Phase1: 前日フォルダ取得（INPUT/YYYY-MM-DD）
        daily_folder = _find_child_folder_by_name(drive, input_folder_id, as_of_date)
        if not daily_folder:
            raise HTTPException(
                status_code=409,
                detail=f"INPUT_NOT_READY: daily folder not found: {as_of_date}",
            )
        daily_folder_id = daily_folder["id"]

        children = _list_child_files(drive, daily_folder_id, page_size=200)

        found_file_names = sorted(
            [
                f["name"]
                for f in children
                if f.get("mimeType") != "application/vnd.google-apps.folder"
            ]
        )

        found_set = set(found_file_names)
        missing_files = [name for name in REQUIRED_FILES if name not in found_set]

        if missing_files:
            return {
                "status": "error",
                "phase": "phase1_validate_inputs",
                "as_of_date": as_of_date,
                "input_daily_folder_id": daily_folder_id,
                "found_files": found_file_names,
                "missing_files": missing_files,
                "extra_files": sorted(list(found_set - set(REQUIRED_FILES))),
            }

        # Phase2: Data読み込み -> Fact生成
        file_id_by_name = {
            f["name"]: f["id"]
            for f in children
            if f.get("mimeType") != "application/vnd.google-apps.folder"
        }

        raw_by_metric: Dict[str, pd.DataFrame] = {}
        for filename in REQUIRED_FILES:
            metric = METRIC_BY_FILENAME[filename]
            file_id = file_id_by_name[filename]
            xbytes = _download_file_bytes(drive, file_id)
            df = _read_excel_from_bytes(xbytes, sheet_name="Data")
            raw_by_metric[metric] = df

        fact_daily, fact_long = _build_fact_daily_and_long(raw_by_metric, as_of_date)

        # Phase3: テンプレ取得 -> 書き込み
        template_bytes = _download_file_bytes(drive, template_file_id)
        output_excel_bytes = _build_output_excel_bytes(template_bytes, fact_daily, fact_long)

        # OUTPUT/YYYY-MM フォルダを作成/取得（★月フォルダ）
        out_month_folder = _ensure_child_folder(drive, output_root_folder_id, month_key)

        # ファイル名（同名で上書きされる）
        out_filename = f"{month_key}_前日確定版_実績.xlsx"

        upserted = _upsert_bytes_to_drive(
            drive,
            parent_folder_id=out_month_folder["id"],
            filename=out_filename,
            content=output_excel_bytes,
        )

        return {
            "status": "ok",
            "phase": "phase3_month_folder_overwrite",
            "as_of_date": as_of_date,
            "month_key": month_key,
            "input_daily_folder_id": daily_folder_id,
            "output_month_folder": out_month_folder,
            "output_file": upserted,
            "fact_daily_rows": int(len(fact_daily)),
            "fact_long_rows": int(len(fact_long)),
            "fact_daily_preview": fact_daily.head(3).to_dict(orient="records"),
            "fact_long_preview": fact_long.head(5).to_dict(orient="records"),
            "extra_files": sorted(list(found_set - set(REQUIRED_FILES))),
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Run failed: {type(e).__name__}: {e}")
