# ----------------------------
# Main Endpoint
# ----------------------------
@app.post("/run_daily_close")
def run_daily_close():

    input_folder_id = os.environ.get("DRIVE_INPUT_FOLDER_ID")
    template_file_id = os.environ.get("DRIVE_TEMPLATE_FILE_ID")

    if not input_folder_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_INPUT_FOLDER_ID")
    if not template_file_id:
        raise HTTPException(status_code=500, detail="Missing env: DRIVE_TEMPLATE_FILE_ID")

    jst = ZoneInfo("Asia/Tokyo")
    as_of_date_dt = datetime.now(jst).date() - timedelta(days=1)
    as_of_date = as_of_date_dt.strftime("%Y-%m-%d")

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
                "missing_files": missing_files,
            }

        # Phase2: Data読み込み
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

        # Phase3: テンプレ取得 -> Excel生成
        template_bytes = _download_file_bytes(drive, template_file_id)
        output_excel_bytes = _build_output_excel_bytes(
            template_bytes,
            fact_daily,
            fact_long,
        )

        # ===== 要約生成 =====
        try:
            summary = generate_summary(fact_daily)
        except Exception as e:
            summary = f"要約生成に失敗しました: {type(e).__name__}: {e}"

        # ===== メール本文 =====
        attach_name = f"{as_of_date}_前日確定版_実績.xlsx"
        subject = f"[前日確定版] {as_of_date} 実績レポート"

        body = (
            f"{as_of_date} の前日確定版レポートを生成しました。\n"
            f"添付ファイルをご確認ください。\n\n"
            f"▼ 5行要約\n"
            f"{summary}\n"
        )

        _send_mail_with_attachment(
            subject=subject,
            body=body,
            attachment_bytes=output_excel_bytes,
            attachment_filename=attach_name,
        )

        return {
            "status": "ok",
            "phase": "phase3_mail_sent",
            "as_of_date": as_of_date,
            "fact_daily_rows": int(len(fact_daily)),
            "fact_long_rows": int(len(fact_long)),
            "attachment_filename": attach_name,
        }

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Run failed: {type(e).__name__}: {e}",
        )
