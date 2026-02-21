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
            }

        # ==========================
        # Phase2: Fact_Long生成
        # ==========================

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
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Phase2 failed: {type(e).__name__}: {e}",
        )

