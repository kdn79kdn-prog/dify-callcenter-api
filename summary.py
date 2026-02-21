import pandas as pd
import numpy as np

# ----------------------------
# 固定KPI（あなたの前提）
# ----------------------------
FIXED_TARGETS = {
    "CPH": 4.0,
    "AHT": 900.0,
    "ATT": 750.0,
    "ACW": 250.0,
    "着座比率": 0.90,  # 90%
    "稼働率": 0.97,    # 97%
}

LOWER_IS_BETTER = {"AHT", "ATT", "ACW"}  # 小さいほど良い指標


def _to_num(s):
    return pd.to_numeric(s, errors="coerce")


def _as_ratio(x: float) -> float:
    # 0-1 or 0-100 を吸収
    if x is None or not np.isfinite(x):
        return np.nan
    return x / 100.0 if x > 1.5 else x


def _pct(x: float) -> str:
    return "n/a" if not np.isfinite(x) else f"{x*100:.1f}%"


def _fmt_num(x: float, decimals: int = 2) -> str:
    if x is None or not np.isfinite(x):
        return "n/a"
    return f"{x:.{decimals}f}"


def _achievement(actual: float, target: float, lower_is_better: bool) -> float:
    if not np.isfinite(actual) or not np.isfinite(target) or target == 0:
        return np.nan
    if lower_is_better:
        return target / actual if actual != 0 else np.nan
    return actual / target


def generate_summary(fact_daily: pd.DataFrame, as_of_date: str) -> str:
    if fact_daily is None or fact_daily.empty:
        return "データがありません。"

    df = fact_daily.copy()

    # 前日分だけに絞る
    if "日付" not in df.columns:
        return "要約生成不可：列不足 ['日付']"
    df["日付"] = df["日付"].astype(str).str.strip()
    target_date = str(as_of_date).strip()
    df = df[df["日付"] == target_date]

    if df.empty:
        return f"{target_date} のデータが見つかりません（日付フィルタ後0件）"

    # ----------------------------
    # 1) CPD（列のCPD目標で集計）
    # ----------------------------
    need_cpd = {"CPD", "CPD目標"}
    if not need_cpd.issubset(df.columns):
        cpd_line1 = "1) 受電実績：CPD（列不足で集計不可）"
        cpd_line2 = "2) CPD未達（人数）：（列不足で集計不可）"
    else:
        df["cpd_act"] = _to_num(df["CPD"])
        df["cpd_tgt"] = _to_num(df["CPD目標"])

        target_mask = df["cpd_tgt"].notna()
        target_n = int(target_mask.sum())

        cpd_act_sum = float(df.loc[target_mask, "cpd_act"].sum(skipna=True))
        cpd_plan_sum = float(df.loc[target_mask, "cpd_tgt"].sum(skipna=True))
        cpd_rate = (cpd_act_sum / cpd_plan_sum) if cpd_plan_sum else np.nan

        hit_mask = target_mask & (df["cpd_act"] >= df["cpd_tgt"])
        hit_n = int(hit_mask.sum())
        miss_n = int(target_n - hit_n)
        hit_rate = hit_n / target_n if target_n else np.nan

        cpd_line1 = f"1) 受電実績：CPD {cpd_act_sum:.0f} / 計画 {cpd_plan_sum:.0f}（達成率 {_pct(cpd_rate)}）"
        cpd_line2 = f"2) CPD未達（人数）：未達 {miss_n}人 / 対象 {target_n}人（達成者率 {_pct(hit_rate)}）"

    # ----------------------------
    # 3) 生産性（CPH）
    # ----------------------------
    cph_act = _to_num(df["CPH"]).mean() if "CPH" in df.columns else np.nan
    cph_tgt = FIXED_TARGETS["CPH"]
    cph_rate = _achievement(cph_act, cph_tgt, lower_is_better=False)
    line3 = f"3) 生産性：CPH {_fmt_num(cph_act,2)} / 目標 {cph_tgt:.2f}（達成率 {_pct(cph_rate)}）"

    # ----------------------------
    # 4) 品質/効率（AHT・ATT・ACW）※小さいほど良い
    # 4行に詰め込む（見やすさ優先）
    # ----------------------------
    def one_lower_metric(name: str) -> str:
        act = _to_num(df[name]).mean() if name in df.columns else np.nan
        tgt = FIXED_TARGETS[name]
        rate = _achievement(act, tgt, lower_is_better=True)
        # 秒の指標なので小数なしで見せる
        act_txt = "n/a" if not np.isfinite(act) else f"{act:.0f}"
        return f"{name} {act_txt}/{tgt:.0f}({_pct(rate)})"

    aht_txt = one_lower_metric("AHT")
    att_txt = one_lower_metric("ATT")
    acw_txt = one_lower_metric("ACW")
    line4 = f"4) 時間系：{aht_txt} / {att_txt} / {acw_txt}"

    # ----------------------------
    # 5) 稼働（着座比率・稼働率）
    # ----------------------------
    seat_act_raw = _to_num(df["着座比率"]).mean() if "着座比率" in df.columns else np.nan
    seat_act = _as_ratio(float(seat_act_raw)) if np.isfinite(seat_act_raw) else np.nan
    seat_tgt = FIXED_TARGETS["着座比率"]
    seat_rate = _achievement(seat_act, seat_tgt, lower_is_better=False)

    occ_act_raw = _to_num(df["稼働率"]).mean() if "稼働率" in df.columns else np.nan
    occ_act = _as_ratio(float(occ_act_raw)) if np.isfinite(occ_act_raw) else np.nan
    occ_tgt = FIXED_TARGETS["稼働率"]
    occ_rate = _achievement(occ_act, occ_tgt, lower_is_better=False)

    line5 = (
        f"5) 稼働系：着座 {_pct(seat_act)}/{_pct(seat_tgt)}({_pct(seat_rate)})"
        f" / 稼働 {_pct(occ_act)}/{_pct(occ_tgt)}({_pct(occ_rate)})"
    )

    return "\n".join([cpd_line1, cpd_line2, line3, line4, line5])
