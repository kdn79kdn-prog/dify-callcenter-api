import pandas as pd
import numpy as np


def _to_num(s):
    return pd.to_numeric(s, errors="coerce")


def generate_summary(fact_daily: pd.DataFrame) -> str:
    if fact_daily.empty:
        return "データがありません。"

    required_cols = {"CPD", "CPD目標"}
    missing = [c for c in required_cols if c not in fact_daily.columns]
    if missing:
        return f"要約生成不可：列不足 {missing}"

    df = fact_daily.copy()

    df["cpd_act"] = _to_num(df["CPD"])
    df["cpd_tgt"] = _to_num(df["CPD目標"])

    target_mask = df["cpd_tgt"].notna()
    target_n = int(target_mask.sum())

    if target_n == 0:
        return "CPD目標が未設定です。"

    cpd_act_sum = float(df.loc[target_mask, "cpd_act"].sum(skipna=True))
    cpd_plan_sum = float(df.loc[target_mask, "cpd_tgt"].sum(skipna=True))

    if cpd_plan_sum == 0:
        rate = np.nan
    else:
        rate = cpd_act_sum / cpd_plan_sum

    hit_mask = target_mask & (df["cpd_act"] >= df["cpd_tgt"])
    hit_n = int(hit_mask.sum())
    miss_n = target_n - hit_n
    hit_rate = hit_n / target_n if target_n else np.nan

    def pct(x):
        return "n/a" if not np.isfinite(x) else f"{x*100:.1f}%"

    summary_lines = [
        f"1) 受電実績：CPD {cpd_act_sum:.0f} / 計画 {cpd_plan_sum:.0f}（達成率 {pct(rate)}）",
        f"2) CPD未達（人数）：未達 {miss_n}人 / 対象 {target_n}人（達成者率 {pct(hit_rate)}）",
        "3) 生産性：未実装",
        "4) 稼働：未実装",
        "5) 着座：未実装",
    ]

    return "\n".join(summary_lines)
