import argparse
import json
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests


APP_ID = os.getenv("APP_ID")
STATS_DATA_ID = os.getenv("STATS_DATA_ID")
SNAPSHOT_DIR = Path("data/snapshot")
REPORT_DIR = Path("reports")


def fetch_cpi_raw(app_id, stats_id, year: str, month: str):
    """e-Stat APIから消費者物価指数のデータを取得"""
    url = "http://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"

    params = {
        "cdArea": "00000",
        "cdTime": f"{year}00{month}{month}",
        "appId": app_id,
        "lang": "J",
        "statsDataId": stats_id,
        "metaGetFlg": "Y",
        "cntGetFlg": "N",
        "explanationGetFlg": "Y",
        "annotationGetFlg": "Y",
        "sectionHeaderFlg": 1,
        "replaceSpCharFlg": 0,
    }

    print(f"--- 読み込み開始: {year}-{month} ---")
    response = requests.get(url, params=params, timeout=60)

    if response.status_code != 200:
        print(f"HTTP Error: {response.status_code}")
        print(response.text[:500])
        return None

    try:
        data_json = response.json()
    except json.JSONDecodeError as e:
        print("JSONパース失敗:", e)
        print(response.text[:1000])
        return None

    try:
        status = data_json["GET_STATS_DATA"]["RESULT"]["STATUS"]
        if status != 0:
            print("API Error:", data_json["GET_STATS_DATA"]["RESULT"])
            return None
    except KeyError:
        print("想定外のJSON構造です")
        return None

    try:
        values = data_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
        class_values = data_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"]
    except KeyError:
        print("必要データが見つかりません")
        return None

    print(f"--- 読み込み完了: {year}-{month} ---")
    return pd.DataFrame(values), pd.DataFrame(class_values)


def data_preprocessed_and_save(df, df_master, file_path: Path):
    """取得したデータを整形し、CSV出力"""
    tab_master = pd.DataFrame(df_master["CLASS"][df_master["@id"] == "tab"].values[0])
    cat_master = pd.DataFrame(df_master["CLASS"][df_master["@id"] == "cat01"].values[0])
    area_master = df_master["CLASS"][df_master["@id"] == "area"].values[0]
    time_master = df_master["CLASS"][df_master["@id"] == "time"].values[0]

    df_merged = df.merge(
        cat_master[["@code", "@name"]],
        left_on="@cat01",
        right_on="@code",
        how="left",
    ).drop(columns="@code")

    df_merged = df_merged.merge(
        tab_master[["@code", "@name"]],
        left_on="@tab",
        right_on="@code",
        how="left",
    ).drop(columns="@code")

    df_merged["地域"] = area_master["@name"]
    df_merged["年月"] = time_master["@name"]

    df_merged = df_merged.drop(columns=["@tab", "@cat01", "@area", "@time"])
    df_merged.columns = ["数値", "単位", "カテゴリ", "指数", "地域", "年月"]
    df_merged = df_merged[["指数", "年月", "地域", "カテゴリ", "数値", "単位"]]

    file_path.parent.mkdir(parents=True, exist_ok=True)
    df_merged.to_csv(file_path, index=False, header=True)


def month_iter(start_year: int, end_year: int, end_month: int):
    y = start_year
    while y <= end_year:
        last_m = end_month if y == end_year else 12
        for m in range(1, last_m + 1):
            yield y, m
        y += 1


def month_iter_from(start_year: int, start_month: int, end_year: int, end_month: int):
    y, m = start_year, start_month
    while (y < end_year) or (y == end_year and m <= end_month):
        yield y, m
        if m == 12:
            y += 1
            m = 1
        else:
            m += 1


def run_fetch(until_year: int | None = None, until_month: int | None = None):
    if not APP_ID or not STATS_DATA_ID:
        raise RuntimeError("APP_ID/STATS_DATA_ID が未設定です。")

    now = datetime.now()
    if until_year is None or until_month is None:
        target_year = now.year
        target_month = now.month - 1
        if target_month == 0:
            target_year -= 1
            target_month = 12
    else:
        target_year, target_month = until_year, until_month

    for y, m in month_iter(1970, target_year, target_month):
        file_path = SNAPSHOT_DIR / f"{y}年{str(m).zfill(2)}月.csv"
        if file_path.exists():
            continue

        result = fetch_cpi_raw(APP_ID, STATS_DATA_ID, str(y), str(m).zfill(2))
        if result is None:
            print(f"スキップ: {y}年{m}月")
            continue

        df_raw, class_master = result
        data_preprocessed_and_save(df_raw, class_master, file_path)
        print(f"保存完了: {file_path}")


def parse_ym_from_filename(path: Path):
    m = re.match(r"(\d{4})年(\d{2})月\.csv$", path.name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def load_snapshot_df(year: int, month: int) -> pd.DataFrame:
    path = SNAPSHOT_DIR / f"{year}年{str(month).zfill(2)}月.csv"
    if not path.exists():
        raise FileNotFoundError(f"snapshot not found: {path}")

    df = pd.read_csv(path)
    df = df.rename(columns={"指数": "種別"})
    df["数値"] = pd.to_numeric(df["数値"], errors="coerce")
    parts = df["カテゴリ"].astype(str).str.split(" ", n=1, expand=True)
    df["カテゴリコード"] = parts[0]
    df["カテゴリ名"] = parts[1].fillna(parts[0])
    return df


def get_value(df: pd.DataFrame, metric_type: str, code: str):
    r = df[(df["種別"] == metric_type) & (df["カテゴリコード"] == code)]
    if r.empty:
        return None
    return float(r.iloc[0]["数値"])


def get_label(df: pd.DataFrame, code: str, fallback: str):
    r = df[df["カテゴリコード"] == code]
    if r.empty:
        return fallback
    return str(r.iloc[0]["カテゴリ名"])


def prev_month(year: int, month: int):
    if month == 1:
        return year - 1, 12
    return year, month - 1


def calc_headline(target_df, prev_df, yoy_df):
    defs = [
        ("総合CPI", "0001"),
        ("コアCPI（生鮮除く）", "0161"),
        ("コアコアCPI（生鮮・エネルギー除く）", "0178"),
    ]
    rows = []
    for label, code in defs:
        now_v = get_value(target_df, "指数", code)
        prev_v = get_value(prev_df, "指数", code)
        yoy_v = get_value(yoy_df, "指数", code)
        yoy_rate = get_value(target_df, "前年同月比", code)
        diff = None if (now_v is None or prev_v is None) else now_v - prev_v

        rows.append(
            {
                "指標": label,
                "今月": now_v,
                "前月": prev_v,
                "前年同月": yoy_v,
                "前月差": diff,
                "前年同月比": yoy_rate,
            }
        )
    return rows


def calc_categories(target_df):
    defs = [
        ("食料", "0002"),
        ("住居", "0045"),
        ("光熱・水道", "0054"),
        ("家具・家事用品", "0060"),
        ("被服及び履物", "0082"),
        ("保健医療", "0107"),
        ("交通・通信", "0111"),
        ("教育", "0118"),
        ("教養娯楽", "0122"),
        ("諸雑費", "0145"),
    ]
    rows = []
    for label, code in defs:
        rows.append(
            {
                "カテゴリ": label,
                "指数": get_value(target_df, "指数", code),
                "前月比": get_value(target_df, "前月比・前年比・前年度比", code),
                "前年同月比": get_value(target_df, "前年同月比", code),
            }
        )
    return rows


def calc_contributors(target_df):
    # 寄与度データが直接ないため、主要項目の前年比から上位/下位を抽出
    candidate_defs = [
        ("電気代", "0056"),
        ("米類", "0004"),
        ("外食", "0042"),
        ("通信料（携帯電話）", "7430"),
        ("テレビ", "9013"),
        ("ガソリン", "7301"),
    ]

    candidates = []
    for label, code in candidate_defs:
        yoy = get_value(target_df, "前年同月比", code)
        if yoy is None:
            continue
        candidates.append({"項目": label, "コード": code, "前年同月比": yoy})

    ups_raw = sorted([r for r in candidates if r["前年同月比"] > 0], key=lambda x: x["前年同月比"], reverse=True)[:3]
    downs_raw = sorted([r for r in candidates if r["前年同月比"] < 0], key=lambda x: x["前年同月比"])[:3]

    ups = [
        {
            "順位": i,
            "項目": r["項目"],
            "前年同月比": r["前年同月比"],
            "コメント": "前年比の上昇が目立つ項目",
        }
        for i, r in enumerate(ups_raw, start=1)
    ]

    downs = [
        {
            "順位": i,
            "項目": r["項目"],
            "前年同月比": r["前年同月比"],
            "コメント": "前年比の下押し寄与が大きい項目",
        }
        for i, r in enumerate(downs_raw, start=1)
    ]

    return ups, downs


def load_recent_monthly_values(code: str, metric_type: str, months: int = 12):
    pairs = []
    for p in sorted(SNAPSHOT_DIR.glob("*.csv")):
        ym = parse_ym_from_filename(p)
        if ym:
            pairs.append((ym, p))

    values = []
    for _, p in pairs[-months:]:
        df = load_snapshot_df(*parse_ym_from_filename(p))
        v = get_value(df, metric_type, code)
        if v is not None:
            values.append(v)
    return values


def evaluate_trend(target_df, year: int, month: int):
    # 3か月連続プラス（総合CPIの前月比）
    plus_3 = True
    y, m = year, month
    for _ in range(3):
        df = load_snapshot_df(y, m)
        mm = get_value(df, "前月比・前年比・前年度比", "0001")
        if mm is None or mm <= 0:
            plus_3 = False
            break
        y, m = prev_month(y, m)

    # 12か月トレンド
    total_yoy_series = load_recent_monthly_values("0001", "前年同月比", 12)
    core_yoy_series = load_recent_monthly_values("0161", "前年同月比", 12)
    total_avg = sum(total_yoy_series) / len(total_yoy_series) if total_yoy_series else 0
    core_avg = sum(core_yoy_series) / len(core_yoy_series) if core_yoy_series else 0

    # 異常値（光熱・水道）
    util_series = load_recent_monthly_values("0054", "前年同月比", 12)
    util_now = get_value(target_df, "前年同月比", "0054")
    util_avg = sum(util_series) / len(util_series) if util_series else 0
    anomaly = util_now is not None and util_now > (util_avg + 2.0)

    short_trend_text = "総合CPIは3か月連続で前月比プラス。" if plus_3 else "総合CPIの前月比は直近3か月で一部マイナス月あり。"
    long_trend_text = f"直近12か月平均で、総合CPI前年比は{total_avg:.1f}%、コアCPI前年比は{core_avg:.1f}%。"

    anomaly_text = "光熱・水道の前年比は直近12か月平均を上回り、要注意。" if anomaly else "顕著な異常値は確認されず。"
    return plus_3, short_trend_text, long_trend_text, anomaly, anomaly_text


def judge_inflation(target_df, plus_3: bool):
    total_yoy = get_value(target_df, "前年同月比", "0001") or 0
    core_yoy = get_value(target_df, "前年同月比", "0161") or 0
    core_core_yoy = get_value(target_df, "前年同月比", "0178") or 0

    if total_yoy > 2.0 and core_yoy > 1.5 and plus_3:
        overall = "インフレ継続"
    elif total_yoy > 0 and core_yoy > 0:
        overall = "緩やかなインフレ"
    else:
        overall = "インフレ鈍化"

    reason = (
        f"総合CPI前年比{total_yoy:.1f}%、コアCPI前年比{core_yoy:.1f}%、"
        f"コアコアCPI前年比{core_core_yoy:.1f}%。"
    )
    return overall, reason


def fmt(v, nd=1):
    if v is None or pd.isna(v):
        return "N/A"
    return f"{v:.{nd}f}"


def fmt_signed(v, nd=1):
    if v is None or pd.isna(v):
        return "N/A"
    return f"{v:+.{nd}f}"


def build_report_markdown(year: int, month: int):
    target_df = load_snapshot_df(year, month)
    py, pm = prev_month(year, month)
    prev_df = load_snapshot_df(py, pm)
    yoy_df = load_snapshot_df(year - 1, month)

    headline = calc_headline(target_df, prev_df, yoy_df)
    categories = calc_categories(target_df)
    ups, downs = calc_contributors(target_df)
    plus_3, short_trend, long_trend, _, anomaly_text = evaluate_trend(target_df, year, month)
    overall, reason = judge_inflation(target_df, plus_3)

    inflation_direction = "緩やかな上昇継続" if overall != "インフレ鈍化" else "鈍化"
    created_at = datetime.now().strftime("%Y-%m-%d %H:%M JST")

    lines = []
    lines.append("# 月次消費者物価指数サマリレポート")
    lines.append("")
    lines.append("## 1. レポート基本情報")
    lines.append(f"- 対象年月: {year}年{month}月")
    lines.append(f"- 作成日時: {created_at}")
    lines.append("- データソース: e-Stat 消費者物価指数")
    lines.append("- 備考: 全国・総合指数ベース")
    lines.append("")

    lines.append("## 2. エグゼクティブサマリー")
    lines.append("- 総括: 総合・コアともに前年同月比でプラス圏を維持。")
    lines.append(f"- インフレ方向感: {inflation_direction}")
    lines.append("- 主な押し上げ要因: 食料、光熱・水道")
    lines.append("- 主な押し下げ要因: 交通・通信の一部項目")
    lines.append("")

    lines.append("## 3. ヘッドライン指標")
    lines.append("| 指標 | 今月 | 前月 | 前年同月 | 前月差 | 前年同月比 |")
    lines.append("|------|------|------|----------|--------|------------|")
    for r in headline:
        lines.append(
            f"| {r['指標']} | {fmt(r['今月'])} | {fmt(r['前月'])} | {fmt(r['前年同月'])} | {fmt_signed(r['前月差'])} | {fmt_signed(r['前年同月比'])}% |"
        )
    lines.append("")

    lines.append("## 4. 主要カテゴリ動向")
    lines.append("| カテゴリ | 指数 | 前月比 | 前年同月比 | 備考 |")
    lines.append("|----------|------|--------|------------|------|")
    for r in categories:
        memo = "上昇" if (r["前年同月比"] or 0) > 1.5 else "安定"
        if (r["前年同月比"] or 0) < 0:
            memo = "下落"
        lines.append(
            f"| {r['カテゴリ']} | {fmt(r['指数'])} | {fmt_signed(r['前月比'])}% | {fmt_signed(r['前年同月比'])}% | {memo} |"
        )
    lines.append("")

    lines.append("## 5. 上昇・下落寄与の大きい項目")
    lines.append("### 上昇寄与上位")
    lines.append("| 順位 | 項目 | 前年同月比 | コメント |")
    lines.append("|------|------|------------|----------|")
    for r in ups:
        lines.append(f"| {r['順位']} | {r['項目']} | {fmt_signed(r['前年同月比'])}% | {r['コメント']} |")
    lines.append("")

    lines.append("### 下落寄与上位")
    lines.append("| 順位 | 項目 | 前年同月比 | コメント |")
    lines.append("|------|------|------------|----------|")
    for r in downs:
        lines.append(f"| {r['順位']} | {r['項目']} | {fmt_signed(r['前年同月比'])}% | {r['コメント']} |")
    lines.append("")

    lines.append("## 6. トレンド評価")
    lines.append(f"- 直近3か月の傾向: {short_trend}")
    lines.append(f"- 直近12か月の傾向: {long_trend}")
    lines.append("- 特記事項: エネルギー・食料の動向は家計負担に直結するため注視。")
    lines.append(f"- 異常値検知: {anomaly_text}")
    lines.append("")

    lines.append("## 7. 機械判定サマリ")
    lines.append(f"- 総合判定: {overall}")
    lines.append(f"- 判定理由: {reason}")
    lines.append("- 注意カテゴリ: 光熱・水道、食料")
    lines.append("- 翌月ウォッチポイント: エネルギー価格の継続性、食料価格の波及、通信料の下押し継続有無")
    lines.append("")

    lines.append("## 8. 付録")
    lines.append("- 判定ルール:")
    lines.append("  - 総合CPI前年同月比 > 2.0% → 強めのインフレ")
    lines.append("  - コアCPI前年同月比 > 1.5% → 基調インフレ継続")
    lines.append("  - 3か月連続で前月比プラス → 短期上昇トレンド")
    lines.append("- 用語定義:")
    lines.append("  - 総合CPI: 全品目を含む消費者物価指数")
    lines.append("  - コアCPI: 生鮮食品を除く総合")
    lines.append("  - コアコアCPI: 生鮮食品及びエネルギーを除く総合")
    lines.append("")
    return "\n".join(lines)


def run_report(year: int | None = None, month: int | None = None):
    now = datetime.now()
    latest_year = now.year
    latest_month = now.month - 1
    if latest_month == 0:
        latest_year -= 1
        latest_month = 12

    if year is None or month is None:
        start_year, start_month = latest_year, latest_month
    else:
        start_year, start_month = year, month

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    for y, m in month_iter_from(start_year, start_month, latest_year, latest_month):
        out = REPORT_DIR / f"{y}-{str(m).zfill(2)}_report.md"
        if out.exists():
            continue

        try:
            md = build_report_markdown(y, m)
        except FileNotFoundError as e:
            print(f"スキップ: {y}年{m}月 ({e})")
            continue

        out.write_text(md, encoding="utf-8")
        print(f"レポートを出力しました: {out}")


def parse_args():
    parser = argparse.ArgumentParser(description="CPI Data Fetcher & Monthly Report Generator")
    sub = parser.add_subparsers(dest="command")

    fetch_cmd = sub.add_parser("fetch", help="e-Statからスナップショットを取得")
    fetch_cmd.add_argument("--year", type=int, help="取得終端年")
    fetch_cmd.add_argument("--month", type=int, help="取得終端月")

    report_cmd = sub.add_parser("report", help="月次サマリレポートを生成")
    report_cmd.add_argument("--year", type=int, help="出力開始年")
    report_cmd.add_argument("--month", type=int, help="出力開始月")

    return parser.parse_args()


def main():
    args = parse_args()

    # 互換性維持: サブコマンドなしは fetch 扱い
    if args.command in (None, "fetch"):
        run_fetch(getattr(args, "year", None), getattr(args, "month", None))
    elif args.command == "report":
        run_report(args.year, args.month)


if __name__ == "__main__":
    main()