import pandas as pd
import requests
import json
import os
from datetime import datetime
from pathlib import Path
import argparse

APP_ID = os.getenv("APP_ID")
STATS_DATA_ID = os.getenv("STATS_DATA_ID")


def get_target_year_month():
    parser = argparse.ArgumentParser()

    parser.add_argument("--year", type=int)
    parser.add_argument("--month", type=int)

    args = parser.parse_args()

    now = datetime.now()

    year = args.year if args.year is not None else now.year
    month = args.month if args.month is not None else now.month

    if int(month) == 1:
        month = 12
        year -= 1
    else:
        month = str(int(month)-1)

    return year, month


now = datetime.now()
year = now.strftime("%Y")
month = now.strftime("%m")
y, m = get_target_year_month()


def fetch_cpi_raw(app_id, stats_id, year=year, month=month):
    """e-Stat APIから消費者物価指数のデータを取得"""
    url = "http://api.e-stat.go.jp/rest/3.0/app/json/getStatsData"
    
    params = {
        "cdArea": "00000",
        # "cdTime": "2026000202",
        "cdTime": str(year)+str("00")+str(month)+str(month),
        "appId": app_id,
        "lang": "J",
        "statsDataId": stats_id,
        "metaGetFlg": "Y",  # メタ情報（品目名など）を含める
        "cntGetFlg": "N",
        "explanationGetFlg": "Y",
        "annotationGetFlg": "Y",
        "sectionHeaderFlg": 1,
        "replaceSpCharFlg": 0,
    }

    print("--- 読み込み開始 ---")
    response = requests.get(url, params=params)

    # --- HTTPエラーチェック ---
    if response.status_code != 200:
        print(f"HTTP Error: {response.status_code}")
        print(response.text[:500])
        return None

    # --- JSONパース（安全版） ---
    try:
        data_json = response.json()
    except json.JSONDecodeError as e:
        print("JSONパース失敗:", e)
        print("レスポンス内容:")
        print(response.text[:1000])
        return None

    # --- API内部エラー確認 ---
    try:
        status = data_json["GET_STATS_DATA"]["RESULT"]["STATUS"]
        if status != 0:
            print("API Error:", data_json["GET_STATS_DATA"]["RESULT"])
            return None
    except KeyError:
        print("想定外のJSON構造です")
        print(data_json)
        return None

    # --- VALUE抽出 ---
    try:
        values = data_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["DATA_INF"]["VALUE"]
    except KeyError:
        print("VALUEが見つかりません")
        print(data_json)
        return None

    # --- クラスマスタ抽出 ---
    try:
        class_values = data_json["GET_STATS_DATA"]["STATISTICAL_DATA"]["CLASS_INF"]["CLASS_OBJ"]
    except KeyError:
        print("品目マスタが見つかりません")
        print(data_json)
        return None

    # --- DataFrame化 ---
    df = pd.DataFrame(values)
    class_master = pd.DataFrame(class_values)

    print("--- 読み込み完了 ---")

    return df, class_master


def data_preprocessed_and_save(df, df_master, file_path):
    """取得したデータを整形し、CSV出力"""
    tab_master = pd.DataFrame(df_master["CLASS"][df_master["@id"] == "tab"].values[0])
    cat_master = pd.DataFrame(df_master["CLASS"][df_master["@id"] == "cat01"].values[0])
    area_master = df_master["CLASS"][df_master["@id"] == "area"].values[0]
    time_master = df_master["CLASS"][df_master["@id"] == "time"].values[0]

    df_merged = df.merge(
        cat_master[["@code", "@name"]],
        left_on="@cat01",
        right_on="@code",
        how="left"
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

    df_merged.to_csv(file_path, index=False, header=True)
    

while y <= int(year):
    for m in range(1, 13):
        print(f"{y}年{m}月")
        file_path = Path(f"data/snapshot/{y}年{str(m).zfill(2)}月.csv")

        if not file_path.exists():
            df_raw, class_master = fetch_cpi_raw(APP_ID, STATS_DATA_ID, str(y), str(m).zfill(2))
            data_preprocessed_and_save(df_raw, class_master, file_path)

            if y == int(year) & m == int(month)-1:
                break

    y += 1