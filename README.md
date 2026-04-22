# 消費者物価指数モニタリング
目的：CPIとニュースを用いて、価格変動の因果関係を理解し、日経TEST対策に活用する

※このサービスは、政府統計総合窓口(e-Stat)のAPI機能を使用していますが、サービスの内容は国によって保証されたものではありません。

## セットアップ

`.env` に以下を設定してください。

- `APP_ID`: e-Stat API のアプリID
- `STATS_DATA_ID`: CPI統計のデータセットID

## 使い方

### 1) スナップショット取得

```bash
python main.py fetch
```

特定の終端年月まで取得する場合:

```bash
python main.py fetch --year 2026 --month 3
```

### 2) 月次サマリレポート生成

```bash
python main.py report --year 2026 --month 3
```

出力先:

- `reports/YYYY-MM_report.md`

## GitHub Actions

`.github/workflows/main.yml` では公表タイミングにあわせて以下を実行します。

1. `python main.py fetch`
2. `python main.py report`

更新があれば `data/snapshot` と `reports` をコミットします。


