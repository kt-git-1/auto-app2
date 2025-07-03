# auto-app2

ENA (European Nucleotide Archive) プロジェクトのリードデータを自動でダウンロードし、Nextflow パイプラインでマージ・解析する自動化ツールです。

## 機能

- ENA API から指定プロジェクトのリードデータ情報を取得
- FTP 経由で .fastq.gz ファイルを自動ダウンロード（並列ダウンロード対応）
- Nextflow スクリプトによるファイルのマージ・解析を自動実行

## ディレクトリ構成

```
auto-app2/
├── main.py                # メインスクリプト
├── nextflow               # Nextflow 実行ファイル
├── nextflow_conf/         # Nextflow スクリプト格納ディレクトリ
│   ├── nextflow_merge_script.nf
│   └── nextflow_run_script.nf
├── data/
│   ├── row_data/          # ダウンロードした生データ
│   └── output_data/       # 解析結果データ
├── requirements.txt       # Python依存パッケージ
└── README.md
```

## セットアップ

1. **依存パッケージのインストール**

   Python 3.8 以上推奨。  
   仮想環境推奨（例: `python -m venv auto-app-env`）

   ```sh
   pip install -r requirements.txt
   ```

2. **実行権限の付与（必要な場合）**

   ```sh
   chmod +x nextflow
   ```

3. **Nextflow のセットアップ**

   `nextflow` 実行ファイルは `main.py` と同じディレクトリに配置してください。

## 使い方

```sh
python main.py [--project_accession PRJEBxxxx] [--base_dir ...] [--output_dir ...] [--workers N]
```

### 例

```sh
python main.py --project_accession PRJEB19970
```

- デフォルトで `data/row_data` にダウンロード、`data/output_data` に解析結果を保存します。
- Nextflow スクリプトは `nextflow_conf/` 配下のものを使用します。

### 主なオプション

- `--project_accession` : ENA プロジェクト番号（デフォルト: PRJEB19970）
- `--base_dir` : 生データ保存先ディレクトリ
- `--output_dir` : 解析結果保存先ディレクトリ
- `--workers` : 並列ダウンロード数（デフォルト: 4）

## 注意事項

- ENA サーバーや FTP サーバーの仕様変更により動作しなくなる場合があります。
- Nextflow スクリプトの内容は `nextflow_conf/` ディレクトリで管理してください。