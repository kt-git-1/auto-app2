# auto-app2

## 概要

**auto-app2** は、NGS（次世代シーケンス）データのダウンロードからマッピング、BAM処理、softclipping、variant calling、各種QC（mapDamage, Qualimap）までを自動化するパイプラインです。  
古DNAやゲノム解析のワークフローを効率化し、再現性の高い解析を実現します。

- **対応OS:** macOS, Linux
- **主な機能:** ENAからのデータ取得、BWAマッピング、BAM処理、softclipping、variant calling、各種QC、VCFファイルの出力

---

## 目次

- [特徴](#特徴)
- [ディレクトリ構成](#ディレクトリ構成)
- [セットアップ](#セットアップ)
  - [1. Condaのインストール](#1-condaのインストール)
  - [2. Conda環境の作成](#2-conda環境の作成)
  - [3. 追加依存パッケージのインストール](#3-追加依存パッケージのインストール)
  - [4. Apple Silicon対応](#4-apple-silicon対応)
- [使い方](#使い方)
- [ワークフロー詳細](#ワークフロー詳細)
- [各種ディレクトリ・ファイルの説明](#各種ディレクトリファイルの説明)
- [トラブルシューティング・FAQ](#トラブルシューティングfaq)

---

## 特徴

- **自動化**: データ取得から解析まで一括実行
- **再現性**: 設定ファイルによるパラメータ管理
- **拡張性**: モジュール構造で機能追加が容易

---

## ディレクトリ構成

```
auto-app2/
├── config.py                # 設定ファイル
├── environment.yml          # Conda環境定義ファイル
├── main.py                  # メインスクリプト
├── modules/                 # 機能別モジュール
│   ├── analyzers.py
│   ├── bam_processor.py
│   ├── bwa_mapper.py
│   ├── ena_downloader.py
│   ├── softclipper.py
│   └── __init__.py
├── data/                    # データ格納用ディレクトリ
│   ├── raw_data/            # 生データ（FASTQ等）
│   ├── reference/           # 参照ゲノム
│   ├── results/             # 解析結果
│   ├── logs/                # ログ
│   └── temp/                # 一時ファイル
├── requirements.txt         # Python依存パッケージ
└── README.md
```

---

## セットアップ

### 1. Condaのインストール

#### macOS

```sh
# Homebrewが未導入の場合
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
brew install --cask miniconda
```

#### Linux

```sh
# Minicondaのインストール例
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh
```

### 2. Conda環境の作成

```sh
conda env create -f environment.yml
conda activate auto-app2-env
```

### 3. 追加依存パッケージのインストール

```sh
pip install -r requirements.txt
```

### 4. Apple Silicon対応

Apple Silicon (M1~M4) Mac では、一部のバイオインフォマティクスツールが conda でインストールできない場合があります。
その場合は Homebrew や Github で個別にインストールしてください：

```sh
brew install bwa
brew install brewsci/bio/adapterremoval
brew install samtools
```

---

## 使い方

### 1. 設定ファイルの編集

`config.py` でパラメータやパスを設定します。  
例: 解析対象のプロジェクトID、参照ゲノムパス、出力ディレクトリなど。

### 2. メインスクリプトの実行

```sh
python3 main.py
```

---

## ワークフロー詳細

1. **ENAからデータをダウンロード**  
   - `modules/ena_downloader.py`  
   - configで指定したプロジェクトID/サンプルIDのFASTQを取得

2. **BWAでリードを参照ゲノムにマッピング**  
   - `modules/bwa_mapper.py`  
   - 参照ゲノム（FASTA）に対してBWA-mem等でマッピング

3. **BAMファイルのソート・重複除去**  
   - `modules/bam_processor.py`  
   - samtools, picard等を利用

4. **softclipping処理**  
   - `modules/softclipper.py`  
   - BAMファイルからsoftclippedリードを抽出・処理

5. **variant calling, mapDamage, Qualimap等の解析**  
   - `modules/analyzers.py`  
   - GATK, mapDamage, Qualimap等を呼び出し

6. **結果の保存・レポート生成**  
   - `data/results/` 以下に各種出力ファイルを保存

---

## 各種ディレクトリ・ファイルの説明

- `config.py`  
  - 解析パラメータやパスの設定ファイル
- `environment.yml`  
  - Conda環境の依存パッケージ定義
- `requirements.txt`  
  - Pythonパッケージの追加依存
- `main.py`  
  - パイプラインのエントリーポイント
- `modules/`  
  - 各種機能ごとのPythonモジュール
- `data/raw_data/`  
  - ダウンロードしたFASTQ等の生データ
- `data/reference/`  
  - 参照ゲノム（FASTA, FAI等）
- `data/results/`  
  - 解析結果（BAM, VCF, QCレポート等）
- `data/logs/`  
  - 実行ログ
- `data/temp/`  
  - 一時ファイル

---

## トラブルシューティング・FAQ

### Q. 依存パッケージのエラー
A. `conda activate auto-app2-env` を忘れていないか確認してください。  
   また、`pip install -r requirements.txt` を再実行してください。

### Q. 外部コマンドが見つからない（bwa, AdapterRemoval等）
A. Apple Silicon Mac の場合は、Homebrew でインストールしてください：
   ```sh
   brew install bwa
   brew install brewsci/bio/adapterremoval
   ```

### Q. データがダウンロードできない
A. configのプロジェクトIDやサンプルIDが正しいか、ネットワーク接続を確認してください。

### Q. BAM/VCF等の出力がない
A. ログファイル（`data/logs/`）を確認し、エラー内容を特定してください。

**備考:**  
- 詳細な使い方やパラメータ説明は、`config.py`や各モジュールのdocstringを参照してください。
- 実際の解析例や出力例も追記可能です。
- ご質問・ご要望はIssueまたは開発者(谷口: taniguchidev33@gmail.com)までご連絡ください。