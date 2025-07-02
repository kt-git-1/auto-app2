import requests
import os
from ftplib import FTP
from urllib.parse import urlparse
import subprocess

# 各パラメータを設定
PROJECT_ACCESSION = "プロジェクト番号"
BASE_DIR = "/Your Absolute Path/auto-app2/data/row_data"
NEXTFLOW_DIR = "/Your Absolute Path/auto-app2/nextflow"
NEXTFLOW_MERGE_SCRIPT = "/Your Absolute Path/auto-app2/nextflow_conf/nextflow_merge_script.nf"
NEXTFLOW_RUN_SCRIPT = "/Your Absolute Path/auto-app2/nextflow_conf/nextflow_run_script.nf"
OUTPUT_DIR = "/Your Absolute Path/auto-app2/data/output_data"

# プロジェクト番号ごとのディレクトリを作成
project_base_dir = os.path.join(BASE_DIR, PROJECT_ACCESSION)
project_output_dir = os.path.join(OUTPUT_DIR, PROJECT_ACCESSION)
os.makedirs(project_base_dir, exist_ok=True)
os.makedirs(project_output_dir, exist_ok=True)

# ENAのAPIにアクセスして入力したプロジェクトの情報取得
def get_api_response(PROJECT_ACCESSION):
    url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={PROJECT_ACCESSION}&result=read_run&fields=sample_accession,submitted_ftp&format=tsv"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        raise Exception(f"Error fetching data for project {PROJECT_ACCESSION}: {e}")

# 応答データを解析して、サンプルアクセッションごとにFTP URLをグループ化
def parse_response_data(response_data):
    sample_to_ftp_urls = {}
    lines = response_data.strip().split('\n')[1:]
    for line in lines:
        parts = line.split('\t')
        sample_acc, ftp_url = parts[0], parts[1]
        if sample_acc not in sample_to_ftp_urls:
            sample_to_ftp_urls[sample_acc] = []
        sample_to_ftp_urls[sample_acc].append(ftp_url)
    return sample_to_ftp_urls

# FTP URLからファイルをダウンロード
def download_from_ftp(ftp_url, destination):
    if not ftp_url.startswith('ftp://'):
        ftp_url = 'ftp://' + ftp_url

    try:
        parse = urlparse(ftp_url)
        ftp_server = parse.netloc
        ftp_path = parse.path
        filename = os.path.basename(ftp_path)

        print(f"Downloading {filename} to {destination}\n")
        with FTP(ftp_server) as ftp:
            ftp.login()
            ftp.cwd(os.path.dirname(ftp_path))
            with open(destination, 'wb') as f:
                ftp.retrbinary('RETR ' + filename, f.write)
    except Exception as e:
        print(f"Error downloading from {ftp_url}: {e}")

# メインプログラム
response_data = get_api_response(PROJECT_ACCESSION)
sample_to_ftp_urls = parse_response_data(response_data)

for sample_acc, ftp_urls in sample_to_ftp_urls.items():
    sample_dir = os.path.join(BASE_DIR, sample_acc)
    os.makedirs(sample_dir, exist_ok=True)
    gz_files = []

    for ftp_url in ftp_urls:
        if ftp_url.endswith('.gz'):  # .gzファイルのみをフィルタリング
            filename = os.path.basename(urlparse(ftp_url).path)
            destination = os.path.join(sample_dir, filename)
            download_from_ftp(ftp_url, destination)
            gz_files.append(destination)

    if gz_files:
        # Nextflowを使用してマージ
        merged_fastq = os.path.join(sample_dir, "merged.fastq.gz")
        print(f"Merging files in {sample_dir} to create {merged_fastq}\n")
        try:
            subprocess.run([NEXTFLOW_DIR, "run", NEXTFLOW_MERGE_SCRIPT, "--input_dir", sample_dir, "--output_file", merged_fastq], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error merging files: {e}")
            continue  # 次のサンプルに進む

        # Nextflowを使用して解析
        sample_output_dir = os.path.join(OUTPUT_DIR, sample_acc)
        os.makedirs(sample_output_dir, exist_ok=True)
        print(f"Analyzing merged file {merged_fastq}\n")
        try:
            subprocess.run([NEXTFLOW_DIR, "run", NEXTFLOW_RUN_SCRIPT, "--input", merged_fastq, "--output_dir", sample_output_dir], check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error analyzing files: {e}")
            continue  # 次のサンプルに進む