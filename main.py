import argparse
import logging
from pathlib import Path
import requests
from ftplib import FTP
from urllib.parse import urlparse
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import sys

# コマンドライン引数を解析し、パイプラインの設定を取得する
def parse_args():
    script_dir = Path(__file__).parent.resolve()
    parser = argparse.ArgumentParser(description="Auto-app2 ENA download and analysis pipeline")
    parser.add_argument("--project_accession", default="PRJEB19970", help="ENA project accession")
    parser.add_argument("--base_dir", type=Path, default=script_dir / "data" / "row_data", help="Base directory for raw data")
    parser.add_argument("--nextflow", type=Path, default=script_dir / "nextflow", help="Nextflow executable (path or just 'nextflow' if in PATH)")
    parser.add_argument("--merge_script", type=Path, default=script_dir / "nextflow_conf" / "nextflow_merge_script.nf", help="Nextflow merge script")
    parser.add_argument("--run_script", type=Path, default=script_dir / "nextflow_conf" / "nextflow_run_script.nf", help="Nextflow run script")
    parser.add_argument("--output_dir", type=Path, default=script_dir / "data" / "output_data", help="Output data directory")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel download workers")
    return parser.parse_args()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ENA APIを呼び出して、指定プロジェクトのファイルレポートを取得する
def get_api_response(PROJECT_ACCESSION, session):
    url = f"https://www.ebi.ac.uk/ena/portal/api/filereport?accession={PROJECT_ACCESSION}&result=read_run&fields=sample_accession,submitted_ftp&format=tsv"
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data for project {PROJECT_ACCESSION}: {e}")
        sys.exit(1)

# API応答TSVを解析し、サンプルアクセッションごとにFTP URLをグループ化する
def parse_response_data(response_data):
    sample_to_ftp_urls = {}
    lines = response_data.strip().split('\n')[1:]
    for line in lines:
        parts = line.split('\t')
        if len(parts) < 2:
            continue
        sample_acc, ftp_urls = parts[0], parts[1]
        # 複数ファイルがカンマ区切りで入っている場合に対応
        for ftp_url in ftp_urls.split(';'):
            ftp_url = ftp_url.strip()
            if not ftp_url:
                continue
            sample_to_ftp_urls.setdefault(sample_acc, []).append(ftp_url)
    return sample_to_ftp_urls

# FTPサーバーから指定したURLのファイルをダウンロードし、保存先パスを返す
def download_from_ftp(ftp_url, destination):
    if not ftp_url.startswith('ftp://'):
        ftp_url = 'ftp://' + ftp_url
    try:
        parse = urlparse(ftp_url)
        ftp_server = parse.netloc
        ftp_path = parse.path
        filename = os.path.basename(ftp_path)
        logger.info(f"Downloading {filename} to {destination}")
        with FTP(ftp_server) as ftp:
            ftp.login()
            ftp.cwd(os.path.dirname(ftp_path))
            with open(destination, 'wb') as f:
                ftp.retrbinary('RETR ' + filename, f.write)
        logger.info(f"Downloaded {filename} to {destination}")
        return destination
    except Exception as e:
        logger.error(f"Error downloading from {ftp_url}: {e}")
        raise

# メイン処理: ディレクトリ準備からダウンロード・マージ・解析まで一連のパイプラインを実行する
def main(args):
    # prepare directories as Path objects
    project_base_dir = args.base_dir / args.project_accession
    project_output_dir = args.output_dir / args.project_accession
    project_base_dir.mkdir(parents=True, exist_ok=True)
    project_output_dir.mkdir(parents=True, exist_ok=True)

    session = requests.Session()

    # ENA API and parsing
    response_data = get_api_response(args.project_accession, session)
    sample_to_ftp_urls = parse_response_data(response_data)

    for sample_acc, ftp_urls in sample_to_ftp_urls.items():
        sample_dir = project_base_dir / sample_acc
        sample_dir.mkdir(parents=True, exist_ok=True)
        gz_files = []

        # parallel download
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            future_to_url = {
                executor.submit(
                    download_from_ftp,
                    url,
                    sample_dir / os.path.basename(urlparse(url if url.startswith('ftp://') else 'ftp://' + url).path)
                ): url
                for url in ftp_urls if url.endswith('.gz')
            }
            for future in as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    dest = future.result()
                    gz_files.append(dest)
                except Exception as e:
                    logger.error(f"Failed download {url}: {e}")

        if not gz_files:
            logger.warning(f"No .gz files downloaded for sample {sample_acc}")
            continue

        merged_fastq = sample_dir / "merged.fastq.gz"
        logger.info(f"Merging files in {sample_dir} to create {merged_fastq}")
        try:
            result = subprocess.run(
                [str(args.nextflow), "run", str(args.merge_script),
                 "--input_dir", str(sample_dir), "--output_file", str(merged_fastq)],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"Merge stdout for {sample_acc}: {result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error merging files for {sample_acc}: {e.stderr.strip()}")
            continue

        sample_output_dir = project_output_dir / sample_acc
        sample_output_dir.mkdir(parents=True, exist_ok=True)
        logger.info(f"Analyzing merged file {merged_fastq}")
        try:
            result = subprocess.run(
                [str(args.nextflow), "run", str(args.run_script),
                 "--input", str(merged_fastq), "--output_dir", str(sample_output_dir)],
                check=True,
                capture_output=True,
                text=True
            )
            logger.info(f"Analysis stdout for {sample_acc}: {result.stdout}")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error analyzing files for {sample_acc}: {e.stderr.strip()}")
            continue

if __name__ == "__main__":
    args = parse_args()
    main(args)