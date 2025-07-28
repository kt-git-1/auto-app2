import argparse
from pathlib import Path
import logging

class PipelineConfig:
    def __init__(self, args):
        self.args = args
        self.script_dir = Path(__file__).parent.resolve()
        self.base_dir = args.base_dir
        self.project_accession = args.project_accession
        
        # ディレクトリ構造
        self.raw_data_dir = self.base_dir / "raw_data" / self.project_accession
        self.results_dir = self.base_dir / "results" / self.project_accession
        self.logs_dir = self.base_dir / "logs"
        self.temp_dir = self.base_dir / "temp"
        
        # 参照ゲノム
        self.reference_genome = args.reference_genome or (self.base_dir / "reference" / "equCab3.nochrUn.fa")

        # 作成
        for dir_path in [self.raw_data_dir, self.results_dir, self.logs_dir, self.temp_dir]:
            dir_path.mkdir(parents=True, exist_ok=True)

def parse_args():
    script_dir = Path(__file__).parent.resolve()
    parser = argparse.ArgumentParser(description="Complete ENA download and analysis pipeline")
    parser.add_argument("--project_accession", default="PRJEB19970", help="ENA project accession")
    parser.add_argument("--base_dir", type=Path, default=script_dir / "data", help="Base directory for all data")
    parser.add_argument("--reference_genome", type=Path, help="Reference genome FASTA file")
    parser.add_argument("--workers", type=int, default=4, help="Number of parallel download workers")
    parser.add_argument("--threads", type=int, default=20, help="Number of threads for analysis")
    parser.add_argument("--java_mem", default="10g", help="Java memory for tools like GATK")
    return parser.parse_args()

def setup_logging(log_file: Path | None = None):
    """
    ログ出力を初期化します。
    コンソールと、log_file が指定されていればそのファイルにもログを出力します。
    """
    handlers = []
    # コンソールハンドラ
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    handlers.append(console_handler)
    # ファイルハンドラ（log_file が指定されている場合）
    if log_file:
        # どのログファイルに出力しているかを明示
        file_handler = logging.FileHandler(str(log_file))
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        handlers.append(file_handler)
    logging.basicConfig(level=logging.INFO, handlers=handlers)
    logger = logging.getLogger(__name__)
    if log_file:
        logger.info(f"ログをファイルに出力しています: {log_file}")
    return logger
