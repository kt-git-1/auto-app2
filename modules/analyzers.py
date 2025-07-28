import subprocess
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

class MapDamageAnalyzer:
    def __init__(self, config):
        self.config = config
    
    def run_mapdamage(self, sample_acc, softclipped_bam):
        """Run mapDamage analysis"""
        logger.info(f"Running mapDamage for {sample_acc}")
        
        sample_outdir = self.config.results_dir / sample_acc / "mapdamage"
        sample_outdir.mkdir(parents=True, exist_ok=True)
        
        # Filter BAM (mapQ>=30, POS>=300)
        filtered_bam = self.config.temp_dir / f"{sample_acc}_filtered.bam"
        sorted_filtered_bam = self.config.temp_dir / f"{sample_acc}_filtered.sorted.bam"
        
        # Filter and sort
        try:
            subprocess.run([
                "samtools", "view", "-b", "-q", "30", "-e", "POS>=300",
                "-o", str(filtered_bam), str(softclipped_bam)
            ], check=True)
            subprocess.run([
                "samtools", "sort", "-o", str(sorted_filtered_bam), str(filtered_bam)
            ], check=True)
            filtered_bam.unlink(missing_ok=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Filtering failed for {sample_acc}: {e}")
            return None
        
        # Run mapDamage
        mapdamage_cmd = [
            "mapDamage", "-i", str(sorted_filtered_bam), 
            "-r", str(self.config.reference_genome), 
            "-d", str(sample_outdir), "--merge-libraries"
        ]
        
        try:
            subprocess.run(mapdamage_cmd, check=True)
            logger.info(f"mapDamage completed for {sample_acc}")
            return sample_outdir
        except subprocess.CalledProcessError as e:
            logger.error(f"mapDamage failed for {sample_acc}: {e}")
            return None

class QualimapAnalyzer:
    def __init__(self, config):
        self.config = config
    
    def run_qualimap(self, sample_acc, dedup_bam):
        """Run Qualimap quality check"""
        logger.info(f"Running Qualimap for {sample_acc}")
        
        sample_outdir = self.config.results_dir / sample_acc / "qualimap"
        sample_outdir.mkdir(parents=True, exist_ok=True)
        
        qualimap_cmd = [
            "qualimap", "bamqc", "-bam", str(dedup_bam),
            "-outdir", str(sample_outdir), "-outformat", "HTML",
            "--java-mem-size=8G"
        ]
        
        try:
            subprocess.run(qualimap_cmd, check=True)
            logger.info(f"Qualimap completed for {sample_acc}")
            return sample_outdir
        except subprocess.CalledProcessError as e:
            logger.error(f"Qualimap failed for {sample_acc}: {e}")
            return None

class HaplotypeCaller:
    def __init__(self, config):
        self.config = config
    
    def run_haplotypecaller(self, sample_acc, dedup_bam):
        """Run GATK HaplotypeCaller"""
        logger.info(f"Running HaplotypeCaller for {sample_acc}")
        
        vcf_dir = self.config.results_dir / sample_acc / "vcf_files"
        vcf_dir.mkdir(parents=True, exist_ok=True)
        vcf_file = vcf_dir / f"{sample_acc}.vcf"
        
        haplotypecaller_cmd = [
            "gatk", "HaplotypeCaller",
            "-R", str(self.config.reference_genome),
            "-I", str(dedup_bam),
            "-O", str(vcf_file),
            "--output-mode", "EMIT_VARIANTS_ONLY",
            "-stand-call-conf", "30",
            "--native-pair-hmm-threads", str(self.config.args.threads)
        ]
        
        try:
            subprocess.run(haplotypecaller_cmd, check=True)
            logger.info(f"HaplotypeCaller completed for {sample_acc}")
            return vcf_file
        except subprocess.CalledProcessError as e:
            logger.error(f"HaplotypeCaller failed for {sample_acc}: {e}")
            return None
