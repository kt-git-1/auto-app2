import subprocess
import logging
import shutil
from pathlib import Path

logger = logging.getLogger(__name__)


class BAMProcessor:
    def __init__(self, config):
        self.config = config
    
    def run_bam_processing(self, sample_acc, softclipped_bam):
        """Sort, deduplicate, and index BAM file"""
        logger.info(f"Running BAM processing for {sample_acc}")
        
        base_name = sample_acc
        
        # Step 1: Sort
        sorted_bam = self.config.bam_dir / f"{base_name}.sorted.bam"
        try:
            subprocess.run([
                "samtools", "sort", "-@", str(self.config.args.threads), 
                "-o", str(sorted_bam), str(softclipped_bam)
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Picard command failed: {e}")
            logger.error(f"Command output: {e.output if hasattr(e, 'output') else 'No output'}")
            raise
        
        # Step 2: CleanSam
        clean_bam = self.config.bam_dir / f"{base_name}.clean.bam"
        try:
            subprocess.run([
                "java", "-Xmx" + self.config.args.java_mem, "-jar", "/usr/local/bin/picard.jar",
                "CleanSam", "I=" + str(sorted_bam), "O=" + str(clean_bam),
                "VALIDATION_STRINGENCY=LENIENT"
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Picard command failed: {e}")
            logger.error(f"Command output: {e.output if hasattr(e, 'output') else 'No output'}")
            raise
        
        # Step 3: AddOrReplaceReadGroups
        grouped_bam = self.config.bam_dir / f"{base_name}.grouped.bam"
        try:
            subprocess.run([
                "java", "-Xmx" + self.config.args.java_mem, "-jar", "/usr/local/bin/picard.jar",
                "AddOrReplaceReadGroups", "I=" + str(clean_bam), "O=" + str(grouped_bam),
                "RGLB=Bayanbulag", "RGSM=" + base_name, "RGPU=tile", "RGPL=ILLUMINA",
                "RGID=" + base_name, "RGDS=" + base_name, "RGCN=KNZWUNIV",
                "VALIDATION_STRINGENCY=LENIENT"
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Picard command failed: {e}")
            logger.error(f"Command output: {e.output if hasattr(e, 'output') else 'No output'}")
            raise
        
        # Step 4: MarkDuplicates
        marked_bam = self.config.bam_dir / f"{base_name}.marked.bam"
        metrics_file = self.config.bam_dir / f"{base_name}.marked_dup_metrics.txt"
        try:
            subprocess.run([
                "java", "-Xmx" + self.config.args.java_mem, "-jar", "/usr/local/bin/picard.jar",
                "MarkDuplicates", "I=" + str(grouped_bam), "O=" + str(marked_bam),
                "M=" + str(metrics_file), "VALIDATION_STRINGENCY=LENIENT"
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Picard command failed: {e}")
            logger.error(f"Command output: {e.output if hasattr(e, 'output') else 'No output'}")
            raise
        
        # Step 5: DeDup using Picard MarkDuplicates with REMOVE_DUPLICATES=true
        dedup_bam = self.config.dedup_dir / f"{base_name}.marked.dedup.bam"
        try:
            subprocess.run([
                "java", "-Xmx" + self.config.args.java_mem, "-jar", "/usr/local/bin/picard.jar",
                "MarkDuplicates", "I=" + str(marked_bam), "O=" + str(dedup_bam),
                "M=" + str(self.config.bam_dir / f"{base_name}.dedup_metrics.txt"),
                "REMOVE_DUPLICATES=true", "VALIDATION_STRINGENCY=LENIENT"
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Picard REMOVE_DUPLICATES failed: {e}")
            raise
        
        # Step 6: Sort deduplicated BAM
        final_dedup_bam = self.config.dedup_dir / f"{base_name}.marked.dedup.sorted.bam"
        try:
            subprocess.run([
                "samtools", "sort", "-o", str(final_dedup_bam), str(dedup_bam)
            ], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Sort command failed: {e}")
            logger.error(f"Command output: {e.output if hasattr(e, 'output') else 'No output'}")
            raise
        
        # Step 7: Index
        try:
            subprocess.run(["samtools", "index", str(final_dedup_bam)], check=True)
        except subprocess.CalledProcessError as e:
            logger.error(f"Index command failed: {e}")
            logger.error(f"Command output: {e.output if hasattr(e, 'output') else 'No output'}")
            raise
        
        logger.info(f"BAM processing completed for {sample_acc}")
        return final_dedup_bam