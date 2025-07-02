nextflow.enable.dsl=2

params.input_dir = ''
params.output_file = ''

process mergeFastq {
    outputDir = new File(params.output_file).parent
    publishDir outputDir, mode: 'copy'

    input:
    path fastq_files

    output:
    path "merged.fastq.gz"

    script:
    """
    cat ${fastq_files.join(' ')} > merged.fastq.gz
    """
}

workflow {
    // fastq_files チャンネルを作成し、merged.fastq.gzを除外する
    fastq_files = Channel
                    .fromPath("${params.input_dir}/*.fastq.gz")
                    .filter { it.name != 'merged.fastq.gz' }
                    .collect()

    mergeFastq(fastq_files)
}
