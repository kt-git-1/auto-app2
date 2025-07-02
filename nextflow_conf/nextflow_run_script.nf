nextflow.enable.dsl=2

params.input = ''
params.output_dir = ''

process analyzeFastq {   
    input:
    path merged_file

    output:
    path "fastqc_output"

    publishDir path: "${params.output_dir}", mode: 'copy'

    script:
    """
    mkdir -p fastqc_output
    fastqc ${merged_file} -o fastqc_output
    """
}

workflow {
    analyzeFastq(Channel.fromPath(params.input))
}
