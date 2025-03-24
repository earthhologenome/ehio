################################################################################
### Fetch preprocessed reads from ERDA
rule download_preprocessed:
    input:
        os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        )
    output:
        r1=os.path.join(
            config["workdir"], 
            "reads/", 
            "{EHI}_M_1.fq.gz"
        ),
        r2=os.path.join(
            config["workdir"], 
            "reads/", 
            "{EHI}_M_2.fq.gz"
        )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads: 1
    resources:
        load=8,
        mem_gb=8,
        time=estimate_time_download
    benchmark:
        os.path.join(config["logdir"] + "/download_preprocessed_benchmark_{EHI}.tsv")
    message:
        "Fetching metagenomics reads for {wildcards.EHI} from ERDA"
    shell:
        """
        # read1
        wget --no-verbose `grep '{wildcards.EHI}' /projects/ehi/data/RUN/{config['batch']}/asb_input.tsv | cut -f3`
        mv {wildcards.EHI}*_1.fq.gz {output.r1}

        # read2
        wget --no-verbose `grep '{wildcards.EHI}' /projects/ehi/data/RUN/{config['batch']}/asb_input.tsv | cut -f4`
        mv {wildcards.EHI}*_2.fq.gz {output.r2}
        """