################################################################################
### Fetch preprocessed reads from ERDA
rule download_preprocessed:
    input:
        os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        )
    output:
        os.path.join(
            config["workdir"], 
            "reads/", 
            "reads_downloaded"
        ),
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads: 1
    resources:
        load=8,
        mem_gb=8,
        time='08:00:00'
    benchmark:
        os.path.join(config["logdir"] + "/download_preprocessed_benchmark.tsv")
    message:
        "Fetching metagenomics reads for {wildcards.EHI} from ERDA"
    shell:
        """

        # read1
        while read i; do echo wget `cut -f2`; done < reads.tsv

        # read1
        while read i; do echo wget `cut -f3`; done < reads.tsv

        touch {output}

        """