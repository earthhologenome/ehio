################################################################################
## Download host genome
rule fetch_host_genome:
    input:
        os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        )
    output:
        ref=os.path.join(
            config["workdir"],
            config["hostgenome"],
            config["hostgenome"] + ".fna.gz"
        )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/1_Preprocess_QC.yaml"
    threads:
        1
    resources:
        mem_gb=8,
        time='02:00:00'
    log:
        os.path.join(config["logdir"] + "/fetch_host_genome.log")
    message:
        "Fetching host genome"
    shell:
        """

        echo "Downloading reference genome"
        mkdir -p {config[workdir]}/{config[hostgenome]}/
        wget {config[host_genome_url]} -q -O {output.ref}

        """