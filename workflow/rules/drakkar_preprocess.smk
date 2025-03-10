################################################################################
### Run drakkar preprocess
rule drakkar_preprocess:
    input:
        ready=os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        ),
        bt2_index=os.path.join(
            config["workdir"],
            config["hostgenome"],
            config["hostgenome"] + "_RN.fna.gz.rev.2.bt2l",
        ),
        rn_catted_ref=os.path.join(
            config["workdir"],
            config["hostgenome"],
            config["hostgenome"] + "_RN.fna.gz"
        ),
        reads=
    output:
        drakkar_out=os.path.join(
            config["workdir"],
            "{sample}_raw_1.fq.gz"
        )
    conda:
        f"{config['codedir']}/conda_envs/lftp.yaml"
    threads:
        1
    resources:
        mem_gb=8,
        time='24:00:00'
    message:
        "Running drakkar {process}"
    shell:
        """

        module load drakkar/1.0.0

        drakkar preprocess \
            -f {} \
            -g {input.rn_catted_ref} \
            -o {}

        """