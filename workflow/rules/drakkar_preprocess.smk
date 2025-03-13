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
        r1=expand(
            os.path.join(
                config["workdir"],
                "{sample}_raw_1.fq.gz"
            ),
            sample=SAMPLE
        ),
        r2=expand(
            os.path.join(
                config["workdir"],
                "{sample}_raw_2.fq.gz"
            ),
            sample=SAMPLE
        ),
    output:
        drakkar_out=os.path.join(
            "/projects/ehi/data/REP/",
            config["batch"] + ".tsv"
        )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads:
        1
    resources:
        mem_gb=8,
        time='24:00:00'
    message:
        "Running drakkar preprocess"
    shell:
        """

        #module load drakkar/1.0.0

        drakkar preprocess \
            -i {} \
            -g {input.rn_catted_ref} \
            -o {}

        """