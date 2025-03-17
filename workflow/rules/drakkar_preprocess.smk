################################################################################
### Run drakkar preprocess
rule drakkar_preprocess:
    input:
        ready=os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        ),
        ref=os.path.join(
            config["workdir"],
            config["hostgenome"],
            config["hostgenome"] + ".fna.gz"
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
            config["workdir"],
            "preprocessing.tsv"
        )
    threads:
        2
    resources:
        mem_gb=16,
        time='24:00:00'
    message:
        "Running drakkar preprocess"
    shell:
        """

        #module load drakkar/1.0.0

        drakkar unlock \
            -i {config[workdir]}

        drakkar preprocess \
            -i {config[workdir]} \
            -r {input.ref} 

        """