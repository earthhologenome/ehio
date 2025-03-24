################################################################################
### Run drakkar cataloging
rule drakkar_cataloging:
    input:
        ready=os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        ),
        r1=expand(
            os.path.join(
                config["workdir"], 
                "reads/", 
                "{EHI}_M_1.fq.gz"
            ),
            sample=SAMPLE
        ),
        r2=expand(
            os.path.join(
                config["workdir"], 
                "reads/", 
                "{EHI}_M_2.fq.gz"
            ),
            sample=SAMPLE
        ),
    output:
        drakkar_out=os.path.join(
            config["workdir"],
            "cataloging.tsv"
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

        drakkar cataloging \
            ---input {config[workdir]} \
            --mode individual

        """