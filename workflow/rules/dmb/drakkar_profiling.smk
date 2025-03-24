################################################################################
### Run drakkar cataloging
rule drakkar_profiling:
    input:
        ready=os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        ),
        reads=os.path.join(
            config["workdir"], 
            "reads/", 
            "reads_downloaded"
        ),
        mags=os.path.join(
            config["workdir"], 
            "mags/", 
            "mags_downloaded"
        )
    output:
        os.path.join(
            config["workdir"],
            "profiling.tsv"
        )
    threads:
        2
    resources:
        mem_gb=16,
        time='24:00:00'
    message:
        "Running drakkar cataloging"
    shell:
        """

        #module load drakkar/1.0.0

        drakkar unlock \
            -i {config[workdir]}

        drakkar profiling \
            --bins_dir config[workdir]/mags/ \
            --reads_dir config[workdir]/reads/ \
            --type genome

        """