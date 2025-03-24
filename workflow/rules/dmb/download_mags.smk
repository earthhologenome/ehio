################################################################################
### Fetch MAGs from ERDA
rule download_mags:
    input:
        os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        )
    output:
        os.path.join(
            config["workdir"], 
            "mags/", 
            "mags_downloaded"
        )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads: 1
    resources:
        mem_gb=8,
        time='08:00:00'
    benchmark:
        os.path.join(config["logdir"] + "/download_mags_benchmark.tsv")
    message:
        "Fetching MAGs from ERDA"
    shell:
        """
        
        # download MAGs
        ##fix formatting 
        dos2unix /projects/ehi/data/RUN/config[batch]/mags.csv

        while read i; do echo $i | wget `cut -f4 -d ','`; done < mags.csv

        touch {output}

        """