################################################################################
### Create PRB folder on ERDA
rule create_PRB_folder:
    output:
        os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
            )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads:
        1
    resources:
        mem_gb=8,
        time='00:03:00'
    message:
        "Creating PRB folder on ERDA"
    shell:
        """

        echo "mkdir EarthHologenomeInitiative/Data/PPR/config[batch]" | sftp erda
        touch {output}

        #Also, log the AirTable that the PRB is running!
        python config[ehi_code_dir]/airtable/log_prb_start_airtable.py --code=config[batch]

        """