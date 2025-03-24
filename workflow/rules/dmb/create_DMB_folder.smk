################################################################################
### Create DMB folder on ERDA
rule create_DMB_folder:
    output:
        os.path.join(
            config["workdir"], 
            "ERDA_folder_created"
        )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads: 1
    resources:
        load=8,
        mem_gb=8,
        time="00:05:00",
    message:
        "Creating dereplication mapping batch folder on ERDA"
    shell:
        """
        echo "mkdir EarthHologenomeInitiative/Data/DMB/{config[batch]}" | sftp erda

        #Also, log the AirTable that the DMB is running!
        python {config[ehi_code_dir]}/airtable/log_dmb_start_airtable.py --code={config[batch]}

        touch {output}
        """