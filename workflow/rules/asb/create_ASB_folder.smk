################################################################################
### Create ASB folder on ERDA
rule create_ASB_folder:
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
        "Creating assembly batch folder on ERDA"
    shell:
        """
        echo "mkdir EarthHologenomeInitiative/Data/ASB/{config[batch]}" | sftp erda

        #Also, log the AirTable that the ASB is running!
        python {config[ehi_code_dir]}/airtable/log_asb_start_airtable.py --code={config[batch]}

        touch {output}
        """