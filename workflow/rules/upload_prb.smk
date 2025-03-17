################################################################################
### Calculate % of each sample's reads mapping to host genome/s (also upload PPR reads to ERDA)
rule upload_to_ERDA:
    input:
        drakkar_out=os.path.join(
            config["workdir"],
            "preprocessing.tsv"
        )
    output:
        os.path.join(
            config["workdir"],
            "files_uploaded"
        )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads:
        2
    resources:
        load=1,
        mem_gb=16,
        time='24:00:00'
    log:
        os.path.join(config["logdir"] + "/erda_upload.log")
    message:
        "Uploading files to ERDA"
    shell:
        """
        #Upload preprocessed reads to ERDA for storage
        lftp sftp://erda -e "put preprocessing/final/*.fq.gz -o /EarthHologenomeInitiative/Data/PPR/{config[batch]}/; bye"
        sleep 5
        lftp sftp://erda -e "put preprocessing/final/*.bam -o /EarthHologenomeInitiative/Data/PPR/{config[batch]}/; bye"
        sleep 5
        lftp sftp://erda -e "put {input.host_bam} -o /EarthHologenomeInitiative/Data/PPR/{config[batch]}/; bye"
        
        touch {output}
        """