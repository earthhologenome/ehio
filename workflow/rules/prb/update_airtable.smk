################################################################################
### Create summary table from outputs
rule update_airtable:
    input:
       drakkar_out=os.path.join(
            config["workdir"],
            "preprocessing.tsv"
        )
    output:
       os.path.join(
            config["workdir"],
            "airtable_patched"
        )
    params:
        tmpdir=os.path.join(
            config["workdir"],
            "tmp/"
        ),
        npar=expand(
            os.path.join(
                config["workdir"],
                "misc/{sample}.npo"
            ),
            sample=SAMPLE
        ),
        misc_dir=os.path.join(
            config["workdir"],
            "misc/"
        )
    conda:
        f"{config['ehi_code_dir']}/conda_envs/lftp.yaml"
    threads:
        1
    resources:
        mem_gb=24,
        time='00:20:00'
    message:
        "Creating a final preprocessing report"
    shell:
        """
        rm -rf {params.tmpdir}

        #Create nonpareil sample metadata file
        mkdir -p {params.tmpdir}
        for i in {params.npar}; do echo $(basename $i) >> {params.tmpdir}/files.txt; done
        for i in {params.npar}; do echo $(basename ${{i/.npo/}}) >> {params.tmpdir}/names.txt; done
        for i in {params.npar}; do echo "#f03b20" >> {params.tmpdir}/colours.txt; done
        echo -e "File\tName\tColour" > {params.tmpdir}/headers.txt
        paste {params.tmpdir}/files.txt {params.tmpdir}/names.txt {params.tmpdir}/colours.txt > {params.tmpdir}/merged.tsv
        cat {params.tmpdir}/headers.txt {params.tmpdir}/merged.tsv > {output.npar_metadata}

        #Create preprocessing report
        mkdir -p {params.tmpdir}
        for i in {input.coverm}; do echo $(basename ${{i/_coverM_mapped_host.tsv}}) >> {params.tmpdir}/names.tsv; done
        for i in {input.coverm}; do grep -v 'Genome' $i | grep -v 'unmapped' | cut -f3; done >> {params.tmpdir}/host_reads.tsv

        for i in {input.fastp}; do grep '"total_reads"' $i | sed -n 1p | cut -f2 --delimiter=: | tr -d ','; done >> {params.tmpdir}/read_pre_filt.tsv
        for i in {input.fastp}; do grep '"total_reads"' $i | sed -n 2p | cut -f2 --delimiter=: | tr -d ','; done >> {params.tmpdir}/read_post_filt.tsv
        for i in {input.fastp}; do grep '"total_bases"' $i | sed -n 1p | cut -f2 --delimiter=: | tr -d ','; done >> {params.tmpdir}/bases_pre_filt.tsv
        for i in {input.fastp}; do grep '"total_bases"' $i | sed -n 2p | cut -f2 --delimiter=: | tr -d ','; done >> {params.tmpdir}/bases_post_filt.tsv
        for i in {input.fastp}; do grep 'adapter_trimmed_reads' $i | cut -f2 --delimiter=: | tr -d ',' | tr -d ' '; done >> {params.tmpdir}/adapter_trimmed_reads.tsv
        for i in {input.fastp}; do grep 'adapter_trimmed_bases' $i | cut -f2 --delimiter=: | tr -d ',' | tr -d ' '; done >> {params.tmpdir}/adapter_trimmed_bases.tsv

        #parse singlem estimates
        for i in {input.read_fraction}; do sed '1d;' $i | cut -f2,3,4,5 >> {params.tmpdir}/singlem.tsv; done

        #parse nonpareil estimates
        for i in {input.npstats}; do sed '1d;' $i | cut -f2,3,4,5,6,7 >> {params.tmpdir}/npstats.tsv; done

        #parse picard dup stats
        for i in {input.dupstats}; do grep 'Unknown' $i | cut -f9 >> {params.tmpdir}/dupstats.tsv; done

        paste {params.tmpdir}/names.tsv {params.tmpdir}/read_pre_filt.tsv {params.tmpdir}/read_post_filt.tsv {params.tmpdir}/bases_pre_filt.tsv {params.tmpdir}/bases_post_filt.tsv {params.tmpdir}/adapter_trimmed_reads.tsv {params.tmpdir}/adapter_trimmed_bases.tsv {params.tmpdir}/host_reads.tsv {params.tmpdir}/singlem.tsv {params.tmpdir}/npstats.tsv {params.tmpdir}/dupstats.tsv > {params.tmpdir}/preprocessing_stats.tsv
        echo -e "EHI_number\treads_pre_fastp\treads_post_fastp\tbases_pre_fastp\tbases_post_fastp\tadapter_trimmed_reads\tadapter_trimmed_bases\thost_reads\tbacterial_archaeal_bases\tmetagenomic_bases\tsinglem_fraction\taverage_genome_size\tkappa\tC\tLR\tmodelR\tLRstar\tdiversity\thost_duplicate_fraction" > {params.tmpdir}/headers.tsv
        cat {params.tmpdir}/headers.tsv {params.tmpdir}/preprocessing_stats.tsv > {output.report}

        cp {output.report} {params.misc_dir}
        cp {output.npar_metadata} {params.misc_dir}
        tar -czf {config[workdir]}/{config[batch]}_stats.tar.gz {params.misc_dir}

        #Upload stats and report to ERDA for storage
        lftp sftp://erda -e "put {config[workdir]}/{config[batch]}_stats.tar.gz -o /EarthHologenomeInitiative/Data/PPR/{config[batch]}/; bye"
        sleep 10
        lftp sftp://erda -e "put {output.report} -o /EarthHologenomeInitiative/Data/REP/; bye"

        #Automatically update the AirTable with the preprocessing stats
        python {config[ehi_code_dir]}/airtable/add_prb_stats_airtable.py --report={output.report} --prb={config[batch]} 

        #Indicate that the PRB is done in AirTable
        python {config[ehi_code_dir]}/airtable/log_prb_done_airtable.py --code={config[batch]}
       
        #Clean up the files/directories
        rm {config[workdir]}/{config[batch]}_stats.tar.gz
        rm -r {config[workdir]}/{config[hostgenome]}/
        rm -r {params.misc_dir}
        rm -r {params.tmpdir}

        """