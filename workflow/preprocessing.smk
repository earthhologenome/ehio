################################################################################
# EHI snakefile for preprocess/QC                                              #
# Raphael Eisenhofer 03/25                                                     #
#         .----------------.  .----------------.  .----------------.           #
#        | .--------------. || .--------------. || .--------------. |          #
#        | |  _________   | || |  ____  ____  | || |     _____    | |          #
#        | | |_   ___  |  | || | |_   ||   _| | || |    |_   _|   | |          #
#        | |   | |_  \_|  | || |   | |__| |   | || |      | |     | |          #
#        | |   |  _|  _   | || |   |  __  |   | || |      | |     | |          #
#        | |  _| |___/ |  | || |  _| |  | |_  | || |     _| |_    | |          # 
#        | | |_________|  | || | |____||____| | || |    |_____|   | |          # 
#        | |              | || |              | || |              | |          # 
#        | '--------------' || '--------------' || '--------------' |          # 
#         '----------------'  '----------------'  '----------------'           # 
################################################################################


################################################################################
### Setup input (from get_preprocessing_input.py)
with open(f"/projects/ehi/data/RUN/{config['batch']}/prb_input.tsv", "r") as f:
    SAMPLE = [line.strip() for line in f]

print("Detected these samples")
print(SAMPLE)


################################################################################
### Define time rule for downloading samples
def estimate_time_download(wildcards, attempt):
    fs_sample = f"{config['workdir']}/{wildcards.sample}_filesize.txt"
    with open(fs_sample, 'r') as f:
        input_size = int(f.read().strip())
    # convert from bytes to gigabytes
    input_size_gb = input_size / (1024 * 1024 * 1024)
    # Multiply by 2, and set time based on 30 MB/s download speed.
    estimate_time_download = ((input_size_gb * 3 ) + 12 ) / 1.25
    return attempt * int(estimate_time_download)

################################################################################
### Setup the desired outputs
#sets drakkar to be run locally
localrules: drakkar_preprocess


rule all:
    input:
        expand("/projects/ehi/data/REP/{batch}.tsv",
                batch=config["batch"]
        )


include: os.path.join(config["codedir"], "rules/prb/create_PRB_folder.smk")
include: os.path.join(config["codedir"], "rules/prb/get_filesize_erda.smk")
include: os.path.join(config["codedir"], "rules/prb/download_raw.smk")
include: os.path.join(config["codedir"], "rules/prb/get_host_genome.smk")
include: os.path.join(config["codedir"], "rules/prb/drakkar_preprocess.smk")
include: os.path.join(config["codedir"], "rules/prb/upload_prb.smk")
include: os.path.join(config["codedir"], "rules/prb/update_airtable.smk")
