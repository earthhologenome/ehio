################################################################################
# EHI snakefile for assembly/binning                                              #
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
### Setup input (from get_assembly_input.py)
df = pd.read_csv(f"/projects/ehi/data/RUN/{config['batch']}asb_input.tsv", sep="\t")

# Use set to create a list of valid combinations of wildcards. Note that 'ID' = EHA number.
valid_combinations = set(
    (row["PR_batch"], row["EHI_number"], row["Assembly_code"], row["metagenomic_bases"], row["singlem_fraction"], row["diversity"], row["C"]) for _, row in df.iterrows()
)

SAMPLE = df["EHI_number"]

print("Detected these samples")
print(SAMPLE)


################################################################################
### Define time rule for downloading samples
def get_row(wildcards):
    return df[
        (df["PR_batch"] == wildcards.PRB) &
        (df["EHI_number"] == wildcards.EHI)
    ].iloc[0]

def calculate_input_size_gb(metagenomic_bases):
    # convert from bytes to gigabytes
    return metagenomic_bases / (1024 * 1024 * 1024)

## Rule-specific time estimations
## This also includes retries by attempt
def estimate_time_download(wildcards, attempt):
    row = get_row(wildcards)
    input_size_gb = calculate_input_size_gb(row["metagenomic_bases"])
    estimate_time_download = (input_size_gb / 1.4)
    estimate_time_download = max(estimate_time_download, 2)
    return attempt * int(estimate_time_download)

################################################################################
### Setup the desired outputs
#sets drakkar to be run locally
localrules: drakkar_cataloging


rule all:
    input:
        expand(
            os.path.join(
            config["workdir"], 
            "reads/", 
            "{combo[0]}/", 
            "{combo[1]}_M_1.fq.gz"
        ),
            combo=valid_combinations,
        ),
        expand("/projects/ehi/data/REP/{batch}.tsv",
                batch=config["batch"]
        )


include: os.path.join(config["codedir"], "rules/asb/create_ASB_folder.smk")
include: os.path.join(config["codedir"], "rules/asb/download_preprocessed.smk")
include: os.path.join(config["codedir"], "rules/asb/drakkar_cataloging.smk")
include: os.path.join(config["codedir"], "rules/asb/assembly_summary.smk")
include: os.path.join(config["codedir"], "rules/asb/log_ASB_finish.smk")
