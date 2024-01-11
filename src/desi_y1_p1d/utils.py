import subprocess
import time


def execute_command(command):
    process = subprocess.run(command, shell=True, capture_output=True, text=True)

    if process.returncode != 0:
        raise ValueError(
            f'Running "{command}" returned non-zero exitcode '
            f'with error {process.stderr}')

    processstdout = process.stdout
    if "error" in processstdout:
        raise Exception(processstdout)

    return processstdout


def _get_catalog_short(catalog):
    catalog_short = catalog.split('/')[-1]
    jj = catalog_short.rfind(".fits")

    return catalog_short[:jj]


def get_folder_structure(realization, version, release, survey, catalog):
    catalog_short = _get_catalog_short(catalog)

    interm_path = (f"{version}/{release}/{survey}"
                   f"/{catalog_short}/{version}.{realization}")

    return interm_path


def get_folder_structure_data(release, survey, catalog):
    catalog_short = _get_catalog_short(catalog)

    interm_path = f"{release}/{survey}/{catalog_short}"

    return interm_path


def get_script_header(outdir, jobname, time_txt, nodes, queue="regular"):
    script_txt = (
        "#!/bin/bash -l\n"
        "#SBATCH -C cpu\n"
        "#SBATCH --account=desi\n"
        f"#SBATCH -q {queue}\n"
        f"#SBATCH --nodes={nodes}\n"
        f"#SBATCH --time={time_txt}\n"
        f"#SBATCH --job-name={jobname}\n"
        f"#SBATCH --output={outdir}/log-{jobname}-%j.out\n"
        f"#SBATCH --error={outdir}/log-{jobname}-%j.out\n\n"
    )

    script_txt += "umask 0027\n\n"

    return script_txt


def get_script_text_for_master_node(command):
    script_txt = ("if [ $SLURM_NODEID -eq 0 ]; then\n"
                  f"    {command}\n"
                  "fi\n")

    return script_txt


def save_submit_script(script_txt, outdir, fname_core):
    submitter_fname = f"{outdir}/submit-{fname_core}.sh"
    with open(submitter_fname, 'w') as f:
        f.write(script_txt)

    return submitter_fname


def submit_script(
        submitter_fname, dep_jobid=None, afterwhat="afterok",
        skip=False, hold=False
):
    dependency_txt = ""
    if isinstance(dep_jobid, int) and dep_jobid > 0:
        dependency_txt = f"--dependency={afterwhat}:{dep_jobid} "
    elif isinstance(dep_jobid, list) and len(dep_jobid) > 0:
        valid_deps = [str(j) for j in dep_jobid if j > 0]
        if valid_deps:
            dependency_txt = f"--dependency={afterwhat}:{':'.join(valid_deps)} "

    if hold:
        hold_txt = "--hold "
    else:
        hold_txt = ""

    command = f"sbatch {hold_txt}{dependency_txt}{submitter_fname}"
    print(command)
    if skip:
        jobid = -1
    else:
        processstdout = execute_command(command)

        # Submitted batch job 19583619
        jobid = int(str(processstdout).split(' ')[-1].strip())

        # limit slurm pings
        time.sleep(1)

    print(f"JobID: {jobid}")
    return jobid
