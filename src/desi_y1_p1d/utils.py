import subprocess
import time


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
    script_txt = "if [ \\$SLURM_NODEID -eq 0 ]; then\n"
    script_txt += f"    {command}"
    script_txt += "fi\n"

    return script_txt


def save_submit_script(script_txt, outdir, fname_core):
    submitter_fname = f"{outdir}/submit-{fname_core}.sh"
    with open(submitter_fname, 'w') as f:
        f.write(script_txt)

    return submitter_fname


def submit_script(
        submitter_fname, dep_jobid=None, afterwhat="afterok", skip=False
):
    dependency_txt = ""
    if isinstance(dep_jobid, int) and dep_jobid > 0:
        dependency_txt = f"--dependency={afterwhat}:{dep_jobid} "
    elif isinstance(dep_jobid, list) and len(dep_jobid) > 0:
        valid_deps = [str(j) for j in dep_jobid if j > 0]
        if valid_deps:
            dependency_txt = f"--dependency={afterwhat}:{':'.join(valid_deps)} "

    command = f"{dependency_txt}{submitter_fname} | tr -dc '0-9'"
    print(f"sbatch {command}")
    if skip:
        jobid = -1
    else:
        jobid = int(subprocess.check_output(["sbatch", command]))
        # limit slurm pings
        time.sleep(40)
    print(f"JobID: {jobid}")
    return jobid
