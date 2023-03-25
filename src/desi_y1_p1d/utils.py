import subprocess


def get_script_header(outdir, jobname, time_txt, nodes):
    script_txt = (
        "#!/bin/bash -l\n"
        "#SBATCH -C cpu\n"
        "#SBATCH --account=desi\n"
        f"#SBATCH --nodes={nodes}\n"
        f"#SBATCH --time={time_txt}\n"
        f"#SBATCH --job-name={jobname}\n"
        f"#SBATCH --output={outdir}/log-{jobname}.txt\n\n"
    )

    return script_txt


def save_submitter_script(
        script_txt, outdir, fname_core,
        env_command=None, dep_jobid=None):
    script_fname = f"{outdir}/run-{fname_core}.sl"
    submitter_fname = f"{outdir}/submit-{fname_core}.sh"

    if dep_jobid and dep_jobid > 0:
        dependency_txt = f"--dependency=afterok:{dep_jobid} "
    else:
        dependency_txt = ""

    with open(submitter_fname, 'w') as f:
        if env_command:
            f.write(f"{env_command}\n\n")
        f.write(f"cat > {script_fname} <<EOF\n")
        f.write(script_txt)
        # f"job_id_current=$(sbatch {scriptname} | tr -dc '0-9')\n"
        f.write(f"sbatch {dependency_txt}{script_fname} | tr -dc '0-9'\n")

    return submitter_fname


def submit_script(submitter_fname):
    print(f"sh {submitter_fname}...")
    jobid = subprocess.check_output(["sh", submitter_fname])
    print(f"JobID: {jobid}")
    return jobid