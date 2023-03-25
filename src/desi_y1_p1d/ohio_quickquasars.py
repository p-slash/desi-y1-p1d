import argparse
from os import makedirs
from datetime import timedelta

from desi_y1_p1d import utils


def get_parser():
    """Constructs the parser needed for the script.

    Returns
    -------
    parser: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    folder_group = parser.add_argument_group("Folder settings")
    qq_group = parser.add_argument_group("Quickquasars settings")
    qsonic_group = parser.add_argument_group("QSOnic settings")
    run_group = parser.add_argument_group("SLURM settings")

    folder_group.add_argument(
        "--rootdir", help="Root dir for mocks", required=True)
    folder_group.add_argument("--delta-dir", help="for delta reductions")
    folder_group.add_argument("--realization", type=int, required=True)
    folder_group.add_argument(
        "--version", required=True, help="e.g., v1.2")
    folder_group.add_argument(
        "--release", default="iron", help="Release")
    folder_group.add_argument(
        "--survey", default="main", help="Survey")
    folder_group.add_argument(
        "--catalog", default="all_v0", help="Catalog")
    folder_group.add_argument(
        "--suffix", default="",
        help="suffix for the realization if custom parameters are passed.")

    qq_group.add_argument(
        "--nexp", type=int, default=1, help="Number of exposures.")
    qq_group.add_argument("--dla", help="Could be 'random' or file.")
    qq_group.add_argument(
        "--bal", type=float, default=0,
        help="Add BAL features with the specified probability. typical: 0.16")
    qq_group.add_argument(
        "--boring", action="store_true", help="Boring mocks.")
    qq_group.add_argument("--zmin-qso", type=float, default=1.8, help="Min z")
    qq_group.add_argument(
        "--seed", default="62300", help="Realization number is concatenated.")
    qq_group.add_argument(
        "--cont-dwave", type=float, default=2.0,
        help="True continuum wavelength steps.")

    run_group.add_argument("--nodes", type=int, default=1, help="Nodes")
    run_group.add_argument("--nthreads", type=int, default=128, help="Threads")
    run_group.add_argument("--time", type=float, help="In hours", default=0.5)
    run_group.add_argument(
        "--batch", action="store_true", help="Submit the job.")

    qsonic_group.add_argument(
        "--wave1", type=float, default=3600.,
        help="First observed wavelength edge.")
    qsonic_group.add_argument(
        "--wave2", type=float, default=6600.,
        help="Last observed wavelength edge.")
    qsonic_group.add_argument(
        "--forest-w1", type=float, default=1050.,
        help="First forest wavelength edge.")
    qsonic_group.add_argument(
        "--forest-w2", type=float, default=1180.,
        help="Last forest wavelength edge.")

    return parser


def get_sysopt(args):
    sysopt = ""
    rseed = f"{args.seed}{args.realization}"
    OPTS_QQ = (f"--zmin {args.zmin_qso} --zbest --bbflux --seed {rseed}"
               f" --exptime {args.nexp}000 --save-continuum"
               f" --save-continuum-dwave {args.cont_dwave}")

    if args.dla:
        sysopt += "1"
        OPTS_QQ += f" --dla {args.dla}"

    # if args.metals:
    #     sysopt += "2"
    #     OPTS_QQ += f" --metals {args.metals}"

    # if args.metals_from_file:
    #     sysopt += "3"
    #     OPTS_QQ += f" --metals-from-file {args.metals_from_file}"

    if args.bal and args.bal > 0:
        sysopt += "4"
        OPTS_QQ += f" --balprob {args.bal}"

    OPTS_QQ += " --sigma_kms_fog 0"
    sysopt += "5"

    if args.boring:
        sysopt += "6"
        OPTS_QQ += " --no-transmission"

    if not sysopt:
        sysopt = "0"

    return sysopt, OPTS_QQ


def create_directories(args, sysopt):
    interm_paths = (f"{args.version}/{args.release}/{args.survey}"
                    f"/{args.catalog}/{args.version}.{args.realization}")
    basedir = (f"{args.rootdir}/{interm_paths}")
    indir = f"{basedir}/transmissions"

    foldername = f"desi-{args.version[1]}.{sysopt}-{args.nexp}{args.suffix}"
    outdir = f"{basedir}/{foldername}"
    outdeltadir = f"{args.delta_dir}/{interm_paths}/{foldername}"

    # make directories to store logs and spectra
    makedirs(outdir, exist_ok=True)
    makedirs(f"{outdir}/logs", exist_ok=True)
    makedirs(f"{outdir}/spectra-16", exist_ok=True)
    makedirs(indir, exist_ok=True)
    makedirs(outdeltadir, exist_ok=True)
    makedirs(f"{outdeltadir}/results", exist_ok=True)

    return indir, outdir, outdeltadir


def create_qq_script(
        realization, indir, outdir, OPTS_QQ, nodes, nthreads, time, dla
):
    time_txt = timedelta(hours=time)

    command = (f"srun -N 1 -n 1 -c {nthreads} "
               f"quickquasars -i \\$tfiles --nproc {nthreads} "
               f"--outdir {outdir}/spectra-16 {OPTS_QQ}")

    script_txt = utils.get_script_header(
        outdir, f"ohio-qq-y1-{realization}", time_txt, nodes)

    script_txt += 'echo "get list of skewers to run ..."\n\n'

    script_txt += f"files=\\`ls -1 {indir}/*/*/lya-transmission*.fits*\\`\n"
    script_txt += "nfiles=\\`echo \\$files | wc -w\\`\n"
    script_txt += f"nfilespernode=\\$(( \\$nfiles/{nodes} + 1))\n\n"

    script_txt += 'echo "n files =" \\$nfiles\n'
    script_txt += 'echo "n files per node =" \\$nfilespernode\n\n'

    script_txt += "first=1\n"
    script_txt += "last=\\$nfilespernode\n"
    script_txt += "for node in \\`seq $nodes\\` ; do\n"
    script_txt += "    echo 'starting node \\$node'\n\n"

    script_txt += "    # list of files to run\n"
    script_txt += "    if (( \\$node == $nodes )) ; then\n"
    script_txt += "        last=""\n"
    script_txt += "    fi\n"
    script_txt += "    echo \\${first}-\\${last}\n"
    script_txt += ("    tfiles=\\`echo \\$files | cut -d "
                   " -f \\${first}-\\${last}\\`\n")
    script_txt += "    first=\\$(( \\$first + \\$nfilespernode ))\n"
    script_txt += "    last=\\$(( \\$last + \\$nfilespernode ))\n"
    script_txt += f"    command={command}\n\n"

    script_txt += "    echo \\$command\n"
    script_txt += f'    echo "log in {outdir}/logs/node-\\$node.log"\n\n'

    script_txt += f"    \\$command >& {outdir}/logs/node-\\$node.log &\n"
    script_txt += "done\n\n"

    script_txt += "wait\n"
    script_txt += "echo 'END'\n\n"

    command = (f"desi_zcatalog -i {outdir}/spectra-16 -o {outdir}/zcat.fits "
               "--minimal --prefix zbest\n")

    if dla:
        command += (f"get-qq-true-dla-catalog {outdir}/spectra-16 {outdir} "
                    f"--nproc {nthreads}\n")

    script_txt += "if [ \\$SLURM_NODEID -eq 0 ]; then\n"
    script_txt += f"    {command}"
    script_txt += "fi\n"

    script_txt += "EOF\n\n"

    submitter_fname = utils.save_submitter_script(
        script_txt, outdir, "quickquasars",
        "source /global/common/software/desi/desi_environment.sh main")

    return submitter_fname


def create_qsonic_script(
        realization, indir, outdeltadir, wave1, wave2,
        forest_w1, forest_w2, dep_jobid=None,
        coadd_arms=True, skip_resomat=False, time=0.3, nodes=1
):
    time_txt = timedelta(hours=time)
    script_txt = utils.get_script_header(
        outdeltadir, f"qsonic-{realization}", time_txt, nodes)

    command = "srun -n 128 -c 2 qsonic-fit \\\n"
    command += f"-i {indir}/spectra-16 \\\n"
    command += f"--catalog {indir}/zcat.fits \\\n"
    command += f"-o {outdeltadir}/Delta \\\n"
    command += f"--mock-analysis \\\n"
    command += f"--rfdwave 0.8 --skip 0.2 \\\n"
    command += f"--no-iterations 10 \\\n"
    command += f"--wave1 {wave1} --wave2 {wave2} \\\n"
    command += f"--forest-w1 {forest_w1} --forest-w2 {forest_w2}"
    if coadd_arms:
        command += " \\\n--coadd-arms"
    if skip_resomat:
        command += " \\\n--skip-resomat"

    command += "\n"

    script_txt += command
    script_txt += "EOF\n\n"

    submitter_fname = utils.save_submitter_script(
        script_txt, outdeltadir, "qsonic-fit", dep_jobid=dep_jobid)

    return submitter_fname


def main():
    parser = get_parser()
    args = parser.parse_args()
    jobid = -1

    sysopt, OPTS_QQ = get_sysopt(args)

    indir, outdir, outdeltadir = create_directories(args, sysopt)

    submitter_fname_qq = create_qq_script(
        args.realization, indir, outdir, OPTS_QQ,
        args.nodes, args.nthreads, args.time, args.dla)

    if args.batch:
        jobid = utils.submit_script(submitter_fname_qq)

    submitter_fname_qsonic = create_qsonic_script(
        args.realization, indir, outdeltadir, args.wave1, args.wave2,
        args.forest_w1, args.forest_w2, dep_jobid=jobid)

    if args.batch:
        jobid = utils.submit_script(submitter_fname_qsonic)
