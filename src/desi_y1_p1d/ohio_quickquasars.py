import argparse
from os import makedirs
from datetime import timedelta
import subprocess


def get_parser():
    """Constructs the parser needed for the script.

    Returns
    -------
    parser: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--rootdir", help="Root dir for mocks", required=True)
    parser.add_argument("--delta-dir", help="for delta reductions")
    parser.add_argument("--realization", type=int, required=True)
    parser.add_argument("--version", required=True, help="e.g., v1.2")
    parser.add_argument("--release", default="iron", required=True)
    parser.add_argument("--survey", default="main", required=True)
    parser.add_argument("--catalog", default="all_v0", required=True)
    parser.add_argument(
        "--nexp", type=int, default=1, required=True,
        help="Number of exposures.")
    parser.add_argument("--dla", help="Could be 'random' or file.")
    parser.add_argument(
        "--bal", type=float, default=0,
        help="Add BAL features with the specified probability. typical: 0.16")
    parser.add_argument(
        "--suffix",
        help="suffix for the realization if custom parameters are passed.")
    parser.add_argument("--nodes", type=int, default=1)
    parser.add_argument("--nthreads", type=int, default=128)
    parser.add_argument("--time", type=float, help="In hours", default=0.5)
    parser.add_argument("--no-submit", action="store_true")
    parser.add_argument("--boring", action="store_true", help="Boring mocks.")
    parser.add_argument("--zmin-qso", type=float, default=1.8)
    parser.add_argument(
        "--seed", default="62300", help="Realization number is concatenated.")
    parser.add_argument("--cont-dwave", type=float, default=2.0)

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

    foldername = f"desi-${args.version:1:1}.{sysopt}-{args.nexp}{args.suffix}/"
    outdir = f"{basedir}/{foldername}/"
    outdeltadir = f"{args.delta_dir}/{interm_paths}/{foldername}/"

    # make directories to store logs and spectra
    makedirs(outdir, exist_ok=True)
    makedirs(f"{outdir}/logs", exist_ok=True)
    makedirs(f"{outdir}/spectra-16", exist_ok=True)
    makedirs(indir, exist_ok=True)
    makedirs(outdeltadir, exist_ok=True)
    makedirs(f"{outdeltadir}/results", exist_ok=True)

    return indir, outdir, outdeltadir


def create_script(
        realization, indir, outdir, OPTS_QQ, nodes, nthreads, time, dla
):
    script_fname = f"{outdir}/execute-quickquasars-run${realization}.sh"
    submitter_fname = f"{outdir}/submit-quickquasars-run${realization}.sh"
    time_txt = timedelta(hours=time)

    command = (f"srun -N 1 -n 1 -c {nthreads} "
               f"quickquasars -i $tfiles --nproc {nthreads}"
               f"--outdir {outdir}/spectra-16 ${OPTS_QQ}")

    script_txt = (
        "#!/bin/bash -l\n"
        "#SBATCH -C cpu\n"
        "#SBATCH --account=desi\n"
        f"#SBATCH --nodes={nodes}\n"
        f"#SBATCH --time={time_txt}\n"
        "#SBATCH --job-name=quickquasar_spectra\n"
        f"#SBATCH --output={outdir}/lyasim.log\n\n"
    )

    script_txt += 'echo "get list of skewers to run ..."\n\n'

    script_txt += f"files=`ls -1 {indir}/*/*/lya-transmission*.fits*`\n"
    script_txt += "nfiles=`echo $files | wc -w`\n"
    script_txt += f"nfilespernode=$(( $nfiles/{nodes} + 1))\n\n"

    script_txt += "echo 'n files =' $nfiles\n"
    script_txt += "echo 'n files per node =' $nfilespernode\n\n"

    script_txt += "first=1\n"
    script_txt += "last=$nfilespernode\n"
    script_txt += "for node in `seq $nodes` ; do\n"
    script_txt += "    echo 'starting node $node'\n\n"

    script_txt += "    # list of files to run\n"
    script_txt += "    if (( $node == $nodes )) ; then\n"
    script_txt += "        last=""\n"
    script_txt += "    fi\n"
    script_txt += "    echo ${first}-${last}\n"
    script_txt += "    tfiles=`echo $files | cut -d " " -f ${first}-${last}`\n"
    script_txt += "    first=$(( $first + $nfilespernode ))\n"
    script_txt += "    last=$(( $last + $nfilespernode ))\n"
    script_txt += f"    command={command}\n\n"

    script_txt += "    echo $command\n"
    script_txt += "    echo 'log in $outdir/logs/node-$node.log'\n\n"

    script_txt += f"    $command >& {outdir}/logs/node-$node.log &\n"
    script_txt += "done\n\n"

    script_txt += "wait\n"
    script_txt += "echo 'END'\n\n"

    command = (f"desi_zcatalog -i {outdir}/spectra-16 -o {outdir}/zcat.fits "
               "--minimal --prefix zbest\n")

    if dla:
        command += (f"get-qq-true-dla-catalog {outdir}/spectra-16 {outdir} "
                    f"--nproc {nthreads}\n")

    script_txt += "if [ $SLURM_NODEID -eq 0 ]; then\n"
    script_txt += f"    {command}"
    script_txt += "fi\n\n"

    script_txt += "EOF\n"

    with open(submitter_fname, 'w') as f:
        f.write("source /global/common/software/desi/desi_environment.sh main")
        f.write(f"cat > {script_fname} <<EOF")
        f.write(script_txt)
        f.write(f"sbatch {script_fname}")

    return submitter_fname


def submit_qq_script(submitter_fname):
    subprocess.call(f"sh {submitter_fname}")


def main():
    parser = get_parser()
    args = parser.parse_args()

    sysopt, OPTS_QQ = get_sysopt(args)

    indir, outdir, _ = create_directories(args, sysopt)

    submitter_fname = create_script(
        args.realization, indir, outdir, OPTS_QQ,
        args.nodes, args.nthreads, args.time)

    if not args.no_submit:
        submit_qq_script(submitter_fname)
