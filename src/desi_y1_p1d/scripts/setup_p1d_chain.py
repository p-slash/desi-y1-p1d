import argparse


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
    trans_group = parser.add_argument_group("Transmission file settings")
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
        "--catalog", help="Catalog",
        default=("/global/cfs/cdirs/desi/survey/catalogs/Y1/QSO/iron/"
                 "QSO_cat_iron_main_dark_healpix_v0.fits")
    )
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
        "--qq-env-command", help="Environment command for quickquasars",
        default="source /global/common/software/desi/desi_environment.sh main")
    qq_group.add_argument(
        "--boring", action="store_true", help="Boring mocks.")
    qq_group.add_argument("--zmin-qso", type=float, default=1.8, help="Min z")
    qq_group.add_argument(
        "--seed-qq", default="62300",
        help="Realization number is concatenated to the right.")
    qq_group.add_argument(
        "--cont-dwave", type=float, default=2.0,
        help="True continuum wavelength steps.")

    trans_group.add_argument(
        "--no-transmissions", action="store_true",
        help="Do not generate transmission files.")
    trans_group.add_argument(
        "--seed-qsotools", default="332298",
        help="Realization number is concatenated to the left.")

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


def main(options=None):
    parser = get_parser()
    args = parser.parse_args(options)
    jobid = -1

    transmissions_dir = create_transmission_directory(
        args, not args.no_transmissions)

    submitter_fname_lya = create_lya_trans_gen_script(
        args.realization, transmissions_dir, args.catalog, args.seed_qsotools)

    if args.batch and submitter_fname_lya:
        jobid = utils.submit_script(submitter_fname_lya)

    sysopt, OPTS_QQ = get_sysopt(args)
    desibase_dir, outdelta_dir = create_qq_directories(args, sysopt)
    submitter_fname_qq = create_qq_script(
        args.realization, transmissions_dir, desibase_dir, OPTS_QQ,
        args.nodes, args.nthreads, args.time, args.dla,
        env_command=args.qq_env_command, dep_jobid=jobid)

    if args.batch:
        jobid = utils.submit_script(submitter_fname_qq)

    submitter_fname_qsonic = create_qsonic_script(
        desibase_dir, outdelta_dir, args.wave1, args.wave2,
        args.forest_w1, args.forest_w2, args.realization, dep_jobid=jobid)

    if args.batch and submitter_fname_qsonic:
        jobid = utils.submit_script(submitter_fname_qsonic)
