import argparse

from desi_y1_p1d.ohio_jobs import JobChain


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

    folder_group.add_argument("--rootdir", help="Root dir for mocks", required=True)
    folder_group.add_argument("--delta-dir", help="for delta reductions")
    folder_group.add_argument("--rn1", type=int, default=0, help="Starting number for realization")
    folder_group.add_argument("--nrealizations", type=int, default=1, help="Number of realization")
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

    qq_group.add_argument("--nexp", type=int, default=1, help="Number of exposures.")
    qq_group.add_argument("--dla", help="Could be 'random' or file.")
    qq_group.add_argument(
        "--bal", type=float, default=0,
        help="Add BAL features with the specified probability. typical: 0.16")
    qq_group.add_argument(
        "--qq-env-command", help="Environment command for quickquasars",
        default="source /global/common/software/desi/desi_environment.sh main")
    qq_group.add_argument("--boring", action="store_true", help="Boring mocks.")
    qq_group.add_argument("--zmin-qso", type=float, default=1.8, help="Min z")
    qq_group.add_argument(
        "--seed-qq", default="62300",
        help="Realization number is concatenated to the right.")
    qq_group.add_argument(
        "--cont-dwave", type=float, default=2.0,
        help="True continuum wavelength steps.")
    qq_group.add_argument(
        "--skip-quickquasars", action="store_true",
        help="Skip quickquasars. Makes sense if you only want to fit the continuum.")

    trans_group.add_argument(
        "--no-transmissions", action="store_true",
        help="Do not generate transmission files.")
    trans_group.add_argument(
        "--seed-qsotools", default="332298",
        help="Realization number is concatenated to the left.")

    run_group.add_argument("--nodes", type=int, default=1, help="Nodes")
    run_group.add_argument("--nthreads", type=int, default=128, help="Threads")
    run_group.add_argument("--time", type=float, help="In hours", default=0.5)
    run_group.add_argument("--batch", action="store_true", help="Submit the job.")

    qsonic_group.add_argument(
        "--wave1", type=float, default=3600.,
        help="First observed wavelength edge.")
    qsonic_group.add_argument(
        "--wave2", type=float, default=6600.,
        help="Last observed wavelength edge.")
    qsonic_group.add_argument(
        "--forest-w1", type=float, default=1040.,
        help="First forest wavelength edge.")
    qsonic_group.add_argument(
        "--forest-w2", type=float, default=1200.,
        help="Last forest wavelength edge.")
    qsonic_group.add_argument(
        "--cont-order", type=int, default=1,
        help="Order of continuum fitting polynomial.")

    return parser


def main(options=None):
    parser = get_parser()
    args = parser.parse_args(options)

    job_chain = JobChain(
        args.rootdir, args.rn1, args.version, args.release,
        args.survey, args.catalog,
        args.nexp, args.zmin_qso, args.cont_dwave, args.dla, args.bal,
        args.boring, args.seed_qq,
        args.qq_env_command, args.suffix,
        args.seed_qsotools, args.delta_dir, args.wave1, args.wave2,
        args.forest_w1, args.forest_w2, args.cont_order
    )

    for jj in range(args.nrealizations):
        print(f"Setting up JobChain for realization {jj+args.rn1}.")

        job_chain.schedule(
            args.nodes, args.nthreads, args.time,
            args.batch, args.no_transmissions, args.skip_quickquasars
        )

        job_chain.inc_realization()

        print(f"JobChain done for realization {jj+args.rn1}.")
        print("==================================================")
