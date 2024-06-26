import argparse
from os import umask

from desi_y1_p1d.ohio_jobs import MockJobChain
from desi_y1_p1d.settings import OhioMockSettings


def get_parser():
    """Constructs the parser needed for the script.

    Returns
    -------
    parser: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "Setting", choices=OhioMockSettings.all_settings, nargs='?',
        help="Base setting for the pipeline. Values can be changed using the options below.")
    parser.add_argument(
        "--print-current-settings", action="store_true",
        help="Shows the current settings and exit.")
    parser.add_argument(
        "--list-available-settings", action="store_true",
        help="List available settings and exit.")

    folder_group = parser.add_argument_group("Folder settings")
    run_group = parser.add_argument_group("SLURM settings")
    qq_group = parser.add_argument_group("Quickquasars settings")
    trans_group = parser.add_argument_group("Transmission file settings")
    qsonic_group = parser.add_argument_group("QSOnic settings")
    qmle_group = parser.add_argument_group("QMLE settings")

    folder_group.add_argument("--root-dir", help="Root dir for mocks")
    folder_group.add_argument("--delta-dir", help="for delta reductions")
    folder_group.add_argument("--rn1", type=int, default=0, help="Starting number for realization")
    folder_group.add_argument("--nrealizations", type=int, default=1, help="Number of realization")
    folder_group.add_argument("--version", help="e.g., v1.2")
    folder_group.add_argument("--release", help="Release")
    folder_group.add_argument("--survey", help="Survey")
    folder_group.add_argument("--catalog", help="Catalog")
    folder_group.add_argument(
        "--suffix-qq", default="",
        help="suffix for the quickquasars realization if custom parameters are passed.")

    qq_group.add_argument("--nexp", type=int, help="Number of exposures.")
    qq_group.add_argument(
        "--dla", choices=["random", "file", ""], help="Add DLAs. 'file' adds clustered DLAs.")
    qq_group.add_argument(
        "--bal", type=float,
        help="Add BAL features with the specified probability. typical: 0.16")
    qq_group.add_argument("--env-command-qq", help="Environment command for quickquasars")
    qq_group.add_argument("--boring", action="store_true", help="Boring mocks.")
    qq_group.add_argument("--zmin-qq", type=float, help="Min redshift")
    qq_group.add_argument(
        "--base-seed-qq", help="Realization number is concatenated to the right.")
    qq_group.add_argument(
        "--cont-dwave", type=float,
        help="True continuum wavelength steps.")
    qq_group.add_argument(
        "--skip-qq", action="store_true",
        help="Skip quickquasars. Makes sense if you only want to fit the continuum.")

    trans_group.add_argument(
        "--no-transmissions", action="store_true",
        help="Do not generate transmission files.")
    trans_group.add_argument(
        "--base-seed-transmissions", help="Realization number is concatenated to the left.")

    run_group.add_argument("--nodes-qq", type=int, help="Nodes")
    run_group.add_argument("--nthreads-qq", type=int, help="Threads")
    run_group.add_argument("--time-qq", type=float, help="In minutes")
    run_group.add_argument(
        "--test", dest="queue", action="store_const", const="debug",
        help="Run on debug queue.")
    run_group.add_argument("--batch", action="store_true", help="Submit the job.")

    qsonic_group.add_argument(
        "--wave1", type=float,
        help="First observed wavelength edge.")
    qsonic_group.add_argument(
        "--wave2", type=float,
        help="Last observed wavelength edge.")
    qsonic_group.add_argument(
        "--forest-w1", type=float,
        help="First forest wavelength edge.")
    qsonic_group.add_argument(
        "--forest-w2", type=float,
        help="Last forest wavelength edge.")
    qsonic_group.add_argument(
        "--cont-order", type=int,
        help="Order of continuum fitting polynomial. Pass <0 for to true continuum.")
    qsonic_group.add_argument(
        "--suffix-qsonic",
        help="suffix for QSOnic reduction if custom parameters are passed.")
    qsonic_group.add_argument(
        "--skip-qsonics", action="store_true",
        help="Skip continuum fitting.")

    qmle_group.add_argument(
        "--skip-qmles", action="store_true",
        help="Skip QMLE. Makes sense if you only want to fit the continuum.")
    qmle_group.add_argument(
        "--ContinuumLogLambdaMargOrder", type=int,
        help="QMLE continuum marginalization order.")
    qmle_group.add_argument(
        "--PrecomputedFisher", help="Precomputed fisher path.")

    return parser


def main(options=None):
    parser = get_parser()
    args = parser.parse_args(options)

    if args.list_available_settings:
        OhioMockSettings.list_available_settings()
        exit(0)

    if args.Setting is None:
        parser.error("the following arguments are required: Setting")

    oh_sett = OhioMockSettings(args.Setting)
    oh_sett.update_from_args(args)

    if args.print_current_settings:
        oh_sett.print()
        exit(0)

    if not args.root_dir:
        print("The following argument is required: --root-dir")
        exit(1)

    # Mask permissions to
    # 0 -> no mask for owner
    # 2 -> remove writing permissions for the group
    # 7 -> all permissions for others
    umask(0o027)

    settings = oh_sett.settings

    job_chain = MockJobChain(args.root_dir, args.rn1, args.delta_dir, settings)

    for jj in range(args.nrealizations):
        print(f"Setting up MockJobChain for realization {jj + args.rn1}.")

        job_chain.schedule()

        job_chain.inc_realization(jj == args.nrealizations - 1)

        print(f"MockJobChain done for realization {jj+args.rn1}.")
        print("==================================================")

    job_chain.submitExtraCommands(jobname="qq-catalog", queue="regular")
    job_chain.save_jobids()
