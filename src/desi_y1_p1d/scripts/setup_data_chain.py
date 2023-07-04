import argparse
from os import umask

from desi_y1_p1d.ohio_jobs import DataJobChain
from desi_y1_p1d.settings import DesiDataSettings


def get_parser():
    """Constructs the parser needed for the script.

    Returns
    -------
    parser: argparse.ArgumentParser
    """
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument(
        "Setting", choices=DesiDataSettings.all_settings, nargs='?',
        help="Base setting for the pipeline. Values can be changed using the options below.")
    parser.add_argument(
        "--print-current-settings", action="store_true",
        help="Shows the current settings and exit.")
    parser.add_argument(
        "--list-available-settings", action="store_true",
        help="List available settings and exit.")

    folder_group = parser.add_argument_group("Folder settings")
    run_group = parser.add_argument_group("SLURM settings")
    qsonic_group = parser.add_argument_group("QSOnic settings")
    qmle_group = parser.add_argument_group("QMLE settings")

    folder_group.add_argument("--redux", help="DESI redux spectro.")
    folder_group.add_argument(
        "--delta-dir",
        help="Directory that has lookuptables/ and specres-list.txt and delta reductions")
    folder_group.add_argument("--release", help="Release")
    folder_group.add_argument("--survey", help="Survey")
    folder_group.add_argument("--catalog", help="Catalog")

    run_group.add_argument("--nodes", type=int, help="Nodes")
    run_group.add_argument("--nthreads", type=int, help="Threads")
    run_group.add_argument("--time", type=float, help="In minutes")
    run_group.add_argument(
        "--test", dest="queue", action="store_const", const="debug",
        help="Run on debug queue.")
    run_group.add_argument("--batch", action="store_true", help="Submit the job.")
    run_group.add_argument(
        "--run-only-these", nargs="*",
        choices=['Lya', 'SB1', 'SB2', 'SB3', 'LyaCalib', 'SB1Calib', 'SB2Calib', 'SB3Calib'],
        help="Only run passed arguments.")

    qsonic_group.add_argument(
        "--cont-order", type=int,
        help="Order of continuum fitting polynomial.")
    qsonic_group.add_argument(
        "--suffix",
        help="suffix for QSOnic reduction if custom parameters are passed.")
    qsonic_group.add_argument(
        "--skip-qsonics", nargs="*", choices=['Lya', 'SB1', 'SB2', 'SB3', 'all'],
        help="Skip continuum fitting step for listed forests.")

    qmle_group.add_argument(
        "--skip-qmles", nargs="*", choices=['Lya', 'SB1', 'SB2', 'SB3', 'all'],
        help="Skip QMLE. Makes sense if you only want to fit the continuum.")

    return parser


def main(options=None):
    parser = get_parser()
    args = parser.parse_args(options)

    if args.list_available_settings:
        DesiDataSettings.list_available_settings()
        exit(0)

    if args.Setting is None:
        parser.error("the following arguments are required: Setting")

    oh_sett = DesiDataSettings(args.Setting)
    oh_sett.update_from_args(args)

    if args.print_current_settings:
        oh_sett.print()
        exit(0)

    if not args.delta_dir:
        print("The following argument is required: --delta-dir")
        exit(1)

    # Mask permissions to
    # 0 -> no mask for owner
    # 2 -> remove writing permissions for the group
    # 7 -> all permissions for others
    umask(0o027)

    job_chain = DataJobChain(args.delta_dir, oh_sett.settings)
    print("Setting up DataJobChain.")
    job_chain.schedule(args.run_only_these)
    print("DataJobChain done.")
    print("==================================================")
    job_chain.save_jobids()
