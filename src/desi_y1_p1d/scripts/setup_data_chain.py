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
    qsonic_group = parser.add_argument_group("QSOnic settings")
    run_group = parser.add_argument_group("SLURM settings")

    folder_group.add_argument(
        "--indir", help="Healpix directory.", required=True)
    folder_group.add_argument("--delta-dir", help="Base dir for delta reductions")

    folder_group.add_argument(
        "--version", required=True, help="e.g., v1.2")
    folder_group.add_argument(
        "--catalog", help="Catalog",
        default=("/global/cfs/cdirs/desi/survey/catalogs/Y1/QSO/iron/"
                 "QSO_cat_iron_main_dark_healpix_v0.fits")
    )
    folder_group.add_argument(
        "--suffix", default="",
        help="suffix for the realization if custom parameters are passed.")

    mask_group = parser.add_argument_group('Masking options')

    mask_group.add_argument(
        "--sky-mask",
        help="Sky mask file.")
    mask_group.add_argument(
        "--bal-mask", action="store_true",
        help="Mask BALs (assumes it is in catalog).")
    mask_group.add_argument(
        "--dla-mask",
        help="DLA catalog to mask.")

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
        "--forest-w1", type=float, default=1040.,
        help="First forest wavelength edge.")
    qsonic_group.add_argument(
        "--forest-w2", type=float, default=1200.,
        help="Last forest wavelength edge.")

    return parser
