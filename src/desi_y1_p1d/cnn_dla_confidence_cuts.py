import argparse
import fitsio


def get_parser():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("DLA_CAT", help="DLA catalogs")

    parser.add_argument(
        "--cnn-conf-high", help="cnn confidence cut for SNR>cnn-snr-divide",
        default=0., type=float)
    parser.add_argument(
        "--cnn-conf-low", help="cnn confidence cut SNR<cnn-snr-divide", default=0.3, type=float)
    parser.add_argument("--cnn-snr-divide", help="cnn snr threshold", default=3.0, type=float)
    # parser.add_argument("--gp-conf", help="gp confidence cut", default=0.9, type=float)
    parser.add_argument("--nhi", help="DLA column density", default=20.3, type=float)

    return parser


def get_fname_dla_base(fname):
    if fname.endswith(".gz"):
        i = fname.rfind(".fits.gz")
    elif fname.endswith(".fits"):
        i = fname.rfind(".fits")
    else:
        raise Exception("Unknown file extention.")

    return fname[:i]


def main():
    args = get_parser().parse_args()

    dla_cat = fitsio.FITS(args.DLA_CAT)[1].read()

    wnhi = dla_cat['NHI'] > args.nhi
    whigh = (
        (dla_cat['DLA_CONFIDENCE'] > args.cnn_conf_high)
        & (dla_cat['S2N'] > args.cnn_snr_divide)
    )
    wlow = (
        (dla_cat['DLA_CONFIDENCE'] > args.cnn_conf_low)
        & (dla_cat['S2N'] <= args.cnn_snr_divide)
    )
    wall = wnhi & (whigh | wlow)

    final_dla_catalog = dla_cat[wall]

    fname_dla_base = get_fname_dla_base(args.DLA_CAT)

    print(f"# DLA in the final catalog {final_dla_catalog.size:d}.")
    fname = (f"{fname_dla_base}"
             f"-nhi{args.nhi:.1f}-cnnSNR{args.cnn_snr_divide:.1f}"
             f"-highcut{args.cnn_conf_high:.1f}"
             f"-lowcut{args.cnn_conf_low:.1f}.fits")

    with fitsio.FITS(fname, 'rw', clobber=True) as fts:
        fts.write(final_dla_catalog, extname='DLACAT')
