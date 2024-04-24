import argparse
import fitsio


def get_parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("DLA_CAT", help="DLA catalogs")

    parser.add_argument(
        "--cnn-conf-high", help="cnn confidence cut for SNR>cnn-snr-divide",
        default=0., type=float)
    parser.add_argument(
        "--cnn-conf-low", help="cnn confidence cut SNR<cnn-snr-divide",
        default=0.3, type=float)
    parser.add_argument(
        "--cnn-snr-divide", help="cnn snr threshold", default=3.0, type=float)
    parser.add_argument(
        "--gp-conf", help="gp confidence cut", default=0, type=float)
    parser.add_argument(
        "--nhi", help="DLA column density", default=20.3, type=float)

    return parser


def get_fname_dla_base(fname):
    if fname.endswith(".gz"):
        i = fname.rfind(".fits.gz")
    elif fname.endswith(".fits"):
        i = fname.rfind(".fits")
    else:
        raise Exception("Unknown file extention.")

    return fname[:i]


def cnn_selection(dla_cat, args):
    if "CNN_DLA_CONFIDENCE" in dla_cat.dtype.names:
        cnn_conf_dname = "CNN_DLA_CONFIDENCE"
    elif "DLA_CONFIDENCE" in dla_cat.dtype.names:
        cnn_conf_dname = "DLA_CONFIDENCE"
    else:
        return True

    whigh = (
        (dla_cat[cnn_conf_dname] > args.cnn_conf_high)
        & (dla_cat['S2N'] > args.cnn_snr_divide)
    )
    wlow = (
        (dla_cat[cnn_conf_dname] > args.cnn_conf_low)
        & (dla_cat['S2N'] <= args.cnn_snr_divide)
    )

    return whigh | wlow


def gp_selection(dla_cat, args):
    if "GP_DLA_CONFIDENCE" not in dla_cat.dtype.names:
        return True

    return dla_cat["GP_DLA_CONFIDENCE"] > args.gp_conf


def main():
    args = get_parser().parse_args()

    dla_cat = fitsio.FITS(args.DLA_CAT)[1].read()

    lycont_lim = 1215.67 / 910.
    wzdla = (
        (dla_cat['Z_DLA'] < dla_cat['Z_QSO'])
        & ((lycont_lim * (1 + dla_cat['Z_DLA'])) > (1 + dla_cat['Z_QSO']))
    )
    wnhi = dla_cat['NHI'] > args.nhi

    wall = ((cnn_selection(dla_cat, args) | gp_selection(dla_cat, args))
            & wnhi & wzdla)

    final_dla_catalog = dla_cat[wall]

    fname_dla_base = get_fname_dla_base(args.DLA_CAT)

    print(f"# DLA in the final catalog {final_dla_catalog.size:d}.")
    fname = (f"{fname_dla_base}"
             f"-nhi{args.nhi:.1f}-cnnSNR{args.cnn_snr_divide:.1f}"
             f"-highcut{args.cnn_conf_high:.1f}"
             f"-lowcut{args.cnn_conf_low:.1f}"
             f"-gpconf{args.gp_conf:.1f}.fits")

    print(f"Saved as {fname}")
    with fitsio.FITS(fname, 'rw', clobber=True) as fts:
        fts.write(final_dla_catalog, extname='DLACAT')
