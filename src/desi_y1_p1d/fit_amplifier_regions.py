import argparse
import fitsio
import numpy as np


def get_parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("SNR_CAT", help="Continuum chi2 catalog")
    parser.add_argument("INDIR", help="Input directory for SNR catalogs")
    parser.add_argument("OUTFILE", help="Output FITS file")
    parser.add_argument(
        "--snr-edges", help="SNR edges", type=float,
        default=[0.3, 1.0, 1.5, 2.0, 3.0, 5.0, 100.0], nargs='+')
    parser.add_argument(
        "--wave-edges", help="Wave edges", type=float,
        default=[3600, 4800, 6000, 7200], nargs='+')

    return parser


def fitAmplifierRegions(data, wave_res):
    wave = data['lambda']
    nres = wave_res.size - 1
    wave_res[-1] = min(wave[-1] + 1, wave_res[-1])

    eta_fit = np.empty((2, nres))
    eta = data['eta'] - 1
    s = data['e_eta'].copy()
    w = s > 0
    we = s[w]**-2
    wave1 = wave[w]

    for i in range(nres):
        i1, i2 = np.searchsorted(wave1, wave_res[i:i + 2])
        eta_fit[0, i] = np.sum(eta[w][i1:i2] * we[i1:i2]) / np.sum(we[i1:i2])
        eta_fit[1, i] = 1 / np.sqrt(np.sum(we[i1:i2]))

    varlss_fit = np.empty((2, nres))
    varlss = data['var_lss']
    s = data['e_var_lss'].copy()
    w = s > 0
    we = s[w]**-2
    wave1 = wave[w]

    for i in range(nres):
        i1, i2 = np.searchsorted(wave1, wave_res[i:i + 2])
        wee = we[i1:i2]
        varlss_fit[0, i] = np.sum(varlss[w][i1:i2] * wee) / np.sum(wee)
        varlss_fit[1, i] = 1 / np.sqrt(np.sum(wee))

    return wave_res, eta_fit, varlss_fit


def main():
    args = get_parser().parse_args()
    args.snr_edges = np.array(args.snr_edges)
    args.wave_edges = np.array(args.wave_edges)
    args.snr_edges.sort()
    args.wave_edges.sort()

    snr_centers = np.empty(args.snr_edges.size - 1)

    cont_chi2_cat = fitsio.read(args.SNR_CAT)
    cat_calib_snrs = [
        fitsio.read(
            f"{args.INDIR}/attributes-snr"
            f"{args.snr_edges[i]:.1f}-{args.snr_edges[i + 1]:.1f}"
            "-variance-stats.fits", ext='VAR_FUNC')
        for i in range(snr_centers.size)
    ]

    for i in range(snr_centers.size):
        w = ((args.snr_edges[i] <= cont_chi2_cat['MEANSNR'][:, 0])
             & (cont_chi2_cat['MEANSNR'][:, 0] < args.snr_edges[i + 1]))

        snr_centers[i] = np.mean(cont_chi2_cat['MEANSNR'][w, 0])

    snr_amp_eta = np.empty((2, args.wave_edges.size - 1, snr_centers.size))
    snr_amp_varlss = np.empty((2, args.wave_edges.size - 1, snr_centers.size))

    for i, data in enumerate(cat_calib_snrs):
        wave_res, snr_amp_eta[:, :, i], snr_amp_varlss[:, :, i] = \
            fitAmplifierRegions(data, args.wave_edges)

    with fitsio.FITS(args.OUTFILE, 'rw', clobber=True) as fts:
        fts.write(wave_res, extname="WAVE_EDGES")
        fts.write(snr_centers, extname="SNR_CENTERS")
        fts.write(snr_amp_eta, extname="ETA")
        fts.write(snr_amp_varlss, extname="VAR_LSS")

    eta_0 = np.empty((wave_res.size - 1))
    eta_1 = np.empty((wave_res.size - 1))
    vl_A = np.empty((wave_res.size - 1))
    vl_beta = np.empty((wave_res.size - 1))
    snr_log_c = np.log(snr_centers)
    log_var_lss = np.log(snr_amp_varlss[0])
    for i in range(eta_0.size):
        p = np.polyfit(snr_centers, snr_amp_eta[0, i], 1,
                       w=1 / snr_amp_eta[1, i])
        eta_0[i] = p[0]
        eta_1[i] = p[1]

        p = np.polyfit(snr_log_c, log_var_lss[i], 1,
                       w=log_var_lss[i] / snr_amp_varlss[1, i])
        vl_A[i] = np.exp(p[0])
        vl_beta[i] = p[1]

    with fitsio.FITS(args.OUTFILE, 'rw') as fts:
        fts.write(
            [wave_res[:-1], eta_0, eta_1, vl_A, vl_beta],
            names=['wave', 'eta_0', 'eta_1', 'A', 'beta'],
            extname='NOISE_COR')
