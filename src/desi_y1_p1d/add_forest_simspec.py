import argparse
import logging

import numpy as np
from astropy.io import fits as asfits

import qsotools.mocklib as lm
import qsotools.fiducial as fid


def get_parser():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--inputdir", '-i', help="Input directory.", required=True)
    parser.add_argument(
        "--expid", required=True,
        help="Expid. Copies simspec-{expid}.fits from inputdir to outputdir")
    # parser.add_argument("--overwrite", default=True, action="store_false",
    #     help="Overwrite if output file exits. Default is true.")
    parser.add_argument("--seed", help="If none, use expid as seed.", type=int)
    parser.add_argument(
        "--log2ngrid", type=int, default=18, help="Number of grid points")
    parser.add_argument(
        "--griddv", type=float, default=2.,
        help="Pixel size of the grid in km/s.")
    parser.add_argument("--dry", help="Do not save", action="store_true")
    parser.add_argument(
        "--check-mean-delta", action="store_true",
        help="Test if mean delta is small")

    return parser


def createEdgesFromCenters(wave_centers):
    npix = len(wave_centers)
    dlm = np.min(wave_centers[1:] - wave_centers[:-1])
    wave_edges = (wave_centers[0] - dlm / 2) + np.arange(npix + 1) * dlm

    return wave_edges


def main():
    args = get_parser().parse_args()

    logging.basicConfig(level=logging.DEBUG)
    initial_specsim_fname = f"{args.inputdir}/simspec-{args.expid}.fits"

    if args.seed is None:
        seed = int(args.expid)
    else:
        seed = args.seed

    lya_m = lm.LyaMocks(
        seed, N_CELLS=2**args.log2ngrid, DV_KMS=args.griddv, REDSHIFT_ON=True)
    lya_m.setCentralRedshift(3.0)
    w1 = (1 + lya_m.z_values[0]) * fid.LYA_WAVELENGTH
    w2 = (1 + lya_m.z_values[-1]) * fid.LYA_WAVELENGTH
    logging.info(f"Mock wave grid range: {w1} - {w2}")

    hdul = asfits.open(initial_specsim_fname, mode='update')

    wave = hdul['WAVE'].data
    true_flux_hdu = hdul['FLUX'].copy()
    true_flux_hdu.name = 'FLUX_TRUE'
    hdul.append(true_flux_hdu)
    wave_edges = createEdgesFromCenters(wave)
    fbrmap = hdul['FIBERMAP'].data
    idx_qsos = np.where(fbrmap['OBJTYPE'] == 'QSO')[0]
    z_qsos = hdul['TRUTH'].data['REDSHIFT'][idx_qsos]

    nqsos = idx_qsos.size
    logging.info(f"There are {nqsos} quasars. Generating transmissions.")

    # Generate nqsos transmission files
    _, fluxes, _ = lya_m.resampledMocks(
        nqsos, err_per_final_pixel=0,
        spectrograph_resolution=0, obs_wave_edges=wave_edges,
        keep_empty_bins=True)

    # Remove absorption above Lya
    logging.info("Removing absorption above Lya.")
    for i in range(nqsos):
        nonlya_ind = wave > fid.LYA_WAVELENGTH * (1 + z_qsos[i])
        fluxes[i][nonlya_ind] = 1

        lya_lim_ind = wave <= (fid.LYA_LIMIT_WAVELENGTH * (1 + z_qsos[i]))
        fluxes[i][lya_lim_ind] = 0

        if args.check_mean_delta:
            _zzz = wave[~nonlya_ind] / fid.LYA_WAVELENGTH - 1
            delta = fluxes[i][~nonlya_ind] / lm.lognMeanFluxGH(_zzz) - 1
            assert np.close(delta.mean(), 0, atol=1e-3, rtol=1e-3)

    logging.info("Multiply transmissions with FLUX")
    hdul['FLUX'].data[idx_qsos] *= fluxes

    logging.info("Multiply PHOT_{ARM} with FLUX.")
    for arm in ['B', 'R', 'Z']:
        wave_arm = hdul[f'WAVE_{arm}'].data
        i1, i2 = np.searchsorted(wave, wave_arm[[0, -1]])
        # dark_curr = np.min(hdul[f'PHOT_{arm}'])
        hdul[f'PHOT_{arm}'].data[idx_qsos] *= fluxes[:, i1:i2 + 1]

    if not args.dry:
        hdul.flush()
    hdul.close()

    logging.info("Done")
