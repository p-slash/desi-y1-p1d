import argparse
import logging
from os import makedirs as os_makedirs

import fitsio
import numpy as np
# from scipy.interpolate import interp1d

import qsotools.fiducial as fid
from qsotools.mocklib import lognMeanFluxGH as TRUE_MEAN_FLUX

from desi_y1_p1d.get_deltas_from_pixsim_coadd import (
    getForestAnalysisRegion, saveDelta)


class Reducer():
    def __init__(self, args):
        self.args = args
        simspec_hdu = fitsio.FITS(args.simspec_file)
        self.truth_fibermap = simspec_hdu['FIBERMAP'].read()
        self.truth_qso_idx = np.where(
            self.truth_fibermap['OBJTYPE'] == 'QSO')[0]

        targetids = self.truth_fibermap[self.truth_qso_idx]['TARGETID']

        logging.info(f"Number of QSO {self.truth_qso_idx.size}")
        logging.info(f"Unique targetid {np.unique(targetids).size}")

        self.truth_wave = simspec_hdu['WAVE'].read()
        self.influx = simspec_hdu['FLUX'].read()
        self.truth_flux = simspec_hdu['FLUX_TRUE'].read()
        self.truth_zqso = simspec_hdu['TRUTH']['REDSHIFT'].read()
        simspec_hdu.close()

        suffix = args.simspec_file.split("/")[-1]
        _len = len("simspec-")
        self.odelta_fname = f"{self.args.outputdir}/delta-{suffix[_len:]}"

    def __call__(self):
        delta_hdu = fitsio.FITS(self.odelta_fname, "rw", clobber=True)

        for i in self.truth_qso_idx:
            thid = self.truth_fibermap['TARGETID'][i]
            ra = self.truth_fibermap['TARGET_RA'][i]
            dec = self.truth_fibermap['TARGET_DEC'][i]
            jj = i
            z_qso = self.truth_zqso[jj]
            assert (z_qso > 2)

            # cut out forest, but do not remove masked pixels individually
            # resolution matrix assumes all pixels to be present
            forest_pixels = getForestAnalysisRegion(
                self.truth_wave, z_qso, self.args)
            remaining_pixels = forest_pixels

            if np.sum(remaining_pixels) < 15:
                # Empty spectrum
                continue

            wave = self.truth_wave[forest_pixels]
            dlambda = np.mean(np.diff(wave))

            # Skip short chunks
            MAX_NO_PIXELS = int(
                (fid.LYA_LAST_WVL - fid.LYA_FIRST_WVL) * (1 + z_qso) / dlambda
            )
            if self.args.skip and (np.sum(remaining_pixels) < MAX_NO_PIXELS * self.args.skip):
                # Short chunk
                continue

            # cont_interp = interp1d(self.truth_wave, self.truth_flux[jj])
            # cont = cont_interp(wave)
            cont = np.interp(wave, self.truth_wave, self.truth_flux[jj])
            z = wave / fid.LYA_WAVELENGTH - 1

            flux = self.influx[i][forest_pixels] / cont
            # ivar = coadd_data['ivar'][i][forest_pixels] * cont**2
            # rmat = coadd_data['reso'][i][:, forest_pixels]
            # mask = coadd_mask[i][forest_pixels] - buggy
            # Cut rmat forest region, but keep individual bad pixel values in
            ivar = 1e4 * cont**2
            rmat = np.ones((1, flux.size))
            # np.delete(coadd_data['reso'][i], ~forest_pixels, axis=1)

            # Make it delta
            tr_mf = TRUE_MEAN_FLUX(z)
            delta = flux / tr_mf - 1
            ivar = ivar * tr_mf**2

            # Mask by setting things to 0
            # delta[mask] = 0
            # ivar[mask]  = 0

            # Save it
            saveDelta(
                thid, wave, delta, ivar, cont, tr_mf,
                z_qso, ra, dec, rmat, delta_hdu)

        delta_hdu.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--simspec-file", help="Simspec file.", required=True)
    parser.add_argument("--outputdir", '-o', help="Output directory", required=True)

    parser.add_argument(
        "--desi-w1", type=float, default=3600.,
        help="Lower wavelength of DESI wave grid in A. Avoid boundary. Default: %(default)s A")
    parser.add_argument(
        "--desi-w2", type=float, default=9800.,
        help="Higher wavelength of DESI wave grid in A. Avoid boundary. Default: %(default)s A")

    parser.add_argument(
        "--z-forest-min", help="Lower end of the forest. Default: %(default)s",
        type=float, default=2.1)
    parser.add_argument(
        "--z-forest-max", help="Upper end of the forest. Default: %(default)s",
        type=float, default=3.5)
    parser.add_argument("--skip", help="Skip short chunks lower than given ratio", type=float)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    os_makedirs(args.outputdir, exist_ok=True)
    reducer = Reducer(args)

    reducer(args.simspec_wforest)
