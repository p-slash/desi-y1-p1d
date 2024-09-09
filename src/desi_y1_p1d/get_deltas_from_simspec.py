import argparse
import logging
from os import makedirs as os_makedirs

import fitsio
import numpy as np
from scipy.interpolate import interp1d

import qsotools.fiducial as fid
from qsotools.mocklib import lognMeanFluxGH as TRUE_MEAN_FLUX


def getForestAnalysisRegion(wave, z_qso, args):
    lya_ind = np.zeros(wave.size, dtype=bool)

    w1 = max(
        fid.LYA_WAVELENGTH * (1 + args.z_forest_min),
        args.desi_w1,
        fid.LYA_FIRST_WVL * (1 + z_qso))
    w2 = min(
        fid.LYA_WAVELENGTH * (1 + args.z_forest_max),
        args.desi_w2,
        fid.LYA_LAST_WVL * (1 + z_qso))
    i1, i2 = np.searchsorted(wave, [w1, w2])

    lya_ind[i1:i2] = True

    return lya_ind


def saveDelta(
        thid, wave, delta, ivar, cont, meanf,
        z_qso, ra, dec, rmat, fdelta
):
    data = np.zeros(
        wave.size,
        dtype=[('LAMBDA', 'f8'), ('DELTA', 'f8'), ('IVAR', 'f8'),
               ('CONT', 'f8'), ('MEANF', 'f8'),
               ('RESOMAT', 'f8')])

    data['LAMBDA'] = wave
    data['DELTA'] = delta
    data['IVAR'] = ivar
    data['CONT'] = cont
    data['MEANF'] = meanf
    data['RESOMAT'] = rmat
    R_kms = 0.1

    hdr_dict = {
        'TARGETID': thid, 'RA': np.radians(ra), 'DEC': np.radians(dec),
        'Z': float(z_qso), 'MEANZ': np.mean(wave) / fid.LYA_WAVELENGTH - 1,
        'MEANRESO': R_kms, 'MEANSNR': np.mean(np.sqrt(data['IVAR'])),
        'LIN_BIN': True, 'DLAMBDA': np.median(np.diff(wave))
    }

    fdelta.write(data, header=hdr_dict)


def read_coadd_into_dict(cfile):
    coadd_hdu = fitsio.FITS(cfile)

    data = {}
    data['truth'] = coadd_hdu['TRUTH'].read()
    data['fibermap'] = coadd_hdu['FIBERMAP'].read()
    data['qso_idx'] = np.where(data['truth']['OBJTYPE'] == 'QSO')[0]

    logging.info("Using only blue (B) arm.")
    data['wave'] = coadd_hdu['WAVE'].read()
    data['flux'] = coadd_hdu['FLUX'].read()
    coadd_hdu.close()

    targetids = data['fibermap'][data['qso_idx']]['TARGETID']

    logging.info(f"Number of QSO in simulated coadd {data['qso_idx'].size}")
    logging.info(
        f"Unique targetid in simulated coadd {np.unique(targetids).size}")

    return data


class Reducer():
    def __init__(self, args):
        self.args = args
        simspec_hdu = fitsio.FITS(args.simspec_truth)
        self.truth_fibermap = simspec_hdu['FIBERMAP'].read()
        self.truth_qso_idx = np.where(
            self.truth_fibermap['OBJTYPE'] == 'QSO')[0]

        targetids = self.truth_fibermap[self.truth_qso_idx]['TARGETID']

        logging.info(f"Number of QSO in truth {self.truth_qso_idx.size}")
        logging.info(f"Unique targetid in truth {np.unique(targetids).size}")

        self.truth_wave = simspec_hdu['WAVE'].read()
        self.truth_flux = simspec_hdu['FLUX_TRUE'].read()
        self.truth_zqso = simspec_hdu['TRUTH']['REDSHIFT'].read()
        simspec_hdu.close()

    def __call__(self, cfile):
        logging.info(f"Reading {cfile}")
        coadd_data = read_coadd_into_dict(cfile)
        suffix = cfile.split("/")[-1]
        _len = len("simspec-")
        output_delta_fname = f"{self.args.outputdir}/delta-{suffix[_len:]}"

        delta_hdu = fitsio.FITS(output_delta_fname, "rw", clobber=True)
        logging.info("Spectra are read.")
        logging.info(f"There are {coadd_data['qso_idx'].size} quasars.")

        assert np.all(coadd_data['fibermap']['TARGETID'] == self.truth_fibermap['TARGETID'])

        for i in coadd_data['qso_idx']:
            thid = coadd_data['fibermap']['TARGETID'][i]
            ra = coadd_data['fibermap']['TARGET_RA'][i]
            dec = coadd_data['fibermap']['TARGET_DEC'][i]
            jj = i
            # jj = np.nonzero(self.truth_fibermap['TARGETID'] == thid)[0]
            z_qso = self.truth_zqso[jj]
            assert (z_qso > 2)

            # cut out forest, but do not remove masked pixels individually
            # resolution matrix assumes all pixels to be present
            forest_pixels = getForestAnalysisRegion(
                coadd_data['wave'], z_qso, self.args)
            remaining_pixels = forest_pixels

            if np.sum(remaining_pixels) < 15:
                # Empty spectrum
                continue

            wave = coadd_data['wave'][forest_pixels]
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

            flux = coadd_data['flux'][i][forest_pixels] / cont
            ivar = coadd_data['ivar'][i][forest_pixels] * cont**2
            rmat = coadd_data['reso'][i][:, forest_pixels]
            # mask = coadd_mask[i][forest_pixels] - buggy
            # Cut rmat forest region, but keep individual bad pixel values in
            # ivar = 1e4 * cont**2
            # rmat = np.ones_like(flux.size)
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
                z_qso, ra, dec, rmat,
                delta_hdu)

        delta_hdu.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--simspec-truth", help="Simspec truth file.", required=True)
    parser.add_argument("--simspec-wforest", help="Simspec with forest file.", required=True)
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
