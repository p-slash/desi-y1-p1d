import argparse
import logging
from multiprocessing import Pool
from os import makedirs as os_makedirs

import numpy as np
import fitsio

import qsotools.fiducial as fid
from qsotools.mocklib import lognMeanFluxGH as TRUE_MEAN_FLUX
from qsotools.specops import fitGaussian2RMat


def createEdgesFromCenters(wave_centers):
    npix = len(wave_centers)
    dlm = np.min(wave_centers[1:] - wave_centers[:-1])
    wave_edges = (wave_centers[0] - dlm / 2) + np.arange(npix + 1) * dlm

    return wave_edges


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

#     lya_ind = np.logical_and(wave >= fid.LYA_FIRST_WVL * (1+z_qso), \
#         wave <= fid.LYA_LAST_WVL * (1+z_qso))

#     w1 = max(fid.LYA_WAVELENGTH*(1+args.z_forest_min), args.desi_w1)
#     w2 = min(fid.LYA_WAVELENGTH*(1+args.z_forest_max), args.desi_w2)
#     forst_bnd = np.logical_and(wave >= w1, wave <= w2)
#     lya_ind = np.logical_and(lya_ind, forst_bnd)

    return lya_ind


def saveDelta(
        thid, wave, delta, ivar, cont, meanf, z_qso, ra, dec, rmat, fdelta
):
    ndiags = rmat.shape[0]

    data = np.zeros(
        wave.size, dtype=[('LAMBDA', 'f8'), ('DELTA', 'f8'), ('IVAR', 'f8'),
                          ('CONT', 'f8'), ('MEANF', 'f8'),
                          ('RESOMAT', 'f8', ndiags)]
    )

    data['LAMBDA'] = wave
    data['DELTA'] = delta
    data['IVAR'] = ivar
    data['CONT'] = cont
    data['MEANF'] = meanf
    if ndiags > 1:
        data['RESOMAT'] = rmat.T
        R_kms = fitGaussian2RMat(thid, wave, rmat)
    else:
        data['RESOMAT'] = rmat[0]
        R_kms = 0.1

    hdr_dict = {
        'TARGETID': thid, 'RA': np.radians(ra), 'DEC': np.radians(dec),
        'Z': float(z_qso), 'MEANZ': np.mean(wave) / fid.LYA_WAVELENGTH - 1,
        'MEANRESO': R_kms, 'MEANSNR': np.mean((1 + delta) * np.sqrt(ivar)),
        'LIN_BIN': True, 'DLAMBDA': np.median(np.diff(wave))
    }

    fdelta.write(data, header=hdr_dict)


def read_coadd_into_dict(cfile):
    coadd_hdu = fitsio.FITS(cfile)

    data = {}
    data['fibermap'] = coadd_hdu['FIBERMAP'].read()
    data['qso_idx'] = np.where(data['fibermap']['OBJTYPE'] == 'QSO')[0]

    logging.info("Using only blue (B) arm.")
    data['wave'] = coadd_hdu['B_WAVELENGTH'].read()
    data['flux'] = coadd_hdu['B_FLUX'].read()
    data['ivar'] = coadd_hdu['B_IVAR'].read()
    data['mask'] = coadd_hdu['B_MASK'].read()
    data['reso'] = coadd_hdu['B_RESOLUTION'].read()
    coadd_hdu.close()

    nuniq = np.unique(data['fibermap'][data['qso_idx']]['TARGETID']).size
    logging.info(f"Number of QSO in simulated coadd {data['qso_idx'].size}")
    logging.info(f"Unique targetid in simulated coadd {nuniq}")

    return data


class Reducer():
    def __init__(self, args):
        self.args = args
        simspec_hdu = fitsio.FITS(args.simspec_file)
        self.truth_fibermap = simspec_hdu['FIBERMAP'].read()
        self.truth_qso_idx = np.where(
            self.truth_fibermap['OBJTYPE'] == 'QSO')[0]

        targetids = self.truth_fibermap[self.truth_qso_idx]['TARGETID']
        logging.info(f"Number of QSO in truth {self.truth_qso_idx.size}")
        logging.info(
            f"Unique targetid in truth {np.unique(targetids).size}")

        self.truth_wave = simspec_hdu['WAVE'].read()
        self.truth_flux = simspec_hdu['FLUX_TRUE'].read()
        self.truth_zqso = simspec_hdu['TRUTH']['REDSHIFT'].read()
        simspec_hdu.close()

    def _isShort(self, z_qso, dlambda, remaining_pixels):
        MAX_NO_PIXELS = int(
            (fid.LYA_LAST_WVL - fid.LYA_FIRST_WVL) * (1 + z_qso) / dlambda)
        return (np.sum(remaining_pixels) < MAX_NO_PIXELS * self.args.skip)

    def __call__(self, cfile):
        print(f"Reading {cfile}")
        coadd_data = read_coadd_into_dict(cfile)
        suffix = cfile.split("/")[-1]
        output_delta_fname = f"{self.args.outputdir}/delta-{suffix[6:]}"

        delta_hdu = fitsio.FITS(output_delta_fname, "rw", clobber=True)
        logging.info("Spectra are read.")
        logging.info(f"There are {coadd_data['qso_idx'].size} quasars.")

        for i in coadd_data['qso_idx']:
            thid = coadd_data['fibermap']['TARGETID'][i]
            ra = coadd_data['fibermap']['TARGET_RA'][i]
            dec = coadd_data['fibermap']['TARGET_DEC'][i]
            jj = np.nonzero(self.truth_fibermap['TARGETID'] == thid)[0][0]
            z_qso = self.truth_zqso[jj]
            assert (z_qso > 2)

            # cut out forest, but do not remove masked pixels individually
            # resolution matrix assumes all pixels to be present
            forest_pixels = getForestAnalysisRegion(
                coadd_data['wave'], z_qso, self.args)
            remaining_pixels = forest_pixels & ~coadd_data['mask'][i]

            if np.sum(remaining_pixels) < 15:
                # Empty spectrum
                continue

            wave = coadd_data['wave'][forest_pixels]
            # Skip short chunks
            isshort = self._isShort(z_qso, wave[1] - wave[0], remaining_pixels)
            if self.args.skip and isshort:
                # Short chunk
                continue

            # cont_interp = interp1d(self.truth_wave, self.truth_flux[jj])
            # cont = cont_interp(wave)

            z = wave / fid.LYA_WAVELENGTH - 1
            cont = np.interp(wave, self.truth_wave, self.truth_flux[jj])
            w = cont <= 0

            flux = coadd_data['flux'][i][forest_pixels] / cont
            ivar = coadd_data['ivar'][i][forest_pixels] * cont**2
            # mask = coadd_mask[i][forest_pixels] - buggy
            # Cut rmat forest region, but keep individual bad pixel values in
            rmat = np.delete(coadd_data['reso'][i], ~forest_pixels, axis=1)

            # Make it delta
            tr_mf = TRUE_MEAN_FLUX(z)
            delta = flux / tr_mf - 1
            ivar = ivar * tr_mf**2
            delta[w] = 0
            ivar[w] = 0

            # Mask by setting things to 0
            # delta[mask] = 0
            # ivar[mask]  = 0

            # Save it
            saveDelta(thid, wave, delta, ivar, cont, tr_mf,
                      z_qso, ra, dec, rmat, delta_hdu)

        delta_hdu.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--coadd-file", '-cfile', help="Coadd input file.", nargs='+')
    parser.add_argument(
        "--simspec-file", '-sfile', help="Simspec (no forest) input file.",
        required=True)
    parser.add_argument(
        "--outputdir", '-o', help="Output directory", required=True)
    # parser.add_argument("--expid", required=True, \
    #     help="Expid. Reads simspec-expid and coadd-expid.")
    parser.add_argument(
        "--desi-w1", type=float, default=3600.0,
        help="Lower wavelength of DESI wave grid in A. Avoid boundary.")
    parser.add_argument(
        "--desi-w2", type=float, default=9800.0,
        help="Higher wavelength of DESI wave grid in A. Avoid boundary.")
    parser.add_argument(
        "--z-forest-min", help="Lower end of the forest. Default: %(default)s",
        type=float, default=2.1)
    parser.add_argument(
        "--z-forest-max", help="Upper end of the forest. Default: %(default)s",
        type=float, default=3.5)
    parser.add_argument(
        "--nproc", help="number of cores available", default=32, type=int)
    parser.add_argument(
        "--skip", help="Skip short chunks lower than given ratio", type=float)
    args = parser.parse_args()

    logging.basicConfig(level=logging.DEBUG)

    os_makedirs(args.outputdir, exist_ok=True)

    nproc = min(len(args.coadd_file), args.nproc)
    with Pool(processes=nproc) as pool:
        pool.map(Reducer(args), args.coadd_file)

    logging.info("Done")
