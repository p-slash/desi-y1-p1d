import argparse
import logging
from multiprocessing import Pool

import numpy as np
from tqdm import tqdm

from qsotools.specops import MeanFluxHist
from qsotools.io import Spectrum, PiccaFile, ConfigQMLE
import qsotools.fiducial as fid
import qsotools.utils as qutil

ZQSO_SHIFT = 0.2


def _readSpectrum(hdu):
    hdr = hdu.read_header()
    data = hdu.read()

    colnames = hdu.get_colnames()

    if "LAMBDA" in colnames:
        wave = data['LAMBDA']
    else:
        wave = 10**data['LOGLAM']

    delta_keys = set(['DELTA', 'DELTA_BLIND'])
    delta = data[delta_keys.intersection(colnames).pop()]
    error = 1 / np.sqrt(data['IVAR'] + 1e-16)
    error[data['IVAR'] < 1e-4] = 0
    if "WEIGHT" in data.dtype.names:
        weight = data['WEIGHT']
    else:
        weight = data['IVAR']

    specres = fid.LIGHT_SPEED / hdr['MEANRESO'] / fid.ONE_SIGMA_2_FWHM
    if 'DLL' in hdr.keys():
        dv = hdr['DLL'] * fid.LIGHT_SPEED * np.log(10)
    else:
        dv = hdr['MEANRESO']
    reso_rkms = None
    qso = Spectrum(
        wave, delta, error, hdr['Z'], specres, dv,
        {'RA': hdr['RA'], 'DEC': hdr['DEC']}, reso_rkms)

    return qso, weight, hdr['MEANSNR']


class CalculateStats():
    def _find_zqso_indx(self, z_qso):
        idx = np.searchsorted(
            self.local_meanflux_hist.hist_redshift_edges + ZQSO_SHIFT, z_qso)
        self.local_zqso_hist[idx] += 1

    def _add_snr_hist(self, snr):
        idx = min(int(snr / self.dsnr), self.local_meansnr_hist.size - 1)
        self.local_meansnr_hist[idx] += 1

    def __init__(self, args, config_qmle):
        self.config_qmle = config_qmle
        self.local_meanflux_hist = MeanFluxHist(args.z1, args.z2, args.dz)
        self.local_zqso_hist = np.zeros(self.local_meanflux_hist.nz + 2)
        self.local_meansnr_hist = np.zeros(args.nsnr)
        self.dsnr = args.dsnr
        self.fdelta = None

    def __call__(self, fnames):
        base, hdus = fnames
        f = f"{self.config_qmle.qso_dir}/{base}"
        pfile = PiccaFile(f, 'r')
        for hdu in hdus:
            qso, weight, meansnr = _readSpectrum(pfile.fitsfile[hdu])
            self.local_meanflux_hist.addSpectrum(
                qso, weight, f1=0, f2=9000, compute_scatter=True)
            self._find_zqso_indx(qso.z_qso)
            self._add_snr_hist(meansnr)

        return (self.local_meanflux_hist, self.local_zqso_hist,
                self.local_meansnr_hist)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("ConfigFile", help="Config file for analysis.")
    parser.add_argument(
        "--z1", help="Lower redshift edge", default=2.0, type=float)
    parser.add_argument(
        "--z2", help="Upper redshift edge", default=4.7, type=float)
    parser.add_argument(
        "--dz", help="Z bin size", default=0.05, type=float)
    parser.add_argument(
        "--nsnr", help="number of snr bins", default=100, type=int)
    parser.add_argument("--dsnr", help="snr bin size", default=0.1, type=float)
    parser.add_argument("--fbase", help="Basename", default="histogram")
    parser.add_argument("--nproc", type=int, default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.DEBUG)

    config_qmle = ConfigQMLE(args.ConfigFile)
    output_dir = config_qmle.parameters['OutputDir']
    output_base = config_qmle.parameters['OutputFileBase']
    output_base = f"{output_dir}/{args.fbase}-{output_base}"

    # Read file list file
    fnames_spectra = config_qmle.readFnameSpectra()

    # If files are in Picca format, decompose filename list into
    # Main file & hdus to read in that main file
    if not config_qmle.picca_input:
        logging.error("File input is not picca!")
        exit(1)

    logging.info("Decomposing filenames to a list of (base, list(hdus)).")
    fnames_spectra = qutil.getPiccaFList(fnames_spectra)

    nfiles = len(fnames_spectra)

    meanflux_hist = MeanFluxHist(args.z1, args.z2, args.dz)
    zqso_hist = np.zeros(meanflux_hist.nz + 2)
    meansnr_hist = np.zeros(args.nsnr)

    with Pool(processes=args.nproc) as pool:
        imap_it = pool.imap(CalculateStats(args, config_qmle), fnames_spectra)
        for mfh, zqh, msnrh in tqdm(imap_it, total=nfiles):
            meanflux_hist += mfh
            zqso_hist += zqh
            meansnr_hist += msnrh

    meanflux_hist.getMeanStatistics(compute_scatter=True)
    meanflux_hist.saveHistograms(output_base)

    np.savetxt(
        f"{output_base}-zqso-hist.csv",
        (meanflux_hist.hist_redshifts + ZQSO_SHIFT, zqso_hist[1:-1]))

    snrcenters = np.arange(args.nsnr) * args.dsnr + (args.dsnr / 2)
    np.savetxt(
        f"{output_base}-forest-meansnr-hist.csv", (snrcenters, meansnr_hist))
