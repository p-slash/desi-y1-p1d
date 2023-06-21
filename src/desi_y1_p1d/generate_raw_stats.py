import argparse
import struct

import numpy as np
import fitsio
from scipy.interpolate import RegularGridInterpolator

import qsotools.fiducial as fid
from qsotools.mocklib import lognMeanFluxGH as TRUE_MEAN_FLUX


def readTrueP1D(fname):
    print("I am reading true power.", flush=True)
    file = open(fname, 'rb')

    nk, nz = struct.unpack('ii', file.read(struct.calcsize('ii')))

    fmt = 'd' * nz
    data = file.read(struct.calcsize(fmt))
    z = np.array(struct.unpack(fmt, data), dtype=np.double)

    fmt = 'd' * nk
    data = file.read(struct.calcsize(fmt))
    k = np.array(struct.unpack(fmt, data), dtype=np.double)

    fmt = 'd' * nk * nz
    data = file.read(struct.calcsize(fmt))
    p1d = np.array(struct.unpack(fmt, data), dtype=np.double).reshape((nz, nk))

    return RegularGridInterpolator((z, k), p1d)


def get_true_var_lss(
        z, dv, truepower_interp2d,
        lnk1=-4 * np.log(10), lnk2=-0.5 * np.log(10), dlnk=0.01
):
    print("I am calculation var_lss.", flush=True)
    R_kms = dv

    Nkpoints = int((lnk2 - lnk1) / dlnk) + 1
    k = np.exp(np.linspace(lnk1, lnk2, Nkpoints))[:, np.newaxis]

    window_fn_2 = np.sinc(k * dv / 2 / np.pi)**2 * np.exp(-k**2 * R_kms**2)
    print(window_fn_2.shape)

    var_lss = np.empty(z.size)
    tp2 = truepower_interp2d((z, k))
    tp2 *= k * window_fn_2 / np.pi
    print(tp2.shape, z.shape)
    var_lss = np.trapz(tp2, dx=dlnk, axis=0)

    return var_lss


def make_up_raw_file(
        fname_out, fname_truepower, w1, w2, rfw1, rfw2, dlambda, dloglam=3e-4
):
    num_bins = int((w2 - w1) / dlambda) + 1
    wave = np.linspace(w1, w2, num_bins)
    z = wave / fid.LYA_WAVELENGTH - 1
    true_mean = TRUE_MEAN_FLUX(z)
    truepower_interp2d = readTrueP1D(fname_truepower)
    var_lss = get_true_var_lss(
        z, fid.LIGHT_SPEED * dlambda / wave, truepower_interp2d)
    flux_variance = var_lss * true_mean**2
    # Unused in p1d analysis
    stack_weight = np.ones_like(wave)
    var_weights = stack_weight

    results = fitsio.FITS(fname_out, 'rw', clobber=True)
    cols = [wave, true_mean, stack_weight, flux_variance, var_weights, var_lss]
    names = ['LAMBDA', 'MEANFLUX', 'WEIGHTS', 'VAR', 'VARWEIGHTS', 'VAR_LSS']
    header = {}
    header['L_MIN'] = w1
    header['L_MAX'] = w2
    header['LR_MIN'] = rfw1
    header['LR_MAX'] = rfw2
    header['DEL_LL'] = dloglam
    header['DEL_L'] = dlambda
    header['LINEAR'] = True
    results.write(cols, names=names, header=header, extname='STATS')
    results.close()


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument(
        "--fname-true-power", help="True power file (required)", required=True,
        default=("/global/cfs/cdirs/desicollab/users/"
                 "naimgk/desilite-mocks/true-power-spectrum.bin")
    )
    parser.add_argument(
        "--out-fname-base", help="Output filename",
        default="ohio-p1d-true-stats")
    parser.add_argument(
        '--lambda-min', type=float, default=3600., required=False,
        help='Lower limit on observed wavelength [Angstrom]')
    parser.add_argument(
        '--lambda-max', type=float, default=6600., required=False,
        help='Upper limit on observed wavelength [Angstrom]')
    parser.add_argument(
        '--lambda-rest-min', type=float, default=1050., required=False,
        help='Lower limit on rest frame wavelength [Angstrom]')
    parser.add_argument(
        '--lambda-rest-max', type=float, default=1180., required=False,
        help='Upper limit on rest frame wavelength [Angstrom]')
    parser.add_argument(
        '--delta-lambda', type=float, default=0.8, required=False,
        help='Size of the rebined pixels in lambda')
    args = parser.parse_args()

    fname_out = (f"{args.out_fname_base}-obs{args.lambda_min:.0f}"
                 f"-{args.lambda_max:.0f}-rf{args.lambda_rest_min:.0f}"
                 f"-{args.lambda_rest_max:.0f}-dw{args.delta_lambda:.1f}.fits")

    make_up_raw_file(fname_out, args.fname_true_power,
                     args.lambda_min, args.lambda_max,
                     args.lambda_rest_min, args.lambda_rest_max,
                     args.delta_lambda)
