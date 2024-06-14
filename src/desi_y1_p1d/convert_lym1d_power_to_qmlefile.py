import argparse
import json

import astropy.io.ascii
import numpy as np


def invertFisher(fisher):
    newf = fisher.copy()
    di = np.diag_indices(fisher.shape[0])
    w = newf[di] == 0
    newf[di] = np.where(w, 1, newf[di])
    newf = np.linalg.inv(newf)
    newf[di] = np.where(w, 0, newf[di])
    return newf


def parse(options=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("InputParams", help="Input json file")
    parser.add_argument("InputQmleFile", help="Input qmle file")
    parser.add_argument("InputQmleInvCov", help="Input qmle invcov file")
    parser.add_argument("SaveDirectory", help="Directory for to save.")
    parser.add_argument("--zmin", help="Z min", type=float, default=2.19)
    parser.add_argument("--kmin", help="k min", type=float, default=1e-3)
    parser.add_argument(
        "--alpha_knyq", help="keep k < alpha * knyq", type=float, default=0.5
    )
    args = parser.parse_args(options)

    return args


def main():
    args = parse()
    with open(args.InputParams) as f:
        lym1d_p1d = json.loads(f.read())

    qmle_p1d = astropy.io.ascii.read(args.InputQmleFile)
    qmle_invcov = np.loadtxt(args.InputQmleInvCov)
    qmle_cov = invertFisher(qmle_invcov)

    for z, p in lym1d_p1d.items():
        p = np.array(p)
        zf = float(z)
        w = np.isclose(qmle_p1d['z'], zf)
        theta = p - qmle_p1d['Pfid'][w]
        qmle_p1d['ThetaP'][w] = theta
        qmle_p1d['Pest'][w] = p

    k_nyq = np.pi / (3e5 * 0.8 / 1215.67 / (1 + qmle_p1d['z']))
    kmax = args.alpha_knyq * k_nyq
    w = ((qmle_p1d['z'] > args.zmin)
         & (qmle_p1d['kc'] > args.kmin)
         & (qmle_p1d['kc'] < kmax)
         )
    qmle_p1d = qmle_p1d[w]
    qmle_cov = qmle_cov[w, :][:, w]

    formats = {}
    for key in qmle_p1d.keys():
        formats[key] = '%14e'
    formats['z'] = '%5.3f'

    astropy.io.ascii.write(
        qmle_p1d,
        f"{args.SaveDirectory}/fiducial_lym1d_p1d_qmleformat_IC.txt",
        format='fixed_width', delimiter=' ',
        formats=formats, overwrite=True)
    np.savetxt(f"{args.SaveDirectory}/fiducial_cov_qmleformat.txt", qmle_cov)
