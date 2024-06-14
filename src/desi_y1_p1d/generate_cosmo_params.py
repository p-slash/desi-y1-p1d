import argparse
import json

import camb
import numpy as np

from astropy.cosmology import Planck18

base_cosmo = {
    'omega_b': Planck18.Ob0 * Planck18.h**2,
    'omega_cdm': Planck18.Odm0 * Planck18.h**2,
    'h': Planck18.h,
    'n_s': Planck18.meta['n'],
    'ln10^{10}A_s': 3.044,
    'log10T': 4.,
    'k_p': 12.5,  # Mpc1
    'gamma': 1.4
}

planck_prior = {
    'omega_b': 0.00014,
    'omega_cdm': 0.00091,
    'h': 0.0042,
    'n_s': 0.0038,
    'ln10^{10}A_s': 0.014,
    'log10T': 0.05,
    'k_p': 1.5,
    'gamma': 0.05
}


def parse(options=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("SEED", help="Seed", type=int)
    parser.add_argument("SaveDirectory", help="Directory for to save.")
    args = parser.parse_args(options)

    return args


def getCambLinearPowerInterp(zlist, cosmo):
    camb_params = camb.set_params(
        redshifts=sorted(zlist, reverse=True),
        WantCls=False, WantScalars=False,
        WantTensors=False, WantVectors=False,
        WantDerivedParameters=False,
        WantTransfer=True,
        omch2=cosmo['omega_cdm'],
        ombh2=cosmo['omega_b'],
        omk=0.,
        H0=cosmo['h'] * 100,
        ns=cosmo['n_s'],
        As=np.exp(cosmo['ln10^{10}A_s']) * 1e-10,
        mnu=0.
    )

    camb_results = camb.get_results(camb_params)
    # Note this interpolator in Mpc units without h
    camb_interp = camb_results.get_matter_power_interpolator(
        nonlinear=False, hubble_units=False,
        k_hunit=False, extrap_kmax=5.)
    return camb_interp.P


def getDeltapNp(z, plin_interp, kpivot=0.7):
    Deltap = kpivot**3 * plin_interp(z, kpivot) / (2 * np.pi**2)
    logkp = np.log(kpivot)
    dlogk = 0.001
    k = np.exp(logkp + np.array([-2., -1., 1., 2.]) * dlogk)
    weights = np.array([1., -8., 8., -1.]) / (12 * dlogk)

    logP = np.log(plin_interp(z, k))
    Np = logP.dot(weights)

    return Deltap, Np


def turner24_mf(z):
    return np.exp(-2.46e-3 * (1 + z)**3.62)


def getHubble(z, cosmo):
    h = cosmo['h']
    Omega_m = cosmo['omega_cdm'] + cosmo['omega_b']
    Omega_m /= h**2
    return 100 * cosmo['h'] * np.sqrt(Omega_m * (1 + z)**3 + 1 - Omega_m)


def getCup1dModelDict(z, cosmo, plin_interp):
    Deltap, Np = getDeltapNp(z, plin_interp)
    sigma_T = \
        9.1 * 10**(cosmo['log10T'] / 2 - 2) * (1 + z) / getHubble(z, cosmo)
    model = {
        'Delta2_p': Deltap, 'n_p': Np,
        'mF': turner24_mf(z), 'sigma_T': sigma_T,
        'gamma': cosmo['gamma'], 'kF_Mpc': cosmo['k_p']
    }
    return model


def getLym1dModelDict(z, cosmo, plin_interp):
    Deltap, n_lya = getDeltapNp(3.0, plin_interp, kpivot=1.0)
    A_lya = Deltap * 2 * np.pi**2
    model = {
        'A_lya': A_lya, 'n_lya': n_lya,
        'Fbar': turner24_mf(z), 'T_0': 10**cosmo['log10T'],
        'gamma': cosmo['gamma'], 'lambda_P': 1000. / cosmo['k_p'],
        'H_0': cosmo['h'] * 100,
        'omega_m': cosmo['omega_cdm'] + cosmo['omega_b']
    }
    return model


def main():
    args = parse()
    new_cosmo = base_cosmo.copy()
    rng = np.random.default_rng(args.SEED)

    for key, std in planck_prior.items():
        new_cosmo[key] += rng.normal(scale=std)
        print(
            key, base_cosmo[key], new_cosmo[key],
            "delta sigma", (new_cosmo[key] - base_cosmo[key]) / std
        )

    zlist = np.arange(13) * 0.2 + 2.0
    plin_interp = getCambLinearPowerInterp(zlist, new_cosmo)

    fiducial_model_cup1d = {}
    fiducial_model_lym1d = {}
    for z in zlist:
        fiducial_model_cup1d[z] = getCup1dModelDict(z, new_cosmo, plin_interp)
        fiducial_model_lym1d[z] = getLym1dModelDict(z, new_cosmo, plin_interp)

    with open(f"{args.SaveDirectory}/fiducial_lym1d_params.txt", 'w') as f:
        f.write(json.dumps(fiducial_model_lym1d))

    with open(f"{args.SaveDirectory}/fiducial_cup1d_params.txt", 'w') as f:
        f.write(json.dumps(fiducial_model_cup1d))
