import argparse
import json

import numpy as np

from lym1d import lym1d

path_nersc = '/global/cfs/cdirs/desi/science/lya/y1-p1d/likelihood_files/'
models_path = 'nyx_files/models_Nyx_Oct2023.hdf5'
emupath = 'nyx_files/lym1d_full_emulator_Oct2023.npz'
runmode, Anmode = 'nyx', 'default'

zmin, zmax = 2.2, 4.6
zlist_thermo = [
    2.2, 2.4, 2.6, 2.8, 3.0, 3.2, 3.4, 3.6, 3.8, 4.0, 4.2, 4.4, 4.6]
# Only IC matters
hascor = {
    'IC': True, 'noise': False,
    'DLA': False, 'reso': False,
    'SN': False, 'AGN': False,
    'zreio': False, 'SiIII': False,
    'SiII': False, 'norm': False
}

# Do not matter
data_path = 'data_files/Chabanier19'
datafile = 'pk_1d_DR12_13bins.out'
invcovfile = 'pk_1d_DR12_13bins_invCov.out'


def parse(options=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("InputParams", help="Input json file")
    parser.add_argument("InputQmleFile", help="Input qmle file")
    parser.add_argument("SaveDirectory", help="Directory for to save.")
    args = parser.parse_args(options)

    return args


def getHubble(z, cosmo):
    h = cosmo['H_0'] / 100
    Omega_m = cosmo['omega_m'] / h**2
    return cosmo['H_0'] * np.sqrt(Omega_m * (1 + z)**3 + 1 - Omega_m)


def correctICnyz(z, k_skm):
    ic_corr_z = np.array([0.15261529, -2.30600644, 2.61877894])
    ic_corr_k = 0.003669741766936781  # s/km
    ancorIC = (
        ic_corr_z[0] * z**2 + ic_corr_z[1] * z + ic_corr_z[2]
    ) * (1 - np.exp(-k_skm / ic_corr_k))
    corICs = (1 - ancorIC / 100)
    return corICs


def main():
    args = parse()

    lym1d_obj = lym1d(
        base_directory=path_nersc,
        models_path=models_path,
        emupath=emupath,
        data_path=data_path,
        runmode=runmode, has_cor=hascor, zmin=zmin, zmax=zmax,
        zs=zlist_thermo, data_filename=datafile,
        inversecov_filename=invcovfile,
    )

    with open(args.InputParams) as f:
        fiducial_model_lym1d = json.loads(f.read())

    qmle_p1d = np.genfromtxt(args.InputQmleFile)[1:]

    p1d = {}
    kbins = np.unique(qmle_p1d[:, 3])
    for z, params in fiducial_model_lym1d.items():
        zf = float(z)
        if zf < 2.1:
            continue

        Hz = getHubble(zf, params) / (1 + zf)
        p1dnow = lym1d_obj.emu(params, zf, kbins * Hz)[0] * Hz

        if hascor['IC']:
            p1dnow /= correctICnyz(zf, kbins)

        p1d[z] = list(p1dnow)

    suffix = "_IC" if hascor['IC'] else ""
    with open(
            f"{args.SaveDirectory}/fiducial_lym1d_p1d{suffix}.txt", 'w'
    ) as f:
        f.write(json.dumps(p1d))
