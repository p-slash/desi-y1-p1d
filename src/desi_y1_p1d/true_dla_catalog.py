import argparse
import glob
from multiprocessing import Pool

import numpy as np
import fitsio

final_dtype = np.dtype([
    ('NHI', 'f8'), ('Z', 'f8'), ('TARGETID', 'i8'), ('DLAID', 'i8')
])


def parse(options=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("Directory", help="Directory for spectra-16.")
    parser.add_argument(
        "SaveDirectory", help="Directory for to save final catalog.")
    parser.add_argument("--nproc", type=int, default=None)
    args = parser.parse_args(options)

    return args


def _getDLACat(ftruth):
    f1 = fitsio.FITS(ftruth)

    hdr_dla = f1['DLA_META'].read_header()

    nrows = hdr_dla['NAXIS']
    if nrows == 0:
        f1.close()
        return None

    dat_dla = f1['DLA_META'].read()
    nrows = len(dat_dla)
    f1.close()

    newdata = np.empty(nrows, dtype=final_dtype)
    newdata['NHI'] = dat_dla['NHI']
    newdata['Z'] = dat_dla['Z_DLA']
    newdata['TARGETID'] = dat_dla['TARGETID']
    newdata['DLAID'] = dat_dla['DLAID']

    return newdata


def main(options=None):
    args = parse(options)

    all_truths = glob.glob(f"{args.Directory}/*/*/truth-*.fits*")

    numpy_arrs = []

    print("Iterating over files.")
    with Pool(processes=args.nproc) as pool:
        imap_it = pool.imap(_getDLACat, all_truths)

        for arr in imap_it:
            if arr is None:
                continue

            numpy_arrs.append(arr)

    final_data = np.concatenate(numpy_arrs)
    ndlas = final_data.size
    print(f"There are {ndlas} DLAs.")

    fname = f"{args.SaveDirectory}/dla_cat.fits"

    with fitsio.FITS(fname, 'rw', clobber=True) as fdla:
        fdla.write(final_data, extname='DLACAT')

    print(f"DLA catalog saved as {fname}.")
