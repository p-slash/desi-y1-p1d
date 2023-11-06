import argparse
import glob
from multiprocessing import Pool

import numpy as np
import fitsio

final_dtype = np.dtype([
    ('CHI2', 'f8'), ('COEFF', 'f8', 4), ('Z', 'f8'), ('ZERR', 'f8'),
    ('ZWARN', 'i8'), ('TARGETID', 'i8'),
    ('TARGET_RA', 'f8'), ('TARGET_DEC', 'f8'),
    ('FLUX_G', 'f4'), ('FLUX_R', 'f4'), ('FLUX_Z', 'f4')
])


def parse(options=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("Directory", help="Directory for spectra-16.")
    parser.add_argument(
        "SaveDirectory", help="Directory for to save final catalog.")
    parser.add_argument("--nproc", type=int, default=None)
    parser.add_argument(
        "--prefix", default='zbest', help='Healpix zbest file prefix.')
    args = parser.parse_args(options)

    return args


def one_zcatalog(fzbest):
    fts = fitsio.FITS(fzbest)

    hdr1 = fts['ZBEST'].read_header()

    nrows = hdr1['NAXIS']
    if nrows == 0:
        fts.close()
        return None

    newdata = np.empty(nrows, dtype=final_dtype)

    data = fts['ZBEST'].read()
    n1 = list(set(data.dtype.names).intersection(final_dtype.names))
    newdata[n1] = data[n1]

    data = fts['FIBERMAP'].read()
    n1 = list(set(data.dtype.names).intersection(final_dtype.names))
    newdata[n1] = data[n1]

    fts.close()

    return newdata


def main(options=None):
    args = parse(options)

    all_truths = glob.glob(f"{args.Directory}/*/*/{args.prefix}-*.fits*")

    numpy_arrs = []

    print("Iterating over files.")
    with Pool(processes=args.nproc) as pool:
        imap_it = pool.imap(one_zcatalog, all_truths)

        for arr in imap_it:
            if arr is None:
                continue

            numpy_arrs.append(arr)

    nqsos = 0
    for arr in numpy_arrs:
        nqsos += arr.size

    print(f"There are {nqsos} quasars.")

    final_data = np.concatenate(numpy_arrs)
    fname = f"{args.SaveDirectory}/zcat.fits"
    with fitsio.FITS(fname, 'rw', clobber=True) as fts:
        fts.write(final_data, extname='ZCATALOG')

    print(f"Quasar catalog saved as {fname}.")
