import argparse
import functools
import glob
from multiprocessing import Pool

import numpy as np
import fitsio
from tqdm import tqdm

from numpy.lib.recfunctions import rename_fields, join_by, drop_fields


final_dtype = np.dtype([
    ('CHI2', 'f8'), ('COEFF', 'f8', 4), ('Z', 'f8'), ('ZERR', 'f8'),
    ('ZWARN', 'i8'), ('TARGETID', 'i8'),
    ('TARGET_RA', 'f8'), ('TARGET_DEC', 'f8'),
    ('FLUX_G', 'f4'), ('FLUX_R', 'f4'), ('FLUX_Z', 'f4')
])


def parse(options=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
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

    nrows = hdr1['NAXIS2']
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


def one_truth_catalog(ftruth):
    fts = fitsio.FITS(ftruth)
    dla_out = None
    bal_out = None

    if 'DLA_META' in fts:
        if fts['DLA_META'].has_data():
            dla_out = fts['DLA_META'].read()
            dla_out = rename_fields(dla_out, {'Z_DLA': 'Z'})

    if 'BAL_META' in fts:
        if fts['BAL_META'].has_data():
            bal_out = fts['BAL_META'].read()

    fts.close()

    return dla_out, bal_out


def stack_and_resize(arr1, arr2):
    two_dim_dtype = [f for f in arr1.dtype.names if arr1[f].ndim == 2]
    all_same = all((arr1.dtype[f].shape[0] == arr2.dtype[f].shape[0] for f in two_dim_dtype))
    if all_same:
        return np.concatenate((arr1, arr2))

    # Create a new dtype with common field names and the maximum shape
    one_dim_dtype = [f for f in arr1.dtype.names if arr1[f].ndim == 1]
    new_dtype = [(f, arr1[f].dtype) for f in one_dim_dtype]

    for field in two_dim_dtype:
        new_shape = max(arr1.dtype[field].shape[0], arr2.dtype[field].shape[0])
        new_dtype.append((field, arr1[field].dtype, new_shape))

    new_dtype = np.dtype(new_dtype)
    stacked_array = np.empty(arr1.size + arr2.size, dtype=new_dtype)

    stacked_array[one_dim_dtype][:arr1.size] = arr1[one_dim_dtype]
    stacked_array[one_dim_dtype][arr1.size:] = arr2[one_dim_dtype]

    # Resize and fill with -1 for the first array
    for field in two_dim_dtype:
        n = new_dtype[field].shape[0] - arr1.dtype[field].shape[0]
        stacked_array[field][:len(arr1)] = np.pad(
            arr1[field], ((0, 0), (0, n)),
            'constant', constant_values=-1)

    # Resize and fill with -1 for the second array
    for field in two_dim_dtype:
        n = new_dtype[field].shape[0] - arr2.dtype[field].shape[0]
        stacked_array[field][len(arr1):] = np.pad(
            arr2[field], ((0, 0), (0, n)),
            'constant', constant_values=-1)

    return stacked_array


def save_data(list_of_arr, fname, what, extname):
    if not list_of_arr:
        print(f"No {what} catalog")
        return None

    if what == "BAL":
        final_data = functools.reduce(stack_and_resize, list_of_arr)
    else:
        final_data = np.concatenate(list_of_arr)
    print(f"There are {final_data.size} {what}s.")

    with fitsio.FITS(fname, 'rw', clobber=True) as fts:
        fts.write(final_data, extname=extname)

    print(f"{what} catalog saved as {fname}.")

    return final_data


def main(options=None):
    args = parse(options)

    all_zbests = glob.glob(f"{args.Directory}/*/*/{args.prefix}-*.fits*")
    all_truths = glob.glob(f"{args.Directory}/*/*/truth-*.fits*")

    zcat_list = []
    dlacat_list = []
    balcat_list = []

    print("Iterating over files.")
    with Pool(processes=args.nproc) as pool:
        imap_it = pool.imap(one_zcatalog, all_zbests)

        for arr in tqdm(imap_it, total=len(all_zbests), desc="zcat"):
            if arr is None:
                continue

            zcat_list.append(arr)

        imap_it = pool.imap(one_truth_catalog, all_truths)

        for dla_out, bal_out in tqdm(imap_it, total=len(all_truths), desc="truth"):
            if dla_out is not None:
                dlacat_list.append(dla_out)

            if bal_out is not None:
                balcat_list.append(bal_out)

    if balcat_list:
        zcat_fname = f"{args.SaveDirectory}/zcat_only.fits"
    else:
        zcat_fname = f"{args.SaveDirectory}/zcat.fits"

    zcat = save_data(zcat_list, zcat_fname, "quasar", "ZCATALOG")
    _ = save_data(
        dlacat_list, f"{args.SaveDirectory}/dla_cat.fits", "DLA", "DLACAT")
    bal_cat = save_data(
        balcat_list, f"{args.SaveDirectory}/bal_cat.fits", "BAL", "BALCAT")

    if bal_cat is None:
        return

    print("Creating zcat.fits with appended BAL info.")
    fill_value = {key: -1 for key in bal_cat.dtype.names}
    zcat_bal = join_by(
        'TARGETID', zcat, drop_fields(bal_cat, 'Z', usemask=False),
        jointype='outer', usemask=False, defaults=fill_value)
    fname = f"{args.SaveDirectory}/zcat.fits"
    with fitsio.FITS(fname, 'rw', clobber=True) as fts:
        fts.write(zcat_bal, extname='ZCATALOG')

    print(f"Quasar+BAL catalog saved as {fname}.")
