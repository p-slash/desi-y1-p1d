import argparse
from multiprocessing import Pool
from os import makedirs

import fitsio
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm


def parse(options=None):
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("--infiles", help="Input preproc zeros", nargs="+")
    parser.add_argument(
        "--outdir", help="Directory for to save.")
    parser.add_argument("--nslice", type=int, default=512)
    parser.add_argument("--mpad", type=int, default=2, help="padding factor")
    parser.add_argument("--mdown", type=int, default=4, help="downsample")
    parser.add_argument("--save-images", action="store_true")
    parser.add_argument("--nproc", default=None, type=int)
    args = parser.parse_args(options)

    return args


def getpower(im, n, mpad, mdown, da=0.6):
    Nx, Ny = im.shape
    ny = Ny // 2
    nl = ny * mpad
    k = np.fft.rfftfreq(nl, d=da)[:-1] * 2 * np.pi
    M = k.size
    k = k.reshape(M // mdown, mdown).mean(axis=-1)

    kplist = [k]
    for i in range(0, Nx, n):
        grid = im[:ny, i:i + n]
        d = np.fft.rfft(grid, axis=0, n=nl)[:-1] * da
        kplist.append(
            np.mean(np.abs(d)**2, axis=1).reshape(
                M // mdown, mdown).mean(axis=-1) / (da * ny))

        grid = im[ny:, i:i + n]
        d = np.fft.rfft(grid, axis=0, n=nl)[:-1] * da
        kplist.append(
            np.mean(np.abs(d)**2, axis=1).reshape(
                M // mdown, mdown).mean(axis=-1) / (da * ny))

    return kplist


def save_image(im, fname):
    plt.figure(figsize=(8, 8))
    plt.imshow(im, vmin=-2, vmax=2)
    plt.savefig(fname, dpi=150, bbox_inches='tight')
    plt.close()


def save_power_image(kplist, n, title, fname):
    k = kplist[0]
    i1, i2 = np.searchsorted(k, [1e-1, 1.0])
    nrow = len(kplist) - 1
    ncol = 2
    nrow = nrow // ncol
    fig, axs = plt.subplots(
        nrow, ncol, figsize=(24, 3 * nrow),
        sharex='all', gridspec_kw={'hspace': 0, 'wspace': 0.1})
    fig.suptitle(title, fontsize=16)
    for i, p in enumerate(kplist[1:]):
        r = i // 2
        c = i % 2
        s = "AB" if c == 0 else "CD"
        if r * n < 2048:
            s = s[0]
        else:
            s = s[1]
        dat = p[i1:i2]
        axs[r, c].plot(k[i1:i2], dat, '.-', label=f"{s}-{r * n}:{(r + 1) * n}")
        axs[r, c].legend(fontsize="x-large")
        mean, std = dat.mean(), dat.std()
        axs[r, c].axhline(mean, c='k')
        axs[r, c].axhspan(mean - std, mean + std, fc='k', alpha=0.3)
        # y1, y2 = np.min(p[i1:i2]), np.max(p[i1:i2])
        # axs[r, c].set_ylim(y1 * 0.95, y2 * 1.05)
        axs[r, c].grid()
        axs[r, c].set_ylabel("P [A]", fontsize=14)

    axs[-1, 0].set_xlabel("k [1 / A]", fontsize=14)
    axs[-1, 1].set_xlabel("k [1 / A]", fontsize=14)
    axs[-1, 0].set_xlim(1e-1, 1)
    axs[-1, 1].set_xlim(1e-1, 1)
    plt.savefig(fname, dpi=150, bbox_inches='tight')
    plt.close()


def one_wrap(f, args):
    fbase = f.split('/')[-1]
    if fbase.endswith(".fits"):
        ext = ".fits"
    elif fbase.endswith(".fits.gz"):
        ext = ".fits.gz"
    else:
        print("Extension error in", f)
        return None, None, None, None
    cam, expid = fbase.replace(ext, "").split("-")[1:3]
    fbase = args.outdir + '/' + fbase

    night = fitsio.read_header(f, ext="IMAGE")['NIGHT']
    im = fitsio.read(f, ext="IMAGE")
    if args.save_images:
        save_image(im, fbase.replace(ext, "-image.png"))

    kplist = getpower(im, args.nslice, args.mpad, args.mdown)
    kplist = np.vstack(kplist)
    save_power_image(
        kplist, args.nslice, f"{night}-{cam}-{expid}",
        fbase.replace(ext, f"-{night}-p1d-image.png"))
    return cam, expid, night, kplist


def main(options=None):
    args = parse(options)
    flist = args.infiles
    makedirs(args.outdir, exist_ok=True)

    ofname = args.outdir + "/preproc-zeros-powers.fits"
    fitsio.write(ofname, None, clobber=True)

    with Pool(processes=args.nproc) as pool:
        inputs = [(f, args) for f in flist]
        results = pool.starmap(one_wrap, inputs)

    results_dict = {}
    for cam, expid, night, kplist in tqdm(results):
        if cam is None:
            continue
        if cam not in results_dict:
            results_dict[cam] = [kplist.copy(), 1]
        else:
            results_dict[cam][0] += kplist
            results_dict[cam][1] += 1

        hdr = {"CAM": cam, "EXPID": expid, "NIGHT": night}
        fitsio.write(
            ofname, kplist, header=hdr, extname=f"{night}-{cam}-{expid}")

    for cam, (kplist, nj) in results_dict.items():
        kplist /= nj
        save_power_image(
            kplist, args.nslice, f"{night}-{cam}-mean",
            ofname.replace(".fits", f"-{night}-{cam}-mean-p1d-image.png"))
        hdr = {"CAM": cam, "EXPID": 0, "NIGHT": night}
        fitsio.write(
            ofname, kplist, header=hdr, extname=f"{night}-{cam}-AVE")
