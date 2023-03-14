# desi-y1-p1d
desi y1 p1d related scripts and version control

# How to generate Ohio Y1 mocks
These mocks can be found in `/global/cfs/cdirs/desicollab/users/naimgk/ohio-p1d/v1.2/iron/main/all_v0/v1.2.0`. Related info can be found in [DESI wiki](https://desi.lbl.gov/trac/wiki/LymanAlphaWG/OhioP1DMocks).

- Download and install [qsotools](https://github.com/p-slash/qsotools) package at v2.5 or higher minors if noted.
- Have DESIENV ready. DESISIM package used was between Mar 13 and later
- Create a `$BASEDIR` to store mocks. We will assume Ohio mock version to be v1.2. This will be updated for newer versions of quickquasars.
- Create folder structure and quickquasars scripts using `ohio-quickquasars-scripter.sh`. You can run `sh ohio-quickquasars-scripter.sh -h` for options.

```shell
sh ohio-quickquasars-scripter.sh --realization 0 --version v1.2 --release iron --survey main --catalog all_v0 --nexp 1
```

- Activate your conda environment for qsotools.

```shell
module load python
conda activate lya
```

- Allocate an interactive node.

```shell
salloc -N 1 -C cpu -q interactive -t 0:10:00
```

- Generate transmission files with qsotools. Need a quasar catalog. This should take less than 5 minutes. Default seed is 332298.

```shell
newGenDESILiteMocks.py v1.2/iron/main/all_v0/v1.2.0/transmissions/ --master-file /global/cfs/cdirs/desi/survey/catalogs/Y1/QSO/iron/QSO_cat_iron_main_dark_healpix_v0.fits --save-qqfile --nproc 128 --seed 0332298
```

- Exit this interactive shell and deactivate your conda environment.
- Load the DESI environment:

```shell
source /global/common/software/desi/desi_environment.sh main
```

- Allocate an interactive node.

```shell
salloc -N 1 -C cpu -q interactive -t 0:25:00
```

- Run quickquasars.

```shell
sh v1.2/iron/main/all_v0/v1.2.0/desi-1.5-1/submit-quickquasars-run0.sh
```

## Seed conventions
- `ohio-quickquasars-scripter.sh` has the following seed convention: `seed="62300${realization}"`.
- Transmissions files from `newGenDESILiteMocks.py` should have the following seed convention: `seed="${realization}332298"`.
