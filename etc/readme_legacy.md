# desi-y1-p1d
desi y1 p1d related scripts and version control

# How to generate Ohio Y1 mocks
These mocks can be found in `/global/cfs/cdirs/desicollab/users/naimgk/ohio-p1d/v1.2/iron/main/all_v0/v1.2.0`. Related info can be found in [DESI wiki](https://desi.lbl.gov/trac/wiki/LymanAlphaWG/OhioP1DMocks).

- Download and install [qsotools](https://github.com/p-slash/qsotools) package at v2.5 or higher minors if noted at `$SCRIPTDIR`.
- Have DESIENV ready. DESISIM package used was between Mar 13 and later
- Create a `$BASEDIR` to store mocks. We will assume Ohio mock version to be v1.2. This will be updated for newer versions of quickquasars.

```shell
mkdir $BASEDIR
cd $BASEDIR
```

- Create folder structure and quickquasars scripts using `ohio-quickquasars-scripter.sh`. You can run `sh ohio-quickquasars-scripter.sh -h` for options.

```shell
sh $SCRIPTDIR/ohio-quickquasars-scripter.sh --realization 0 --version v1.2 --release iron --survey main --catalog all_v0 --nexp 1
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

- Produce true DLA catalog if added.

```shell
python $SCRIPTDIR/getMockTrueDLAcat.py v1.2/iron/main/all_v0/v1.2.0/desi-1.5-1/spectra-16/ v1.2/iron/main/all_v0/v1.2.0/desi-1.5-1 --nproc 128
```

## Seed conventions
- `ohio-quickquasars-scripter.sh` has the following seed convention: `seed="62300${realization}"`.
- Transmissions files from `newGenDESILiteMocks.py` should have the following seed convention: `seed="${realization}332298"`.

# Getting P1D
## Delta reduction
+ I will be using qsonic for continuum fitting. Get the stable qsonic in a conda environment following instructions [here](https://qsonic.readthedocs.io/en/stable/installation.html).
+ Use the following as a template for `run-qsonic-fit.sh`:

```shell
basedir="/global/cfs/cdirs/desicollab/users/naimgk/ohio-p1d/v1.2/iron/main/all_v0/v1.2.0/desi-1.5-1"
inputdir="${basedir}/spectra-16"
catalog="${basedir}/zcat.fits"
outdir="/pscratch/sd/n/naimgk/ohio-p1d-analysis/v1.2/iron/main/all_v0/v1.2.0/desi-1.5-1/Delta-0.5.0-dev"

srun -n 128 -c 2 qsonic-fit \
--input-dir $inputdir \
--catalog $catalog \
-o $outdir \
--mock-analysis \
--no-iterations 10 \
--rfdwave 0.8 --skip 0.2 \
--wave1 3600 --wave2 6600
```
+ Activate your qsonic environment. Allocate an interactive node and run this script `sh run-qsonic-fit.sh`. This should need less than 5 mins.

```shell
salloc -N 1 -C cpu -q interactive -t 0:10:00
```

## Running QMLE
+ Create the required filename list using `getLists4QMLEfromPICCA.py` script from qsotools. You can use the same interactive node if you have qsotools installed in the same environment.

```shell
getLists4QMLEfromPICCA.py $outdir --nproc 128
```

+ Copy `qmle-config-template.param` to your working directory and modify it.
+ You first need to create lookup tables. These do not change between realizations, so pick a location that is easy to access from all settings. Note folder is relative to the output directory in the config file. To create the lookup tables (assuming you're using the resolution matrix):

```shell
srun -n 1 -c 2 CreateSQLookUpTable qmle-config.param
```

+ After this is done you can run QMLE.

```shell
srun -n 128 -c 2 LyaPowerEstimate qmle-config.param
```

