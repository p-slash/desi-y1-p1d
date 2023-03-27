DESI Y1 P1D related scripts and version control.

# Installation
Install directly from GitHub:
```shell
pip install desi_y1_p1d@git+https://github.com/p-slash/desi-y1-p1d.git
```
+ To install a specific version, add for example `@v0.1` at the end.
+ This will automatically install [qsotools](https://github.com/p-slash/qsotools), [qsonic](https://qsonic.readthedocs.io/en/stable/installation.html) and their requirements which include `numpy, scipy, mpi4py, iminuit, ...`. However, it could be better if you setup your own environment first. Pay specific attention to `mpi4py` package that qsonic needs. Follow these [instructions](https://docs.nersc.gov/development/languages/python/parallel-python/#mpi4py-in-your-custom-conda-environment) for NERSC.
+ You are responsible for the DESI environment, which should not be a problem on NERSC.

# Usage for mocks
`setup-ohio-chain` program is responsible for creating job scripts and chain scheduling them on NERSC Perlmutter. To streamline and version control the pipeline, certain settings are set within the package. Intended usage is to specify which settings to use. See avaliable settings in this [folder](src/desi_y1_p1d/configs/) or list them:
```shell
setup-ohio-chain --list-available-settings
```
You also need to set the following options:
+ `--root-dir`: directory to save the mock spectra. It will create the correct folder structure within this.
+ `--delta-dir`: directory to save the delta reductions. It will again create the correct folder structure.
+ `--rn1`: starting value for the realization. default: 0
+ `--nrealizations`: number of realizations. default: 1

*Example:* Create scripts for 10 Iron v0 realizations without any systematics and queue them.
```shell
setup-ohio-chain y1_iron_v0_nosyst --root-dir $PSCRATCH/ohio-mocks --delta-dir $PSCRATCH/ohio-deltas --nrealizations 10 --batch
```

You can change any option by providing a value in argument. However, you should also provide suffix(es) to distinguish these runs. To see the help and all options you can change:
```shell
setup-ohio-chain -h
```
To look into the details of your settings without running anything:
```shell
setup-ohio-chain [SETTING] --print-current-settings [other options can be passed.]
```

Finally, you can skip creating and batching scripts for transmissions files and/or quickquasars using `--no-transmissions` and `--skip-qq` options.

# Usage for data
`setup-desi-chain` program is responsible for running Lya, SB1-2-3 analysis back to back. Its usage is similar to the mocks.


# Legacy code
See [this folder](etc/) for reference scripts and steps taken.

