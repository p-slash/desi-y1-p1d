# directory structure
# ohio-p1d / v1.0 / everest / all / catalog / v1.0.x / desi-1.0-everest

## Set the desired options.
# Changing realization will change the seed
# and put mocks into the corresponding folder
nodes=1
nthreads=128
time="00:30:00"
rootdir="/global/cfs/cdirs/desi/users/naimgk/ohio-p1d"
# required options
realization=
version=
release=
survey=
catalog=
nexp=

# Note with dash (-) if running unusual parameters, 
# e.g. suffix="-RandomDLAs"
suffix=
extra_opts=

dla=""             # Can be random or file
metals_qq=""        # Metals can be added by quickquasars or from a file
metals_file=""
balprob=0           # BAL (Fiducial is 0.16) probability, empty is default of quickquasars (no BALs).
sigma_fog=0         # Fingers of god in km/s, leave empty for default value of quickquasars (150 km/s)
boring=false        # Do not multiply continuum by transmission, use F=1 everywhere

# Some default parameters you may want to change
cont_dwave=2.0
zmin=1.8

batch=false
runnow=false
# Print out config arguments
function print_config_help() {
    echo "usage"
    echo "--realization: [realization number] (required) (default: None)"
    echo "--version: [version] e.g. v1.2 (required) (default: None)"
    echo "--release: [release] e.g. fuji, himalayas (required) (default: None)"
    echo "--survey: [survey] e.g. all, main (required) (default: None)"
    echo "--catalog: [catalog] e.g. afterburn-unique (required) (default: None)"
    echo "--nexp: [number of exposures] e.g. 1, 4, 1000 (required) (default: None)"
    
    echo "--batch: submit the script as job. DESI env must be loaded."
    echo "--run: runs the script. must be on interactive node and have DESI env loaded."

    echo "--dla: [random or file] (default: ${dla})"
    echo "--balprob: [number between 0 and 1] add BAL features with the specified probability (default: ${balprob})"

    echo "--suffix: [suffix] suffix for the realization if custom parameters are passed."
    echo "--extra-opts: [extra_opts] extra options to pass to quickquasars."

    echo "--nodes: [number of nodes] (default: ${nodes})"
    echo "--nthreads: [number of threads] (default: ${nthreads})"
    echo "--time: [hh:mm:ss] (default: ${time})"
    echo "--rootdir: rootdir for mocks (default: ${rootdir})"
    echo "--help | -h : print this help"
}

# Set config options
function set_config_options() {
    while :; do
        case $1 in
        -h|--help)  print_config_help; exit;;
        --realization) realization="$2"; shift;;
        --version) version="$2"; shift;;
        --release) release="$2"; shift;;
        --survey) survey="$2"; shift;;
        --catalog) catalog="$2"; shift;;
        --nexp) nexp="$2"; shift;;

        --batch) batch=true;;
        --run)   runnow=true;;

        --dla) dla="$2"; shift;;
        --balprob) balprob="$2"; shift;;
        --suffix) suffix="$2"; shift;;
        --extra-opts) extra_opts="$2"; shift;;

        --nodes) nodes="$2"; shift;;
        --nthreads) nthreads="$2"; shift;;
        --time) time="$2"; shift;;
        -*) printf 'ERROR: Unknown option: %s\n' "$1"; exit 1;;
        *)  break
        esac
        shift
    done
}

function check_required() {
    for var in "$@"; do
        if [[ -z "${var}" ]]; then
            printf 'ERROR: required option: --%s\n' "$2"
            exit 1
        fi
        shift
    done
}

# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
# Main script starts here
# Default values
CONFIGURE_ARGS="$*"
set_config_options $CONFIGURE_ARGS
check_required "$realization" "realization" "$version" "version" "$release" "release" "$survey" "survey"
check_required "$catalog" "catalog" "$nexp" "nexp"

basedir="${rootdir}/${version}/${release}/${survey}/${catalog}/${version}.${realization}"
idir="${basedir}/transmissions/"

## Derive relevant variables and setup relevant directories.
seed="62300${realization}"
sysopt=""
OPTS_QQ="--zmin ${zmin} --zbest --bbflux --seed ${seed} --exptime ${nexp}000 --save-continuum --save-continuum-dwave ${cont_dwave} ${extra_opts}"

if [[ ! -z "${dla}" ]]; then
    sysopt+="1"
    OPTS_QQ+=" --dla ${dla} "
fi

if [[ ! -z "${metals_qq}" ]]; then
    sysopt+="2"
    OPTS_QQ+=" --metals ${metals_qq}"
fi

if [[ ! -z "${metals_file}" ]]; then
    sysopt+="3"
    OPTS_QQ+=" --metals-from-file ${metals_file}"
fi

if [[ ! -z "${balprob}" ]]; then
    if [[ $balprob != 0 ]]; then
        sysopt+="4"
        OPTS_QQ+=" --balprob ${balprob}"
    fi
fi

if [[ ! -z "${sigma_fog}" ]]; then
    OPTS_QQ+=" --sigma_kms_fog ${sigma_fog}"
    if [[ $sigma_fog == 0 ]]; then
        sysopt+="5"
    fi
fi

if [[ "$boring" == true ]]; then
    sysopt+="6"
    OPTS_QQ+=" --no-transmission"
fi

if [[ -z "${sysopt}" ]]; then
    sysopt="0"
fi

printf "Saving scripts to ${basedir}\n"

# file name convention
# desi-${version}.${sysopt}-${nexp}-${suffix}
# We do not need nexp in e2e analysis, so it simply says e2e
outdir="${basedir}/desi-${version:1:1}.${sysopt}-${nexp}${suffix}/"
runfile="${outdir}/submit-quickquasars-run${realization}.sh"

# make directories to store logs and spectra
if [[ ! -d $outdir ]]; then
    mkdir -p $outdir
    mkdir -p $idir
fi
if [[ ! -d $outdir/logs ]]; then
    mkdir -p $outdir/logs
fi
if [[ ! -d $outdir/spectra-16 ]]; then
    mkdir -p $outdir/spectra-16
fi

##############################################################################
## Generate the run file.
echo "Run file will be written to "$runfile
cat > $runfile <<EOF
#!/bin/bash -l

#SBATCH -C cpu
#SBATCH --account=desi
#SBATCH --nodes=$nodes
#SBATCH --time=00:30:00
#SBATCH --job-name=quickquasar_spectra
#SBATCH --output=$outdir/lyasim.log

echo "get list of skewers to run ..."

files=\`ls -1 $idir/*/*/lya-transmission*.fits*\`
nfiles=\`echo \$files | wc -w\`
nfilespernode=\$(( \$nfiles/$nodes + 1))

echo "n files =" $nfiles
echo "n files per node =" \$nfilespernode

first=1
last=\$nfilespernode
for node in \`seq $nodes\` ; do
    echo "starting node \$node"

    # list of files to run
    if (( \$node == $nodes )) ; then
    last=""
    fi
    echo \${first}-\${last}
    tfiles=\`echo \$files | cut -d " " -f \${first}-\${last}\`
    first=\$(( \$first + \$nfilespernode ))
    last=\$(( \$last + \$nfilespernode ))
    command="srun -N 1 -n 1 -c $nthreads quickquasars -i \$tfiles --nproc $nthreads --outdir $outdir/spectra-16 ${OPTS_QQ}" 

    echo \$command
    echo "log in $outdir/logs/node-\$node.log"

    \$command >& $outdir/logs/node-\$node.log &

done

wait
echo "END"

if [ $SLURM_NODEID -eq 0 ]; then
    desi_zcatalog -i ${outdir}/spectra-16 -o ${outdir}/zcat.fits --minimal --prefix zbest
fi

EOF

#Run the job.
if [[ "$runnow" == true ]]; then
    sh $runfile
#Send the job.
elif [[ "$batch" == true ]]; then
    sbatch $runfile
fi

##############################################################################