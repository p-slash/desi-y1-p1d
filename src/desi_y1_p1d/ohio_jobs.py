from os import makedirs
import os.path
import glob
from datetime import timedelta, datetime

from desi_y1_p1d import utils


class Job():
    def __init__(self, settings, section):
        self.name = section
        self.nodes = settings.getint(section, 'nodes')
        self.nthreads = settings.getint(section, 'nthreads')
        self.time = settings.getfloat(section, 'time')
        self.queue = settings[section]['queue']
        self.batch = settings.getboolean(section, 'batch')
        self.skip = settings.getboolean(section, 'skip', fallback=False)
        self.submitter_fname = None

    def create_directory(self, create_dir=None):
        raise NotImplementedError

    def create_script(self):
        raise NotImplementedError

    def setup(self):
        if self.skip:
            return

        self.create_directory()
        self.create_script()

    def schedule(self, dep_jobid=None, hold=False):
        skip = not (self.batch and self.submitter_fname)

        if skip:
            return -1

        print(f"Setting up a {self.name} job...")

        jobid = utils.submit_script(
            self.submitter_fname, dep_jobid, skip=skip, hold=hold)
        print(f"{self.name} job submitted with JobID: {jobid}")
        print("--------------------------------------------------")
        return jobid


class OhioQuickquasarsJob(Job):
    def __init__(self, rootdir, realization, settings):
        super().__init__(settings, 'quickquasars')

        self.rootdir = rootdir
        self.realization = realization

        self.version = settings['ohio']['version']
        self.release = settings['ohio']['release']
        self.survey = settings['ohio']['survey']
        self.catalog = settings['ohio']['catalog']

        self.nexp = settings.getint('quickquasars', 'nexp')
        self.zmin_qq = settings.getfloat('quickquasars', 'zmin_qq')
        self.cont_dwave = settings.getfloat('quickquasars', 'cont_dwave')
        self.dla = settings['quickquasars']['dla']
        self.bal = settings.getfloat('quickquasars', 'bal')
        self.boring = settings.getboolean('quickquasars', 'boring')
        self.seed_base = settings.getint('quickquasars', 'base_seed')
        self.env_command = settings['quickquasars']['env_command_qq']
        self.suffix = settings['quickquasars']['suffix']
        self.exptime_fluxr_catalog = settings.get(
            'quickquasars', 'exptime_fluxr_catalog', fallback="")

        self.exptime = f"{self.nexp}000"
        self.seed = f"{self.seed_base}{self.realization}"
        self.set_sysopt()

        self.foldername = f"desi-{self.version[1]}.{self.sysopt}"
        self._set_dirs()

        self.submitter_fname = None

    def _set_dirs(self):
        self.interm_path = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)
        self.desibase_dir = os.path.join(
            self.rootdir, self.interm_path, self.foldername)
        self.transmissions_dir = os.path.join(
            self.rootdir, self.interm_path, "transmissions")

    def inc_realization(self):
        self.realization += 1
        self.seed = f"{self.seed_base}{self.realization}"
        self._set_dirs()

    def set_sysopt(self):
        sysopt = ""
        OPTS_QQ = (f"--zmin {self.zmin_qq} --zbest --bbflux --seed {self.seed}"
                   f" --save-continuum --save-continuum-dwave {self.cont_dwave}")

        if self.exptime_fluxr_catalog:
            OPTS_QQ += f" --from-catalog {self.exptime_fluxr_catalog}"
        else:
            OPTS_QQ += f" --exptime {self.exptime}"

        if self.dla:
            sysopt += f"{self.dla[0]}1"
            OPTS_QQ += f" --dla {self.dla}"

        # if self.metals:
        #     sysopt += "2"
        #     OPTS_QQ += f" --metals {self.metals}"

        # if self.metals_from_file:
        #     sysopt += "3"
        #     OPTS_QQ += f" --metals-from-file {self.metals_from_file}"

        if self.bal and self.bal > 0:
            sysopt += "4"
            OPTS_QQ += f" --balprob {self.bal}"

        OPTS_QQ += " --sigma_kms_fog 0"
        sysopt += "5"

        if self.boring:
            sysopt += "6"
            OPTS_QQ += " --no-transmission"

        if not sysopt:
            sysopt = "0"

        self.sysopt = f"{sysopt}-{self.nexp}{self.suffix}"
        self.OPTS_QQ = OPTS_QQ

    def create_directory(self, create_dir=True):
        if not create_dir:
            return self.desibase_dir

        # make directories to store logs and spectra
        # print("Creating directories:")
        # print(f"+ {self.desibase_dir}")
        # print(f"+ {self.desibase_dir}/logs")
        # print(f"+ {self.desibase_dir}/spectra-16")

        makedirs(self.desibase_dir, exist_ok=True)
        makedirs(f"{self.desibase_dir}/logs", exist_ok=True)
        makedirs(f"{self.desibase_dir}/spectra-16", exist_ok=True)

        return self.desibase_dir

    def getQqCatalogCommand(self):
        relpath_to_tr = os.path.relpath(self.desibase_dir, self.transmissions_dir)
        extra_commands = [
            f'cd {self.transmissions_dir}',
            f"qq-zcatalog {relpath_to_tr}/spectra-16 {relpath_to_tr}"]
        script_txt = " \\\n&& ".join(extra_commands) + '\n'
        return script_txt

    def create_script(self):
        """ Creates and writes the script for quickquasars run. Sets self.submitter_fname.

        Args:
            dep_jobid (int): Dependent JobID. Defaults to None.
        """
        time_txt = timedelta(minutes=self.time)

        script_txt = utils.get_script_header(
            self.desibase_dir, f"ohio-qq-y1-{self.realization}",
            time_txt, self.nodes, self.queue)

        relpath_to_tr = os.path.relpath(self.desibase_dir, self.transmissions_dir)

        command = (f"srun -N 1 -n 1 -c {self.nthreads} "
                   f"quickquasars -i $tfiles --nproc {self.nthreads} "
                   f"--outdir {relpath_to_tr}/spectra-16 {self.OPTS_QQ}")

        script_txt += (
            f'{self.env_command}\n\n'
            '# Change directory...\n'
            f'cd {self.transmissions_dir}\n'
            'echo "get list of skewers to run ..."\n\n'

            "files=`ls -1 ./*/*/lya-transmission*.fits*`\n"
            "nfiles=`echo $files | wc -w`\n"
            f"nfilespernode=$(( $nfiles/{self.nodes} + 1))\n\n"

            'echo "n files =" $nfiles\n'
            'echo "n files per node =" $nfilespernode\n\n'

            "first=1\n"
            "last=$nfilespernode\n"
            f"for node in `seq {self.nodes}` ; do\n"
            "    echo 'starting node $node'\n\n"

            "    # list of files to run\n"
            f"    if (( $node == {self.nodes} )) ; then\n"
            "        last=""\n"
            "    fi\n"
            "    echo ${first}-${last}\n"
            '    tfiles=`echo $files | cut -d " " -f ${first}-${last}`\n'
            "    first=$(( $first + $nfilespernode ))\n"
            "    last=$(( $last + $nfilespernode ))\n"
            f'    command="{command}"\n\n'

            "    echo $command\n"
            f'    echo "log in {relpath_to_tr}/logs/node-$node.log"\n\n'

            f"    $command >& {relpath_to_tr}/logs/node-$node.log &\n"
            "done\n\n"

            "wait\n"
            "echo 'END'\n\n")

        # script_txt += utils.get_script_text_for_master_node(
        #     f"qq-zcatalog {relpath_to_tr}/spectra-16 {relpath_to_tr}")
        self.submitter_fname = utils.save_submit_script(
            script_txt, self.desibase_dir, "quickquasars")

        print(f"Quickquasars script is saved as {self.submitter_fname}.")


class OhioTransmissionsJob(Job):
    def __init__(self, rootdir, realization, settings):
        super().__init__(settings, 'transmissions')

        self.rootdir = rootdir
        self.realization = realization

        self.version = settings['ohio']['version']
        self.release = settings['ohio']['release']
        self.survey = settings['ohio']['survey']
        self.catalog = settings['ohio']['catalog']

        self.seed_base = settings.getint('transmissions', 'base_seed')
        self.seed = f"{self.realization}{self.seed_base}"

        self._set_dirs()
        self.submitter_fname = None

    def _set_dirs(self):
        self.interm_path = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)

        self.transmissions_dir = os.path.join(
            self.rootdir, self.interm_path, "transmissions")

    def inc_realization(self):
        self.realization += 1
        self.seed = f"{self.realization}{self.seed_base}"

        self._set_dirs()
        self.submitter_fname = None

    def create_directory(self, create_dir=True):
        if create_dir:
            # print("Creating directories:")
            # print(f"+ {self.transmissions_dir}")
            makedirs(self.transmissions_dir, exist_ok=True)

        return self.transmissions_dir

    def create_script(self):
        """ Creates and writes the script for newGenDESILiteMocks run.
        Sets self.submitter_fname.
        """
        time_txt = timedelta(minutes=self.time)

        script_txt = utils.get_script_header(
            self.transmissions_dir, f"ohio-trans-y1-{self.realization}",
            time_txt, nodes=self.nodes, queue=self.queue)

        script_txt += 'echo "Generating transmission files using qsotools."\n\n'
        script_txt += (f"newGenDESILiteMocks {self.transmissions_dir} "
                       f"--master-file {self.catalog} --save-qqfile --nproc {self.nthreads} "
                       f"--seed {self.seed}\n")

        self.submitter_fname = utils.save_submit_script(
            script_txt, self.transmissions_dir, f"gen-trans-{self.realization}")

        print(f"newGenDESILiteMocks script is saved as {self.submitter_fname}.")


class QSOnicJob(Job):
    def __init__(
            self, rootdir, foldername, is_mock, settings, section, jobname=""
    ):
        super().__init__(settings, section)
        qsonic_settings = settings[section]
        self.rootdir = rootdir
        self.foldername = foldername
        self.is_mock = is_mock

        if jobname:
            self.jobname = jobname
        else:
            self.jobname = f"qsonic-{self.foldername}"

        self.wave1 = qsonic_settings['wave1']
        self.wave2 = qsonic_settings['wave2']
        self.forest_w1 = qsonic_settings['forest_w1']
        self.forest_w2 = qsonic_settings['forest_w2']
        self.cont_order = qsonic_settings.getint('cont_order')
        self.use_truecont = self.cont_order < 0
        self.coadd_arms = qsonic_settings.get('coadd_arms', fallback="before")
        self.exposures = qsonic_settings.get('exposures', fallback="")
        self.rfdwave = qsonic_settings.getfloat('rfdwave', fallback=0.8)
        self.arms = qsonic_settings.get('arms', fallback="")
        self.fiducial_meanflux = qsonic_settings.get(
            'fiducial_meanflux', fallback=None)
        self.fiducial_varlss = qsonic_settings.get(
            'fiducial_varlss', fallback=None)
        self.skip_resomat = qsonic_settings.getboolean(
            'skip_resomat', fallback=False)
        self.run_snr_splits = qsonic_settings.getboolean(
            'run_snr_splits', fallback=False)
        self.dla = qsonic_settings.get("dla-mask", "")
        self.bal = qsonic_settings.getboolean('bal-mask', fallback=False)
        self.sky = qsonic_settings['sky-mask']
        self.extra_opts = qsonic_settings.get("fit_extra_opts", fallback="")
        self.env_command = qsonic_settings['env_command']
        self.tile_format = False
        self.calibfile = qsonic_settings.get("calibration", fallback=None)

        if self.use_truecont:
            assert self.fiducial_varlss
            assert self.fiducial_meanflux
        elif self.fiducial_varlss:
            print("Ignore fiducial_varlss. Keep fiducial_meanflux.")
            self.fiducial_varlss = ""

        if self.use_truecont:
            self.suffix = "-tc"
        else:
            self.suffix = f"-co{self.cont_order}"
        if self.dla or self.bal or self.sky:
            self.suffix += "-m"
        if self.dla:
            self.suffix += "d"
        if self.bal:
            self.suffix += "b"
        if self.sky:
            self.suffix += "s"
        self.suffix += qsonic_settings['suffix']

        self.catalog = None
        self.indir = None
        self.interm_path = None
        self.outdelta_dir = None
        self.submitter_fname = None

    def create_directory(self, create_dir=True):
        if not self.rootdir:
            return None

        if create_dir:
            makedirs(self.outdelta_dir, exist_ok=True)

        return self.outdelta_dir

    def getSnrSplitCalibRuns(
            self, snr_edges=[0.3, 1.0, 1.5, 2.0, 3.0, 5.0, 100.]
    ):
        cpus_pt = 256 // (self.nthreads // self.nodes)
        extra_commands = []
        extra_commands.append(f"mkdir -p snr-splits")
        for i in range(len(snr_edges) - 1):
            qsonic_command = (
                f"srun -N {self.nodes} -n {self.nthreads} -c {cpus_pt} "
                f"qsonic-calib \\\n-i . -o snr-splits --fbase attributes \\\n"
                f"--min-snr {snr_edges[i]} --max-snr {snr_edges[i + 1]} \\\n"
                f"--wave1 {self.wave1} --wave2 {self.wave2} --var-use-cov"
            )
            extra_commands.append(qsonic_command)

        return extra_commands

    def getfitAmplifierRegionsCommands(self):
        extra_commands = [
            f"cd {self.outdelta_dir}",
            ("fitAmplifierRegions continuum_chi2_catalog.fits "
             "snr-splits snr-splits/summary.fits")]

        script_txt = " \\\n&& ".join(extra_commands) + '\n'
        return script_txt

    def create_script(self):
        """ Creates and writes the script for QSOnic run.
        Sets self.submitter_fname.

        Args:
            dep_jobid (int): Dependent JobID. Defaults to None.
        """
        if self.outdelta_dir is None:
            return None

        time_txt = timedelta(minutes=self.time)
        script_txt = utils.get_script_header(
            self.outdelta_dir, self.jobname, time_txt, self.nodes, self.queue)

        script_txt += f"cd {self.outdelta_dir}\n\n"
        script_txt += f'{self.env_command}\n\n'
        cpus_pt = 256 // (self.nthreads // self.nodes)
        # script_txt += (f"export OMP_NUM_THREADS={cpus_pt}\n"
        #                "export OMP_PLACES=threads\n"
        #                "export OMP_PROC_BIND=spread\n\n")

        commands = []
        qsonic_command = (
            f"srun -N {self.nodes} -n {self.nthreads} -c {cpus_pt} "
            f"qsonic-fit \\\n-i {self.indir} -o . \\\n"
            f"--catalog {self.catalog} \\\n"
            f"--rfdwave {self.rfdwave} --skip 0.2 \\\n"
            f"--num-iterations 20 \\\n"
            f"--cont-order {self.cont_order} \\\n"
            f"--wave1 {self.wave1} --wave2 {self.wave2} \\\n"
            f"--forest-w1 {self.forest_w1} --forest-w2 {self.forest_w2}")

        if self.arms:
            qsonic_command += f" \\\n--arms {self.arms}"
        if self.exposures:
            qsonic_command += f" \\\n--exposures {self.exposures}"
        if self.is_mock:
            qsonic_command += " \\\n--mock-analysis"
            if self.use_truecont:
                qsonic_command += " \\\n--true-continuum"
        if self.tile_format:
            qsonic_command += " \\\n--tile-format"
        if self.dla:
            qsonic_command += f" \\\n--dla-mask {self.dla}"
        if self.bal:
            qsonic_command += " \\\n--bal-mask"
        if self.sky:
            qsonic_command += f" \\\n--sky-mask {self.sky}"
        if self.coadd_arms:
            qsonic_command += f" \\\n--coadd-arms {self.coadd_arms}"
        if self.skip_resomat:
            qsonic_command += " \\\n--skip-resomat"
        if self.fiducial_meanflux:
            qsonic_command += f" \\\n--fiducial-meanflux {self.fiducial_meanflux}"
        if self.fiducial_varlss:
            qsonic_command += f" \\\n--fiducial-varlss {self.fiducial_varlss}"
        if self.calibfile:
            qsonic_command += (f" \\\n--noise-calibration {self.calibfile}"
                               f" \\\n--flux-calibration {self.calibfile}")
        if self.extra_opts:
            qsonic_command += f" \\\n{self.extra_opts}"

        commands.append(qsonic_command)

        script_txt += " \\\n&& ".join(commands) + '\n'

        if self.run_snr_splits:
            extra_commands = self.getSnrSplitCalibRuns()
            script_txt += '\n' + "\n\n".join(extra_commands) + '\n'

        self.submitter_fname = utils.save_submit_script(
            script_txt, self.outdelta_dir, "qsonic-fit")

        print(f"QSOnic script is saved as {self.submitter_fname}.")


class QSOnicMockJob(QSOnicJob):
    def __init__(
            self, rootdir, interm_path, desibase_dir, foldername,
            realization, settings
    ):
        super().__init__(rootdir, foldername, True, settings, 'qsonic')
        self.interm_path = interm_path
        self.desibase_dir = desibase_dir
        self.realization = realization

        self._set_paths()
        self.submitter_fname = None
        self.jobname = f"qsonic-{self.realization}"

    def _set_paths(self):
        self.indir = f"{self.desibase_dir}/spectra-16"
        self.catalog = f"{self.desibase_dir}/zcat.fits"

        if self.dla:
            self.dla = f"{self.desibase_dir}/dla_cat.fits"

        if self.rootdir:
            self.outdelta_dir = os.path.join(
                self.rootdir, self.interm_path, self.foldername, f"Delta{self.suffix}")
        else:
            self.outdelta_dir = None

    def inc_realization(self, new_interm_path, new_desibase_dir):
        self.realization += 1
        self.jobname = f"qsonic-{self.realization}"
        self.interm_path = new_interm_path
        self.desibase_dir = new_desibase_dir

        self._set_paths()
        self.submitter_fname = None


class QSOnicDataJob(QSOnicJob):
    def __init__(self, rootdir, forest, desi_settings, settings, section):
        super().__init__(rootdir, forest, False, settings, section)

        self.catalog = desi_settings['catalog']

        if desi_settings['survey'] == "tiles":
            self.indir = f"{desi_settings['redux']}/{desi_settings['release']}/tiles/cumulative"
            self.tile_format = True
        else:
            self.indir = f"{desi_settings['redux']}/{desi_settings['release']}/healpix"
            self.tile_format = False

        self.interm_path = utils.get_folder_structure_data(
            desi_settings['release'], desi_settings['survey'], self.catalog)
        self.outdelta_dir = os.path.join(
            self.rootdir, self.interm_path, self.foldername, f"Delta{self.suffix}")

        self.submitter_fname = None


class LyspeqJob(Job):
    def __init__(
            self, rootdir, outdelta_dir, sysopt,
            settings, section='qmle', jobname='Lya'
    ):
        super().__init__(settings, section)
        sb_suff = "-sb" if "SB" in section else ""

        # Filenames should relative to outdelta_dir
        self.working_dir = os.path.abspath(outdelta_dir)
        self.qmle_settings = dict(settings[section])
        self.qmle_settings['FileInputDir'] = "."
        self.qmle_settings['FileNameList'] = "fname_list.txt"
        self.qmle_settings['LookUpTableDir'] = os.path.abspath(
            f"{rootdir}/lookuptables{sb_suff}")
        self.qmle_settings['FileNameRList'] = os.path.abspath(
            f"{rootdir}/specres_list-rmat.txt")
        self.qmle_settings['OutputFileBase'] = self._get_output_fbase(settings, sysopt)
        self.qmle_settings['OutputDir'] = f"results-{self.qmle_settings['OutputFileBase']}"

        self.config_file = f"config-qmle-{self.qmle_settings['OutputFileBase']}.txt"
        self.jobname = f"{jobname}-{self.qmle_settings['OutputFileBase']}"

        self.abspath_configfile = f"{self.working_dir}/{self.config_file}"
        self.abspath_outputdir = f"{self.working_dir}/{self.qmle_settings['OutputDir']}"

    def _get_output_fbase(self, settings, sysopt):
        if "ohio" in settings.sections():
            release = settings['ohio']['release']
            fbase = f"mock-{release}-{sysopt}"
        else:
            release = settings['desi']['release']
            fbase = f"desi-{release}"

        oversample = int(self.qmle_settings.get('OversampleRmat', 0))
        deconvolve = float(self.qmle_settings.get('ResoMatDeconvolutionM', 0))
        marg = int(self.qmle_settings.get('ContinuumLogLambdaMargOrder', -1))
        nchunks = int(self.qmle_settings.get('DynamicChunkNumber', 0))

        if oversample > 0:
            fbase += f"-o{oversample}"
        if deconvolve > 0:
            fbase += f"-dc{deconvolve:.2f}"
        if nchunks > 0:
            fbase += f"-n{nchunks}"
        if marg > -1:
            fbase += f"-cm{marg}"

        return fbase

    def create_directory(self, create_dir=True):
        if not create_dir:
            return

        makedirs(self.abspath_outputdir, exist_ok=True)
        makedirs(self.qmle_settings['LookUpTableDir'], exist_ok=True)

        if not os.path.exists(self.qmle_settings['FileNameRList']):
            with open(self.qmle_settings['FileNameRList'], 'w') as frfile:
                frfile.write("1\n")
                frfile.write("1000000 0.1\n")

    def create_config(self):
        if self.skip:
            return

        omitted_keys = [
            "nodes", "nthreads", "batch", "skip", "time", "queue",
            "env_command"]
        config_lines = [
            f"{key} {value}\n"
            for key, value in self.qmle_settings.items()
            if key not in omitted_keys
        ]
        config_txt = ''.join(config_lines)
        with open(self.abspath_configfile, 'w') as f:
            f.write(config_txt)

        print(f"LyspeqJob config is saved as {self.abspath_configfile}.")


class QmleJob(LyspeqJob):
    def get_bootstrap_commands(self):
        save_boots = int(self.qmle_settings['NumberOfBoots']) > 0
        if not save_boots:
            return []

        obase = self.qmle_settings['OutputFileBase']
        bootcovfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{obase}_bootstrap_mean_fisher_matrix.txt")
        infisherfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{obase}_it1_fisher_matrix.txt")
        incovfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{obase}_it1_inversefisher_matrix.txt")

        extra_commands = [
            f"cd {self.working_dir}",
            (f"regularizeBootstrapCov --boot-matrix {bootcovfile} "
             f"--qmle-fisher {infisherfile} "
             f"--qmle-cov {incovfile} "
             f"--qmle-sparcity-cut 0 --force-posdef-diff "
             f"--nz {self.qmle_settings['NumberOfRedshiftBins']} "
             f"--fbase {self.qmle_settings['OutputFileBase']}_")]

        script_txt = " \\\n&& ".join(extra_commands) + '\n'
        return script_txt

    def create_script(self):
        self.create_config()

        time_txt = timedelta(minutes=self.time)

        script_txt = utils.get_script_header(
            self.abspath_outputdir, self.jobname,
            time_txt, self.nodes, self.queue)
        cpus_pt = 256 // (self.nthreads // self.nodes)

        script_txt += f"cd {self.working_dir}\n\n"
        script_txt += self.get_filelist_script_txt()
        script_txt += f"{self.qmle_settings['env_command']}\n\n"
        script_txt += (f"export OMP_NUM_THREADS={cpus_pt}\n"
                       "export OMP_PLACES=threads\n"
                       "export OMP_PROC_BIND=spread\n\n")

        script_txt += (
            f"srun -N {self.nodes} -n {self.nthreads} -c {cpus_pt} "
            f"--cpu_bind=cores LyaPowerEstimate {self.config_file}\n")

        # remove error_logs
        o = self.qmle_settings['OutputDir']
        script_txt += utils.get_script_text_for_master_node(
            f"if [ $(ls -1 {o}/error_log*.txt 2>/dev/null | wc -l) -gt 0 ]; "
            f"then cat {o}/error_log*.txt > {o}/all_error_logs.txt && "
            f"rm {o}/error_log*.txt; fi")

        self.submitter_fname = utils.save_submit_script(
            script_txt, self.working_dir, self.jobname)

        print(f"QmleJob script is saved as {self.submitter_fname}.")

    def needs_sqjob(self):
        files_exits = True
        files_exits &= os.path.exists(
            f"{self.qmle_settings['LookUpTableDir']}/signal_R1000000_dv0.1.dat"
        )

        deriv_files = glob.glob(
            f"{self.qmle_settings['LookUpTableDir']}/deriv_R1000000_dv0.1*.dat"
        )
        expected_nk = (
            int(self.qmle_settings.get("NumberOfLinearBins"))
            + int(self.qmle_settings.get("NumberOfLog10Bins")))
        files_exits &= len(deriv_files) == expected_nk

        return not files_exits

    def get_filelist_script_txt(self):
        script_txt = " && ".join([
            "getLists4QMLEfromPICCA . --nproc 128",
            "getLists4QMLEfromPICCA . --nproc 128 --snr-cut 1",
            "getLists4QMLEfromPICCA . --nproc 128 --snr-cut 2"
        ])  # + f'\n\ncd {self.working_dir}\n\n'

        script_txt = (
            'if [[ ! -f "' + self.qmle_settings['FileNameList'] + '" ]]; then\n'
            f"    {script_txt}\n"
            "fi\n\n")

        return script_txt


class XeQmleJob(QmleJob):
    def get_bootstrap_commands(self):
        save_boots = int(self.qmle_settings['NumberOfBoots']) > 0
        if not save_boots:
            return []

        obase = self.qmle_settings['OutputFileBase']
        bootcovfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{obase}_bootstrap_mean_fisher_matrix.txt")
        infisherfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{obase}_it1_fisher_matrix.txt")
        incovfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{obase}_it1_inversefisher_matrix.txt")

        extra_commands = [
            f"cd {self.working_dir}",
            (f"regularizeBootstrapCov --boot-matrix {bootcovfile} "
             f"--qmle-fisher {infisherfile} "
             f"--qmle-cov {incovfile} "
             "--qmle-sparcity-cut 0 "
             f"--nz {self.qmle_settings['NumberOfRedshiftBins']} "
             f"--fbase {self.qmle_settings['OutputFileBase']}_")]

        script_txt = " \\\n&& ".join(extra_commands) + '\n'
        return script_txt

    def create_script(self):
        self.qmle_settings['FileNameList'] = "fname_list_xe.txt"
        self.create_config()

        time_txt = timedelta(minutes=self.time)

        script_txt = utils.get_script_header(
            self.abspath_outputdir, self.jobname,
            time_txt, self.nodes, self.queue)
        cpus_pt = 256 // (self.nthreads // self.nodes)

        script_txt += f"cd {self.working_dir}\n\n"
        script_txt += self.get_filelist_script_txt()
        script_txt += f"{self.qmle_settings['env_command']}\n\n"
        script_txt += (f"export OMP_NUM_THREADS={cpus_pt}\n"
                       "export OMP_PLACES=threads\n"
                       "export OMP_PROC_BIND=spread\n\n")

        script_txt += (
            f"srun -N {self.nodes} -n {self.nthreads} -c {cpus_pt} "
            f"--cpu_bind=cores LyaPowerxQmlExposure {self.config_file}\n")

        # remove error_logs
        o = self.qmle_settings['OutputDir']
        script_txt += utils.get_script_text_for_master_node(
            f"if [ $(ls -1 {o}/error_log*.txt 2>/dev/null | wc -l) -gt 0 ]; "
            f"then cat {o}/error_log*.txt > {o}/all_error_logs.txt && "
            f"rm {o}/error_log*.txt; fi")

        self.submitter_fname = utils.save_submit_script(
            script_txt, self.working_dir, self.jobname)

        print(f"QmleJob script is saved as {self.submitter_fname}.")

    def get_filelist_script_txt(self):
        script_txt = " && ".join([
            f"ls -1 delta-*.fits* | wc -l > fname_list_xe.txt",
            f"ls -1 delta-*.fits* >> fname_list_xe.txt",
        ])

        script_txt = (
            'if [[ ! -f "fname_list_xe.txt" ]]; then\n'
            f"    {script_txt}\n"
            "fi\n\n")

        return script_txt


class SQJob(LyspeqJob):
    def __init__(
            self, rootdir, outdelta_dir, sysopt,
            settings, section='qmle', jobname='sq-job'
    ):
        super().__init__(
            rootdir, outdelta_dir, sysopt, settings, section, jobname)

        self.config_file = "config-qmle-sq.txt"
        self.qmle_settings['OutputDir'] = self.qmle_settings['LookUpTableDir']
        self.abspath_outputdir = self.qmle_settings['LookUpTableDir']
        self.abspath_configfile = (
            f"{self.qmle_settings['OutputDir']}/{self.config_file}")

    def create_script(self, dep_jobid=None):
        time_txt = timedelta(minutes=15.)
        self.create_config()

        script_txt = utils.get_script_header(
            self.qmle_settings['LookUpTableDir'], self.jobname,
            time_txt, 1, self.queue)

        script_txt += f"{self.qmle_settings['env_command']}\n"
        script_txt += f"srun -N 1 -n 1 -c 2 CreateSQLookUpTable {self.abspath_configfile}\n"

        self.submitter_fname = utils.save_submit_script(
            script_txt, self.qmle_settings['LookUpTableDir'], self.jobname)

        print(f"SQJob script is saved as {self.submitter_fname}.")


class RegCovJob(Job):
    def __init__(self, rootdeltadir, lyspeq_job):
        self.name = "regbootcov"
        self.nodes = 1
        self.nthreads = 1
        self.time = 5.
        self.queue = "shared"
        self.batch = lyspeq_job.batch
        self.skip = lyspeq_job.skip
        self.rootdeltadir
        self.submitter_fname = None

        self.commands = []

    def addQmleJob(self, qmle_job):
        self.commands.append(f"cd {qmle_job.working_dir}\n")
        self.commands.extend(f"{qmle_job.get_bootstrap_commands()}\n")

    def create_directory(self, create_dir=None):
        pass

    def create_script(self):
        time_txt = timedelta(minutes=self.time * len(self.commands))
        raise NotImplementedError


class JobChain():
    def __init__(self, parentdir):
        self.parentdir = parentdir
        self.all_jobids = []
        self.held_jobids = []
        self.extra_commands = []

    def schedule_job(self, job, dep_jobid=None, hold=False):
        jobid = job.schedule(dep_jobid=dep_jobid, hold=hold)
        if jobid != -1:
            self.all_jobids.append(jobid)
            if hold:
                self.held_jobids.append(jobid)
        return jobid

    def releaseHeldJobs(self):
        if not self.held_jobids:
            print("No held jobs to release")
            return

        jobids_txt = ",".join([str(j) for j in self.held_jobids])
        command = f"scontrol release {jobids_txt}"
        print(utils.execute_command(command))

    def addExtraCommand(self, cmd):
        self.extra_commands.append(cmd)

    def submitExtraCommands(
            self, jobname="post-completion", queue="shared", time="00:30:00"
    ):
        if not self.extra_commands:
            return

        datestamp = datetime.today().strftime('%Y%m%d-%I%M%S%p')
        script_txt = utils.get_script_header(
            self.parentdir, jobname, time, 1, queue)
        script_txt += "\n\n".join(self.extra_commands) + '\n'

        submitter_fname = utils.save_submit_script(
            script_txt, self.parentdir, f"{jobname}-{datestamp}")

        if not self.all_jobids:
            return

        jobid = utils.submit_script(submitter_fname, self.all_jobids)
        if jobid != -1:
            self.all_jobids.append(jobid)

    def save_jobids(self):
        if not self.all_jobids:
            return

        jobids_txt = ",".join([str(j) for j in self.all_jobids])
        command = f"sacct -X -o JobID,JobName%30,State,Elapsed -j {jobids_txt}\n"
        datestamp = datetime.today().strftime('%Y%m%d-%I%M%S%p')
        with open(f'{self.parentdir}/jobids-{datestamp}.txt', 'w') as file:
            file.write(jobids_txt)

        script_txt = utils.get_script_header(
            self.parentdir, "summary-job", "00:02:00", 1, "shared")
        script_txt += command
        submitter_fname = utils.save_submit_script(
            script_txt, self.parentdir, f"summary-job-{datestamp}")
        utils.submit_script(submitter_fname, self.all_jobids, "afterany")


class MockJobChain(JobChain):
    def __init__(
            self, rootdir, realization, delta_dir, settings
    ):
        super().__init__(rootdir)

        self.sq_job = None
        self.sq_jobid = -1
        self.dla_bal_combos = None
        self.settings = settings
        self.delta_dir = delta_dir

        self.tr_job = OhioTransmissionsJob(rootdir, realization, settings)
        self.qq_job = OhioQuickquasarsJob(rootdir, realization, settings)

        key = ""

        if self.qq_job.dla:
            self.settings.set(
                "qsonic", "dla-mask", f"{self.qq_job.desibase_dir}/dla_cat.fits")
            key += "-dla"

        if self.qq_job.bal > 0:
            key += "-bal"
            # settings.set("qsonic", "bal-mask", combo[1])

        if not key:
            key = "-nosyst"

        qsonic_job = QSOnicMockJob(
            self.delta_dir,
            self.qq_job.interm_path, self.qq_job.desibase_dir,
            self.qq_job.foldername, realization, self.settings
        )

        qmle_job = QmleJob(
            self.delta_dir, qsonic_job.outdelta_dir, self.qq_job.sysopt,
            self.settings, jobname=f"qmle{key}-{realization}")

        self.qsonic_qmle_job = [qsonic_job, qmle_job]

        if not self.sq_job and qmle_job.needs_sqjob():
            self.sq_job = SQJob(
                self.delta_dir, qsonic_job.outdelta_dir, self.qq_job.sysopt,
                self.settings, jobname="sq-job")

    def setup(self):
        self.tr_job.setup()
        self.qq_job.setup()

        qsonic_job, qmle_job = self.qsonic_qmle_job
        qsonic_job.setup()
        qmle_job.setup()

        if self.sq_job:
            self.sq_job.setup()

    def schedule(self):
        self.setup()

        if self.sq_job:
            self.sq_jobid = self.schedule_job(self.sq_job)
            self.sq_job = None

        jobid = self.schedule_job(self.tr_job)
        jobid = self.schedule_job(self.qq_job, jobid)

        if jobid != -1:
            self.addExtraCommand(self.qq_job.getQqCatalogCommand())

        qsonic_job, qmle_job = self.qsonic_qmle_job
        jobid = self.schedule_job(qsonic_job, jobid)
        jobid = self.schedule_job(qmle_job, [jobid, self.sq_jobid])

        if jobid != -1:
            self.addExtraCommand(qmle_job.get_bootstrap_commands())

    def inc_realization(self, is_last):
        if is_last:
            return

        self.tr_job.inc_realization()
        self.qq_job.inc_realization()
        if self.qq_job.dla:
            self.settings.set(
                "qsonic", "dla-mask", f"{self.qq_job.desibase_dir}/dla_cat.fits")

        qsonic_job, qmle_job = self.qsonic_qmle_job
        qsonic_job.inc_realization(self.qq_job.interm_path, self.qq_job.desibase_dir)

        qmlejobname = qmle_job.jobname.split('-')[:2]
        qmlejobname[-1] = str(qsonic_job.realization)
        qmlejobname = '-'.join(qmlejobname)
        self.qsonic_qmle_job[1] = QmleJob(
            self.delta_dir, qsonic_job.outdelta_dir,
            self.qq_job.sysopt, self.settings, jobname=qmlejobname)


class DataJobChain(JobChain):
    def __init__(self, delta_dir, settings):
        super().__init__(delta_dir)

        desi_settings = settings['desi']
        qsonic_sections = [
            x for x in settings.sections() if x.startswith("qsonic.")]

        self.qsonic_jobs = {}
        self.qmle_jobs = {}
        self.sq_jobs = {}
        for qsection in qsonic_sections:
            forest = qsection[len("qsonic."):]

            self.qsonic_jobs[forest] = QSOnicDataJob(
                delta_dir, forest, desi_settings, settings, qsection)

            if self.qsonic_jobs[forest].exposures:
                WhichQmleJob = XeQmleJob
            else:
                WhichQmleJob = QmleJob

            self.qmle_jobs[forest] = WhichQmleJob(
                delta_dir, self.qsonic_jobs[forest].outdelta_dir,
                sysopt=None, settings=settings, section=f"qmle.{forest}",
                jobname=f"qmle-{forest}")

            # Treat all SBs the same
            sq_key = forest[:2]
            if sq_key not in self.sq_jobs and self.qmle_jobs[forest].needs_sqjob():
                self.sq_jobs[sq_key] = SQJob(
                    delta_dir, self.qsonic_jobs[forest].outdelta_dir,
                    sysopt=None, settings=settings, section=f"qmle.{forest}",
                    jobname=f"sq-job-{forest}")

    def setup(self):
        for job in self.qsonic_jobs.values():
            job.setup()
        for job in self.qmle_jobs.values():
            job.setup()
        for job in self.sq_jobs.values():
            job.setup()

    def schedule(self, keys_to_run=[]):
        sq_jobids = {}
        last_qsonic_jobid = None

        self.setup()

        # Make sure LyaCalib runs last
        forests = list(self.qsonic_jobs.keys())

        if keys_to_run:
            keys_to_run = set(keys_to_run)
            assert all(_ in forests for _ in keys_to_run)

            forests = list(keys_to_run)

        forests.sort()

        for forest in forests:
            qsonic_job = self.qsonic_jobs[forest]
            qmle_job = self.qmle_jobs[forest]

            last_qsonic_jobid = self.schedule_job(qsonic_job, last_qsonic_jobid)

            sq_key = forest[:2]
            sq_job = self.sq_jobs.pop(sq_key, None)
            if sq_job:
                sq_jobids[sq_key] = self.schedule_job(sq_job)

            jobid_sq = sq_jobids.get(sq_key, -1)

            jobid = self.schedule_job(qmle_job, [last_qsonic_jobid, jobid_sq])
            if jobid != -1:
                self.addExtraCommand(qmle_job.get_bootstrap_commands())

        self.submitExtraCommands(jobname="bootstrap")


class DataSplitJobChain(JobChain):
    def __init__(self, delta_dir, settings):
        super().__init__(delta_dir)

        desi_settings = settings['desi']
        catalog_base = desi_settings['catalog']
        nsplits = desi_settings.getint('number of splits')
        qsonic_sections = [x for x in settings.sections() if x.startswith("qsonic.")]

        self.qsonic_jobs = {}
        self.qmle_jobs = {}
        self.sq_jobs = {}
        for qsection in qsonic_sections:
            forest = qsection[len("qsonic."):]
            calibfile_base = settings[qsection].get("calibration")

            any_needs_sqjob = False
            for ii in range(nsplits):
                key = f"{forest}-s{ii}"
                desi_settings['catalog'] = f"{catalog_base}{ii}.fits"
                if calibfile_base:
                    settings[qsection]['calibration'] = f"{calibfile_base}{ii}.fits"

                self.qsonic_jobs[key] = QSOnicDataJob(
                    delta_dir, forest, desi_settings, settings, qsection)
                self.qmle_jobs[key] = QmleJob(
                    delta_dir, self.qsonic_jobs[key].outdelta_dir,
                    sysopt=None, settings=settings, section=f"qmle.{forest}",
                    jobname=f"qmle-{key}")
                any_needs_sqjob |= self.qmle_jobs[key].needs_sqjob()

            # Treat all SBs the same
            sq_key = forest[:2]
            if sq_key not in self.sq_jobs and any_needs_sqjob:
                key = f"{forest}-s0"
                self.sq_jobs[sq_key] = SQJob(
                    delta_dir, self.qsonic_jobs[key].outdelta_dir,
                    sysopt=None, settings=settings, section=f"qmle.{forest}",
                    jobname=f"sq-job-{forest}")
                self.sq_jobs[sq_key].setup()

    def setup(self):
        for job in self.qsonic_jobs.values():
            job.setup()
        for job in self.qmle_jobs.values():
            job.setup()
        for job in self.sq_jobs.values():
            job.setup()

    def schedule(self, keys_to_run=[], i1=0):
        sq_jobids = {}
        last_qsonic_jobid = None
        self.setup()

        keys = list(self.qsonic_jobs.keys())

        if keys_to_run:
            keys_to_run = set(keys_to_run)
            assert all(_ in keys for _ in keys_to_run)

            keys = list(keys_to_run)

        if i1 > 0:
            keys = [_ for _ in keys if int(_.split('-s')[-1]) >= i1]

        for key in keys:
            qsonic_job = self.qsonic_jobs[key]
            qmle_job = self.qmle_jobs[key]

            last_qsonic_jobid = self.schedule_job(qsonic_job, last_qsonic_jobid)
            if last_qsonic_jobid != -1:
                self.addExtraCommand(qsonic_job.getfitAmplifierRegionsCommands())

            sq_key = key[:2]
            sq_job = self.sq_jobs.pop(sq_key, None)
            if sq_job:
                sq_jobids[sq_key] = self.schedule_job(sq_job, hold=True)

            jobid_sq = sq_jobids.get(sq_key, -1)

            jobid = self.schedule_job(qmle_job, [last_qsonic_jobid, jobid_sq])
            if jobid != -1:
                self.addExtraCommand(qmle_job.get_bootstrap_commands())

        self.releaseHeldJobs()
        self.submitExtraCommands(jobname="fit-amps")
