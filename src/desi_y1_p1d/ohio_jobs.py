from os import makedirs
import os.path
import glob
from datetime import timedelta

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

    def create_directory(self, create_dir=None):
        raise NotImplementedError

    def create_script(self, dep_jobid=None):
        raise NotImplementedError

    def schedule(self, dep_jobid=None):
        print(f"Setting up a {self.name} job...")
        self.create_directory(not self.skip)

        if self.skip:
            self.submitter_fname = None
        else:
            self.create_script(dep_jobid)

        jobid = -1
        if self.batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)
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

        self.exptime = f"{self.nexp}000"
        self.seed = f"{self.seed_base}{self.realization}"
        self.set_sysopt()

        self.foldername = f"desi-{self.version[1]}.{self.sysopt}-{self.nexp}{self.suffix}"
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
                   f" --exptime {self.exptime} --save-continuum"
                   f" --save-continuum-dwave {self.cont_dwave}")

        if self.dla:
            sysopt += "1"
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

        self.sysopt = sysopt
        self.OPTS_QQ = OPTS_QQ

    def create_directory(self, create_dir=True):
        if not create_dir:
            return self.desibase_dir

        # make directories to store logs and spectra
        print("Creating directories:")
        print(f"+ {self.desibase_dir}")
        print(f"+ {self.desibase_dir}/logs")
        print(f"+ {self.desibase_dir}/spectra-16")

        makedirs(self.desibase_dir, exist_ok=True)
        makedirs(f"{self.desibase_dir}/logs", exist_ok=True)
        makedirs(f"{self.desibase_dir}/spectra-16", exist_ok=True)

        return self.desibase_dir

    def create_script(self, dep_jobid=None):
        """ Creates and writes the script for quickquasars run. Sets self.submitter_fname.

        Args:
            dep_jobid (int): Dependent JobID. Defaults to None.

        Returns:
            submitter_fname (str): Filename of the submitter script.
        """
        time_txt = timedelta(minutes=self.time)

        script_txt = utils.get_script_header(
            self.desibase_dir, f"ohio-qq-y1-{self.realization}",
            time_txt, self.nodes, self.queue)

        relpath_to_tr = os.path.relpath(self.desibase_dir, self.transmissions_dir)

        command = (f"srun -N 1 -n 1 -c {self.nthreads} "
                   f"quickquasars -i \\$tfiles --nproc {self.nthreads} "
                   f"--outdir {relpath_to_tr}/spectra-16 {self.OPTS_QQ}")

        script_txt += '# Change directory...\n'
        script_txt += f'cd {self.transmissions_dir}\n'
        script_txt += 'echo "get list of skewers to run ..."\n\n'

        script_txt += f"files=\\`ls -1 ./*/*/lya-transmission*.fits*\\`\n"
        script_txt += "nfiles=\\`echo \\$files | wc -w\\`\n"
        script_txt += f"nfilespernode=\\$(( \\$nfiles/{self.nodes} + 1))\n\n"

        script_txt += 'echo "n files =" \\$nfiles\n'
        script_txt += 'echo "n files per node =" \\$nfilespernode\n\n'

        script_txt += "first=1\n"
        script_txt += "last=\\$nfilespernode\n"
        script_txt += f"for node in \\`seq {self.nodes}\\` ; do\n"
        script_txt += "    echo 'starting node \\$node'\n\n"

        script_txt += "    # list of files to run\n"
        script_txt += f"    if (( \\$node == {self.nodes} )) ; then\n"
        script_txt += "        last=""\n"
        script_txt += "    fi\n"
        script_txt += "    echo \\${first}-\\${last}\n"
        script_txt += '    tfiles=\\`echo \\$files | cut -d " " -f \\${first}-\\${last}\\`\n'
        script_txt += "    first=\\$(( \\$first + \\$nfilespernode ))\n"
        script_txt += "    last=\\$(( \\$last + \\$nfilespernode ))\n"
        script_txt += f'    command="{command}"\n\n'

        script_txt += "    echo \\$command\n"
        script_txt += f'    echo "log in {relpath_to_tr}/logs/node-\\$node.log"\n\n'

        script_txt += f"    \\$command >& {relpath_to_tr}/logs/node-\\$node.log &\n"
        script_txt += "done\n\n"

        script_txt += "wait\n"
        script_txt += "echo 'END'\n\n"

        command = (f"desi_zcatalog -i {relpath_to_tr}/spectra-16 "
                   f"-o {relpath_to_tr}/zcat.fits --minimal --prefix zbest\n")

        if self.dla:
            command += (f"get-qq-true-dla-catalog {relpath_to_tr}/spectra-16 "
                        f"{relpath_to_tr} --nproc {self.nthreads}\n")

        script_txt += utils.get_script_text_for_master_node(command)

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.desibase_dir, "quickquasars",
            env_command=self.env_command,
            dep_jobid=dep_jobid)

        print(f"Quickquasars script is saved as {self.submitter_fname}.")

        return self.submitter_fname


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
            print("Creating directories:")
            print(f"+ {self.transmissions_dir}")
            makedirs(self.transmissions_dir, exist_ok=True)

        return self.transmissions_dir

    def create_script(self, dep_jobid=None):
        """ Creates and writes the script for newGenDESILiteMocks run.
        Sets self.submitter_fname.

        Args:
            dep_jobid (int): Dependent JobID. Defaults to None.

        Returns:
            submitter_fname (str): Filename of the submitter script.
        """
        time_txt = timedelta(minutes=self.time)

        script_txt = utils.get_script_header(
            self.transmissions_dir, f"ohio-trans-y1-{self.realization}",
            time_txt, nodes=self.nodes, queue=self.queue)

        script_txt += 'echo "Generating transmission files using qsotools."\n\n'
        script_txt += (f"newGenDESILiteMocks {self.transmissions_dir} "
                       f"--master-file {self.catalog} --save-qqfile --nproc {self.nthreads} "
                       f"--seed {self.seed}\n")

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.transmissions_dir, f"gen-trans-{self.realization}",
            dep_jobid=dep_jobid)

        print(f"newGenDESILiteMocks script is saved as {self.submitter_fname}.")

        return self.submitter_fname


class QSOnicJob(Job):
    def __init__(self, rootdir, foldername, is_mock, settings, section):
        super().__init__(settings, section)
        qsonic_settings = settings[section]
        self.rootdir = rootdir
        self.foldername = foldername
        self.is_mock = is_mock

        self.wave1 = qsonic_settings['wave1']
        self.wave2 = qsonic_settings['wave2']
        self.forest_w1 = qsonic_settings['forest_w1']
        self.forest_w2 = qsonic_settings['forest_w2']
        self.cont_order = qsonic_settings['cont_order']
        self.coadd_arms = qsonic_settings.getboolean('coadd_arms', fallback=False)
        self.skip_resomat = qsonic_settings.getboolean('skip_resomat', fallback=False)
        self.dla = qsonic_settings.get("dla-mask", "")
        self.bal = qsonic_settings.getboolean('bal-mask', fallback=False)
        self.sky = qsonic_settings['sky-mask']

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
            print("Creating directory:")
            print(f"+ {self.outdelta_dir}")

            makedirs(self.outdelta_dir, exist_ok=True)

        return self.outdelta_dir

    def create_script(self, dep_jobid=None):
        """ Creates and writes the script for QSOnic run.
        Sets self.submitter_fname.

        Args:
            dep_jobid (int): Dependent JobID. Defaults to None.

        Returns:
            submitter_fname (str): Filename of the submitter script.
        """
        if self.outdelta_dir is None:
            return None

        time_txt = timedelta(minutes=self.time)
        script_txt = utils.get_script_header(
            self.outdelta_dir, f"qsonic-{self.foldername}", time_txt, self.nodes, self.queue)

        commands = []

        qsonic_command = (
            f"srun -N {self.nodes} -n {self.nthreads} -c 2 qsonic-fit \\\\\n"
            f"-i {self.indir} \\\\\n"
            f"--catalog {self.catalog} \\\\\n"
            f"-o {self.outdelta_dir} \\\\\n"
            f"--rfdwave 0.8 --skip 0.2 \\\\\n"
            f"--no-iterations 20 \\\\\n"
            f"--cont-order {self.cont_order} \\\\\n"
            f"--wave1 {self.wave1} --wave2 {self.wave2} \\\\\n"
            f"--forest-w1 {self.forest_w1} --forest-w2 {self.forest_w2}")

        if self.is_mock:
            qsonic_command += f" \\\\\n--mock-analysis"
        if self.dla:
            qsonic_command += f" \\\\\n--dla-mask {self.dla}"
        if self.bal:
            qsonic_command += " \\\\\n--bal-mask"
        if self.sky:
            qsonic_command += f" \\\\\n--sky-mask {self.sky}"
        if self.coadd_arms:
            qsonic_command += " \\\\\n--coadd-arms"
        if self.skip_resomat:
            qsonic_command += " \\\\\n--skip-resomat"

        commands.append(qsonic_command)
        commands.append(f"cd {self.outdelta_dir}")
        commands.append(
            f"srun -N {self.nodes} -n {self.nthreads} -c 2 qsonic-calib \\\\\n"
            f"-i . -o ./var_stats \\\\\n"
            f"--wave1 {self.wave1} --wave2 {self.wave2}")

        commands.append(f"getLists4QMLEfromPICCA . --nproc 128")
        commands.append(f"getLists4QMLEfromPICCA . --nproc 128 --snr-cut 1")
        commands.append(f"getLists4QMLEfromPICCA . --nproc 128 --snr-cut 2")

        script_txt += " \\\\\n&& ".join(commands) + '\n'

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.outdelta_dir, "qsonic-fit", dep_jobid=dep_jobid)

        print(f"QSOnic script is saved as {self.submitter_fname}.")

        return self.submitter_fname


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

    def _set_paths(self):
        self.indir = f"{self.desibase_dir}/spectra-16"
        self.catalog = f"{self.desibase_dir}/zcat.fits"
        if self.rootdir:
            self.outdelta_dir = os.path.join(
                self.rootdir, self.interm_path, self.foldername, f"Delta{self.suffix}")
        else:
            self.outdelta_dir = None

    def inc_realization(self, new_interm_path, new_desibase_dir):
        self.realization += 1
        self.interm_path = new_interm_path
        self.desibase_dir = new_desibase_dir

        self._set_paths()
        self.submitter_fname = None


class QSOnicDataJob(QSOnicJob):
    def __init__(self, rootdir, forest, desi_settings, settings, section):
        super().__init__(rootdir, forest, False, settings, section)

        self.catalog = desi_settings['catalog']

        self.indir = f"{desi_settings['redux']}/{desi_settings['release']}/healpix"
        self.interm_path = utils.get_folder_structure_data(
            desi_settings['release'], desi_settings['survey'], self.catalog)
        self.outdelta_dir = os.path.join(
            self.rootdir, self.interm_path, self.foldername, f"Delta{self.suffix}")

        self.submitter_fname = None


class LyspeqJob(Job):
    def __init__(
            self, rootdir, outdelta_dir, sysopt,
            settings, section='qmle'
    ):
        super().__init__(settings, section)
        sb_suff = "-sb" if "SB" in section else ""
        self.qmle_settings = dict(settings[section])
        self.qmle_settings['FileInputDir'] = outdelta_dir
        # Filenames should relative to outdelta_dir
        self.qmle_settings['FileNameList'] = f"{outdelta_dir}/fname_list.txt"
        self.qmle_settings['OutputDir'] = os.path.abspath(f"{outdelta_dir}/../results/")
        self.qmle_settings['LookUpTableDir'] = f"{rootdir}/lookuptables{sb_suff}"
        self.qmle_settings['FileNameRList'] = f"{rootdir}/specres_list-rmat.txt"
        self.qmle_settings['OutputFileBase'] = self._get_output_fbase(settings, sysopt)

        self.config_file = os.path.abspath(
            f"{outdelta_dir}/../config-qmle-{self.qmle_settings['OutputFileBase']}.txt")

        frname = self.qmle_settings['FileNameRList']
        if not os.path.exists(frname):
            with open(frname, 'w') as frfile:
                frfile.write("1\n")
                frfile.write("1000000 0.1\n")

    def _get_output_fbase(self, settings, sysopt):
        if "ohio" in settings.sections():
            release = settings['ohio']['release']
            fbase = f"mock-{release}-{sysopt}"
        else:
            release = settings['desi']['release']
            fbase = f"desi-{release}"

        oversample = int(self.qmle_settings.get('OversampleRmat', 0))
        deconvolve = float(self.qmle_settings.get('ResoMatDeconvolutionM', 0))
        nchunks = int(self.qmle_settings.get('DynamicChunkNumber', 0))

        if oversample > 0:
            fbase += f"-o{oversample}"
        if deconvolve > 0:
            fbase += f"-dc{deconvolve:.2f}"
        if nchunks > 0:
            fbase += f"-n{nchunks}"

        return fbase

    def create_directory(self, create_dir=True):
        if not create_dir:
            return

        # make directories to store outputs and lookup tables
        print("Creating directories:")
        print(f"+ {self.qmle_settings['OutputDir']}")
        print(f"+ {self.qmle_settings['LookUpTableDir']}")

        makedirs(self.qmle_settings['OutputDir'], exist_ok=True)
        makedirs(self.qmle_settings['LookUpTableDir'], exist_ok=True)

    def create_config(self):
        if self.skip:
            return

        omitted_keys = [
            "nodes", "nthreads", "batch", "skip", "time", "queue", "env_command",
            "number_of_bootstraps", "boot_seed"
        ]
        config_lines = [
            f"{key} {value}\n"
            for key, value in self.qmle_settings.items()
            if key not in omitted_keys
        ]
        config_txt = ''.join(config_lines)
        with open(self.config_file, 'w') as f:
            f.write(config_txt)

        print(f"LyspeqJob config is saved as {self.config_file}.")


class QmleJob(LyspeqJob):
    def get_bootstrap_commands(self):
        save_boots = int(self.qmle_settings['SaveEachProcessResult']) > 0
        if not save_boots:
            return []

        nboots = int(self.qmle_settings["number_of_bootstraps"])
        boot_seed = self.qmle_settings["boot_seed"]
        if nboots <= 0:
            return []

        inbootfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{self.qmle_settings['OutputFileBase']}-bootresults.dat")

        commands = []
        commands.append(
            f"bootstrapQMLE {inbootfile} --bootnum {nboots} "
            f"--fbase {self.qmle_settings['OutputFileBase']} "
            f"--seed {boot_seed}")

        outcovfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{self.qmle_settings['OutputFileBase']}-bootstrap-cov-n{nboots}-s{boot_seed}.txt")
        infisherfile = os.path.join(
            self.qmle_settings['OutputDir'],
            f"{self.qmle_settings['OutputFileBase']}_it1_fisher_matrix.txt")

        commands.append(
            f"regularizeBootstrapCov --boot-cov {outcovfile} "
            f"--qmle-fisher {infisherfile} "
            f"--qmle-sparcity-cut 0.001 --reg-in-cov "
            f"--fbase {self.qmle_settings['OutputFileBase']}-")

        return commands

    def create_script(self, dep_jobid=None):
        self.create_config()

        time_txt = timedelta(minutes=self.time)

        name = f"qmle-{self.qmle_settings['OutputFileBase']}"
        oneup_dir = os.path.abspath(f"{self.qmle_settings['OutputDir']}/../")

        script_txt = utils.get_script_header(
            self.qmle_settings['OutputDir'], name,
            time_txt, self.nodes, self.queue)

        commands = []

        commands.append(f"srun -N {self.nodes} -n {self.nthreads} -c 2 "
                        f"LyaPowerEstimate {self.config_file}")
        commands.extend(self.get_bootstrap_commands())

        script_txt += " \\\\\n&& ".join(commands) + '\n'

        self.submitter_fname = utils.save_submitter_script(
            script_txt, oneup_dir, name,
            env_command=self.qmle_settings['env_command'],
            dep_jobid=dep_jobid)

        print(f"QmleJob script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def needs_sqjob(self):
        files_exits = True
        files_exits &= os.path.exists(
            f"{self.qmle_settings['LookUpTableDir']}/signal_R1000000_dv0.1.dat")

        deriv_files = glob.glob(
            f"{self.qmle_settings['LookUpTableDir']}/deriv_R1000000_dv0.1*.dat")
        expected_nk = (
            int(self.qmle_settings.get("NumberOfLinearBins"))
            + int(self.qmle_settings.get("NumberOfLog10Bins")))
        files_exits &= len(deriv_files) == expected_nk

        return not files_exits


class SQJob(LyspeqJob):
    def create_script(self, dep_jobid=None):
        # self.create_config()

        time_txt = timedelta(minutes=5.)

        script_txt = utils.get_script_header(
            self.qmle_settings['LookUpTableDir'], "sq-job",
            time_txt, 1, self.queue)

        script_txt += f"srun -N 1 -n 1 -c 2 CreateSQLookUpTable {self.config_file}\n"

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.qmle_settings['LookUpTableDir'], "sq-job",
            env_command=self.qmle_settings['env_command'],
            dep_jobid=dep_jobid)

        print(f"SQJob script is saved as {self.submitter_fname}.")

        return self.submitter_fname


class MockJobChain():
    def __init__(
            self, rootdir, realization, delta_dir, settings
    ):
        self.settings = settings
        self.tr_job = OhioTransmissionsJob(rootdir, realization, settings)

        self.qq_job = OhioQuickquasarsJob(rootdir, realization, settings)

        self.qsonic_job = QSOnicMockJob(
            delta_dir,
            self.qq_job.interm_path, self.qq_job.desibase_dir, self.qq_job.foldername,
            realization,
            settings
        )

        self.qmle_job = QmleJob(
            rootdir, self.qsonic_job.outdelta_dir,
            self.qq_job.sysopt, settings)

        if self.qmle_job.needs_sqjob():
            self.sq_job = SQJob(
                rootdir, self.qsonic_job.outdelta_dir, None, settings)
        else:
            self.sq_job = None

    def schedule(self):
        jobid = -1
        sq_jobid = -1
        if self.sq_job:
            sq_jobid = self.sq_job.schedule()
            self.sq_job = None

        jobid = self.tr_job.schedule()
        jobid = self.qq_job.schedule(jobid)

        jobid = self.qsonic_job.schedule(jobid)
        jobid = self.qmle_job.schedule([jobid, sq_jobid])

    def inc_realization(self):
        self.tr_job.inc_realization()
        self.qq_job.inc_realization()
        self.qsonic_job.inc_realization(self.qq_job.interm_path, self.qq_job.desibase_dir)
        self.qmle_job = QmleJob(
            self.qq_job.rootdir, self.qsonic_job.outdelta_dir,
            self.qq_job.sysopt, self.settings)


class DataJobChain():
    def __init__(self, rootdir, delta_dir, settings):
        desi_settings = settings['desi']
        qsonic_sections = [x for x in settings.sections() if x.startswith("qsonic.")]

        self.qsonic_jobs = {}
        self.qmle_jobs = {}
        self.sq_jobs = {}
        for qsection in qsonic_sections:
            forest = qsection[len("qsonic."):]

            self.qsonic_jobs[forest] = QSOnicDataJob(
                delta_dir, forest, desi_settings, settings, qsection)
            self.qmle_jobs[forest] = QmleJob(
                rootdir, self.qsonic_jobs[forest].outdelta_dir,
                sysopt=None, settings=settings, section=f"qmle.{forest}")

            # Treat all SBs the same
            sq_key = forest[:-1]
            if sq_key not in self.sq_jobs and self.qmle_jobs[forest].needs_sqjob():
                self.sq_jobs[sq_key] = SQJob(
                    rootdir, self.qsonic_jobs[forest].outdelta_dir,
                    sysopt=None, settings=settings, section=f"qmle.{forest}")

    def schedule(self):
        sq_jobids = {}
        last_qsonic_jobid = None

        for forest, qsonic_job in self.qsonic_jobs.items():
            last_qsonic_jobid = qsonic_job.schedule(dep_jobid=last_qsonic_jobid)

            sq_key = forest[:-1]
            sq_job = self.sq_jobs.pop(sq_key, None)
            if sq_job:
                sq_jobids[sq_key] = sq_job.schedule()

            jobid_sq = sq_jobids.get(sq_key, -1)
            jobids = [last_qsonic_jobid, jobid_sq]

            self.qmle_jobs[forest].schedule(dep_jobid=jobids)
