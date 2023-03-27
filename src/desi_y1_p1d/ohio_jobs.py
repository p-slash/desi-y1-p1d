from os import makedirs
from os.path import join as ospath_join
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
        self.desibase_dir = ospath_join(
            self.rootdir, self.interm_path, self.foldername)
        self.transmissions_dir = ospath_join(
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

        command = (f"srun -N 1 -n 1 -c {self.nthreads} "
                   f"quickquasars -i \\$tfiles --nproc {self.nthreads} "
                   f"--outdir {self.desibase_dir}/spectra-16 {self.OPTS_QQ}")

        script_txt = utils.get_script_header(
            self.desibase_dir, f"ohio-qq-y1-{self.realization}",
            time_txt, self.nodes, self.queue)

        script_txt += 'echo "get list of skewers to run ..."\n\n'

        script_txt += f"files=\\`ls -1 {self.transmissions_dir}/*/*/lya-transmission*.fits*\\`\n"
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
        script_txt += f'    echo "log in {self.desibase_dir}/logs/node-\\$node.log"\n\n'

        script_txt += f"    \\$command >& {self.desibase_dir}/logs/node-\\$node.log &\n"
        script_txt += "done\n\n"

        script_txt += "wait\n"
        script_txt += "echo 'END'\n\n"

        command = (f"desi_zcatalog -i {self.desibase_dir}/spectra-16 "
                   f"-o {self.desibase_dir}/zcat.fits --minimal --prefix zbest\n")

        if self.dla:
            command += (f"get-qq-true-dla-catalog {self.desibase_dir}/spectra-16 "
                        f"{self.desibase_dir} --nproc {self.nthreads}\n")

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

        self.transmissions_dir = ospath_join(
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
        script_txt += (f"newGenDESILiteMocks.py {self.transmissions_dir} "
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
        self.coadd_arms = qsonic_settings['coadd_arms']
        self.skip_resomat = qsonic_settings['skip_resomat']
        self.dla = qsonic_settings.get("dla-mask", "")
        self.bal = qsonic_settings['bal-mask']
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

        script_txt += f"srun -N {self.nodes} -n {self.nthreads} -c 2 qsonic-fit \\\\\n"
        script_txt += f"-i {self.indir} \\\\\n"
        script_txt += f"--catalog {self.catalog} \\\\\n"
        script_txt += f"-o {self.outdelta_dir} \\\\\n"
        script_txt += f"--rfdwave 0.8 --skip 0.2 \\\\\n"
        script_txt += f"--no-iterations 20 \\\\\n"
        script_txt += f"--cont-order {self.cont_order} \\\\\n"
        script_txt += f"--wave1 {self.wave1} --wave2 {self.wave2} \\\\\n"
        script_txt += f"--forest-w1 {self.forest_w1} --forest-w2 {self.forest_w2}"

        if self.is_mock:
            script_txt += f" \\\\\n--mock-analysis"
        if self.dla:
            script_txt += f" \\\\\n--dla-mask {self.dla}"
        if self.bal:
            script_txt += " \\\\\n--bal-mask"
        if self.sky:
            script_txt += f" \\\\\n--sky-mask {self.sky}"
        if self.coadd_arms:
            script_txt += " \\\\\n--coadd-arms"
        if self.skip_resomat:
            script_txt += " \\\\\n--skip-resomat"

        script_txt += "\n\n"

        script_txt += f"srun -N {self.nodes} -n {self.nthreads} -c 2 qsonic-calib \\\\\n"
        script_txt += f"-i {self.outdelta_dir} \\\\\n"
        script_txt += f"-o {self.outdelta_dir}/var_stats \\\\\n"
        script_txt += f"--wave1 {self.wave1} --wave2 {self.wave2} \\\\\n"
        script_txt += f"--forest-w1 {self.forest_w1} --forest-w2 {self.forest_w2}"
        script_txt += "\n\n"

        script_txt += f"getLists4QMLEfromPICCA.py {self.outdelta_dir} --nproc 128\n"
        script_txt += f"getLists4QMLEfromPICCA.py {self.outdelta_dir} --nproc 128 --snr-cut 1\n"
        script_txt += f"getLists4QMLEfromPICCA.py {self.outdelta_dir} --nproc 128 --snr-cut 2\n"

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.outdelta_dir, "qsonic-fit", dep_jobid=dep_jobid)

        print(f"QSOnic script is saved as {self.submitter_fname}.")

        return self.submitter_fname


class QSOnicMockJob(QSOnicJob):
    def __init__(
            self, rootdir, interm_path, desibase_dir, foldername,
            realization, settings, section
    ):
        super().__init__(rootdir, foldername, True, settings, section)
        self.interm_path = interm_path
        self.desibase_dir = desibase_dir
        self.realization = realization

        self._set_paths()
        self.submitter_fname = None

    def _set_paths(self):
        self.indir = f"{self.desibase_dir}/spectra-16"
        self.catalog = f"{self.desibase_dir}/zcat.fits"
        if self.rootdir:
            self.outdelta_dir = ospath_join(
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
        self.outdelta_dir = ospath_join(
            self.rootdir, self.interm_path, self.foldername, f"Delta{self.suffix}")

        self.submitter_fname = None


class QmleJob(Job):
    """ To be implemented."""

    def __init__(self):
        pass


class MockJobChain():
    def __init__(
            self, rootdir, realization, delta_dir, settings
    ):
        self.tr_job = OhioTransmissionsJob(rootdir, realization, settings)

        self.qq_job = OhioQuickquasarsJob(rootdir, realization, settings)

        self.qsonic_job = QSOnicMockJob(
            delta_dir,
            self.qq_job.interm_path, self.qq_job.desibase_dir, self.qq_job.foldername,
            realization,
            settings, 'qsonic'
        )

    def schedule(self):
        jobid = -1

        jobid = self.tr_job.schedule()
        jobid = self.qq_job.schedule(jobid)
        jobid = self.qsonic_job.schedule(jobid)

    def inc_realization(self):
        self.tr_job.inc_realization()
        self.qq_job.inc_realization()
        self.qsonic_job.inc_realization(self.qq_job.interm_path, self.qq_job.desibase_dir)


class DataJobChain():
    def __init__(self, delta_dir, settings):
        desi_settings = settings['desi']
        qsonic_sections = [x for x in settings.sections() if x.startswith("qsonic.")]

        self.qsonic_jobs = {}
        self.qmle_jobs = {}
        for forest in qsonic_sections:
            self.qsonic_jobs[forest] = QSOnicDataJob(
                delta_dir, forest, desi_settings, settings, forest)
            self.qmle_jobs[forest] = QmleJob()

    def schedule(self):
        for forest, qsonic_job in self.qsonic_jobs.items():
            jobid = qsonic_job.schedule()

            qmle_job = self.qmle_jobs[forest]
            qmle_job.schedule(dep_jobid=jobid)
