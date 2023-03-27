from os import makedirs
from os.path import join as ospath_join
from datetime import timedelta

from desi_y1_p1d import utils


class OhioQuickquasarsJob():
    def __init__(self, rootdir, realization, settings):
        self.rootdir = rootdir
        self.realization = realization

        self.version = settings['ohio']['version']
        self.release = settings['ohio']['release']
        self.survey = settings['ohio']['survey']
        self.catalog = settings['ohio']['catalog']

        self.nexp = settings['quickquasars']['nexp']
        self.zmin_qq = settings['quickquasars']['zmin_qq']
        self.cont_dwave = settings['quickquasars']['cont_dwave']
        self.dla = settings['quickquasars']['dla']
        self.bal = settings['quickquasars']['bal']
        self.boring = settings['quickquasars']['boring']
        self.seed_base = settings['quickquasars']['base_seed']
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

    def create_script(self, nodes, nthreads, time, queue="regular", dep_jobid=None):
        """ Creates and writes the script for quickquasars run. Sets self.submitter_fname.

        Args:
            nodes (int): Number of nodes.
            nthreads (int): Number of threads.
            time (float): Time need in hours.
            dep_jobid (int): Dependent JobID. Defaults to None.

        Returns:
            submitter_fname (str): Filename of the submitter script.
        """
        time_txt = timedelta(hours=time)

        command = (f"srun -N 1 -n 1 -c {nthreads} "
                   f"quickquasars -i \\$tfiles --nproc {nthreads} "
                   f"--outdir {self.desibase_dir}/spectra-16 {self.OPTS_QQ}")

        script_txt = utils.get_script_header(
            self.desibase_dir, f"ohio-qq-y1-{self.realization}", time_txt, nodes, queue)

        script_txt += 'echo "get list of skewers to run ..."\n\n'

        script_txt += f"files=\\`ls -1 {self.transmissions_dir}/*/*/lya-transmission*.fits*\\`\n"
        script_txt += "nfiles=\\`echo \\$files | wc -w\\`\n"
        script_txt += f"nfilespernode=\\$(( \\$nfiles/{nodes} + 1))\n\n"

        script_txt += 'echo "n files =" \\$nfiles\n'
        script_txt += 'echo "n files per node =" \\$nfilespernode\n\n'

        script_txt += "first=1\n"
        script_txt += "last=\\$nfilespernode\n"
        script_txt += f"for node in \\`seq {nodes}\\` ; do\n"
        script_txt += "    echo 'starting node \\$node'\n\n"

        script_txt += "    # list of files to run\n"
        script_txt += f"    if (( \\$node == {nodes} )) ; then\n"
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
                        f"{self.desibase_dir} --nproc {nthreads}\n")

        script_txt += utils.get_script_text_for_master_node(command)

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.desibase_dir, "quickquasars",
            env_command=self.env_command,
            dep_jobid=dep_jobid)

        print(f"Quickquasars script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def schedule(self, nodes, nthreads, time, batch, queue="regular", dep_jobid=None, skip=False):
        print("Setting up a quickquasars job...")
        self.create_directory(not skip)

        if skip:
            self.submitter_fname = None
        else:
            self.create_script(nodes, nthreads, time, queue, dep_jobid)

        jobid = -1
        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)
            print(f"quickquasars job submitted with JobID: {jobid}")

        print("--------------------------------------------------")
        return jobid


class OhioTransmissionsJob():
    def __init__(self, rootdir, realization, settings):
        self.rootdir = rootdir
        self.realization = realization

        self.version = settings['ohio']['version']
        self.release = settings['ohio']['release']
        self.survey = settings['ohio']['survey']
        self.catalog = settings['ohio']['catalog']

        self.seed_base = settings['transmissions']['base_seed']
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

    def create_script(self, time=5., queue="regular", dep_jobid=None):
        """ Creates and writes the script for newGenDESILiteMocks run. Uses 1 node and 128 CPUs.
        Sets self.submitter_fname.

        Args:
            time (float): Time need in minutes.
            dep_jobid (int): Dependent JobID. Defaults to None.

        Returns:
            submitter_fname (str): Filename of the submitter script.
        """
        time_txt = timedelta(minutes=time)

        script_txt = utils.get_script_header(
            self.transmissions_dir, f"ohio-trans-y1-{self.realization}",
            time_txt, nodes=1, queue=queue)

        script_txt += 'echo "Generating transmission files using qsotools."\n\n'
        script_txt += (f"newGenDESILiteMocks.py {self.transmissions_dir} "
                       f"--master-file {self.catalog} --save-qqfile --nproc 128 "
                       f"--seed {self.seed}\n")

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.transmissions_dir, f"gen-trans-{self.realization}",
            dep_jobid=dep_jobid)

        print(f"newGenDESILiteMocks script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def schedule(self, batch, queue="regular", dep_jobid=None, skip=False):
        print("Setting up a qsotools transmission generation job...")
        self.create_directory(not skip)

        if skip:
            self.submitter_fname = None
        else:
            self.create_script(queue=queue, dep_jobid=dep_jobid)

        jobid = -1
        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)
            print(f"qsotools job submitted with JobID: {jobid}")

        print("--------------------------------------------------")
        return jobid


class QSOnicJob():
    def __init__(
            self, rootdir, interm_path, desibase_dir, foldername,
            realization, qsonic_settings
    ):
        self.rootdir = rootdir
        self.interm_path = interm_path
        self.desibase_dir = desibase_dir
        self.foldername = foldername
        self.realization = realization

        self.wave1 = qsonic_settings['wave1']
        self.wave2 = qsonic_settings['wave2']
        self.forest_w1 = qsonic_settings['forest_w1']
        self.forest_w2 = qsonic_settings['forest_w2']
        self.cont_order = qsonic_settings['cont_order']
        self.coadd_arms = qsonic_settings['coadd_arms']
        self.skip_resomat = qsonic_settings['skip_resomat']
        self.suffix = qsonic_settings['suffix']

        self.delta_dir = f"Delta{self.suffix}"

        self._set_outdelta_dir()
        self.submitter_fname = None

    def _set_outdelta_dir(self):
        if self.rootdir:
            self.outdelta_dir = ospath_join(
                self.rootdir, self.interm_path, self.foldername, self.delta_dir)
        else:
            self.outdelta_dir = None

    def inc_realization(self, new_interm_path, new_desibase_dir):
        self.realization += 1
        self.interm_path = new_interm_path
        self.desibase_dir = new_desibase_dir

        self._set_outdelta_dir()

        self.submitter_fname = None

    def create_directory(self, create_dir=True):
        if not self.rootdir:
            return None

        if create_dir:
            print("Creating directory:")
            print(f"+ {self.outdelta_dir}")

            makedirs(self.outdelta_dir, exist_ok=True)

        return self.outdelta_dir

    def create_script(self, nodes=1, time=0.3, queue="regular", dep_jobid=None):
        """ Creates and writes the script for QSOnic run. Uses 128 CPUs per node.
        Sets self.submitter_fname.

        Args:
            nodes (int): Number of nodes
            time (float): Time need in hours.
            dep_jobid (int): Dependent JobID. Defaults to None.

        Returns:
            submitter_fname (str): Filename of the submitter script.
        """
        if self.outdelta_dir is None:
            return None

        time_txt = timedelta(hours=time)
        nthreads = nodes * 128
        script_txt = utils.get_script_header(
            self.outdelta_dir, f"qsonic-{self.realization}", time_txt, nodes, queue)

        command = f"srun -N {nodes} -n {nthreads} -c 2 qsonic-fit \\\\\n"
        command += f"-i {self.desibase_dir}/spectra-16 \\\\\n"
        command += f"--catalog {self.desibase_dir}/zcat.fits \\\\\n"
        command += f"-o {self.outdelta_dir} \\\\\n"
        command += f"--mock-analysis \\\\\n"
        command += f"--rfdwave 0.8 --skip 0.2 \\\\\n"
        command += f"--no-iterations 10 \\\\\n"
        command += f"--wave1 {self.wave1} --wave2 {self.wave2} \\\\\n"
        command += f"--forest-w1 {self.forest_w1} --forest-w2 {self.forest_w2}"
        if self.coadd_arms:
            command += " \\\\\n--coadd-arms"
        if self.skip_resomat:
            command += " \\\\\n--skip-resomat"

        command += "\n\n"

        command += f"srun -N {nodes} -n {nthreads} -c 2 qsonic-calib \\\\\n"
        command += f"-i {self.outdelta_dir} \\\\\n"
        command += f"-o {self.outdelta_dir}/var_stats \\\\\n"
        command += f"--wave1 {self.wave1} --wave2 {self.wave2} \\\\\n"
        command += f"--forest-w1 {self.forest_w1} --forest-w2 {self.forest_w2}"
        command += "\n\n"

        script_txt += command
        script_txt += f"getLists4QMLEfromPICCA.py {self.outdelta_dir} --nproc 128\n"
        script_txt += f"getLists4QMLEfromPICCA.py {self.outdelta_dir} --nproc 128 --snr-cut 1\n"
        script_txt += f"getLists4QMLEfromPICCA.py {self.outdelta_dir} --nproc 128 --snr-cut 2\n"

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.outdelta_dir, "qsonic-fit", dep_jobid=dep_jobid)

        print(f"QSOnic script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def schedule(self, batch, queue="regular", dep_jobid=None, skip=False):
        print("Setting up a QSOnic transmission generation job...")
        self.create_directory(not skip)

        if skip:
            self.submitter_fname = None
        else:
            self.create_script(queue=queue, dep_jobid=dep_jobid)

        jobid = -1
        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)
            print(f"QSOnic job submitted with JobID: {jobid}")

        print("--------------------------------------------------")
        return jobid


class QmleJob():
    """ To be implemented."""


class JobChain():
    def __init__(
            self, rootdir, realization, delta_dir, settings
    ):
        self.tr_job = OhioTransmissionsJob(rootdir, realization, settings)

        self.qq_job = OhioQuickquasarsJob(rootdir, realization, settings)

        self.qsonic_job = QSOnicJob(
            delta_dir,
            self.qq_job.interm_path, self.qq_job.desibase_dir, self.qq_job.foldername,
            realization,
            settings['qsonic']
        )

    def schedule(self, slurm_settings, no_transmissions, no_quickquasars):
        jobid = -1

        nodes = slurm_settings['nodes']
        nthreads = slurm_settings['nthreads']
        time = slurm_settings['time']
        batch = slurm_settings['batch']
        queue = slurm_settings['queue']

        jobid = self.tr_job.schedule(batch, queue, skip=no_transmissions)
        jobid = self.qq_job.schedule(nodes, nthreads, time, batch, queue, jobid, no_quickquasars)
        jobid = self.qsonic_job.schedule(batch, queue, jobid)

    def inc_realization(self):
        self.tr_job.inc_realization()
        self.qq_job.inc_realization()
        self.qsonic_job.inc_realization(self.qq_job.interm_path, self.qq_job.desibase_dir)
