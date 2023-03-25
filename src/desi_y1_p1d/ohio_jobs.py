from os import makedirs
from os.path import join as ospath_join
from datetime import timedelta

from desi_y1_p1d import utils


class OhioQuickquasarsJob():
    def __init__(
            self, rootdir, realization, version, release, survey, catalog,
            nexp, zmin_qso, cont_dwave, dla, bal, boring, seed_base,
            qq_env_command, suffix=""
    ):
        self.rootdir = rootdir
        self.realization = realization
        self.version = version
        self.release = release
        self.survey = survey
        self.catalog = catalog
        self.nexp = nexp
        self.exptime = f"{nexp}000"
        self.zmin_qso = zmin_qso
        self.cont_dwave = cont_dwave
        self.dla = dla
        self.bal = bal
        self.boring = boring
        self.seed_base = seed_base
        self.seed = f"{seed_base}{self.realization}"
        self.env_command = qq_env_command
        self.suffix = suffix

        self.interm_path = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)

        self.set_sysopt()

        self.foldername = f"desi-{self.version[1]}.{self.sysopt}-{self.nexp}{self.suffix}"
        self.desibase_dir = None
        self.transmissions_dir = None

        self.submitter_fname = None

    def inc_realization(self):
        self.realization += 1
        self.seed = f"{self.seed_base}{self.realization}"
        self.interm_path = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)

        self.desibase_dir = None
        self.submitter_fname = None

    def set_sysopt(self):
        sysopt = ""
        OPTS_QQ = (f"--zmin {self.zmin_qso} --zbest --bbflux --seed {self.seed}"
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

    def create_directory(self):
        self.desibase_dir = ospath_join(
            self.rootdir, self.interm_path, self.foldername)
        self.transmissions_dir = ospath_join(
            self.rootdir, self.interm_path, "transmissions")
        # make directories to store logs and spectra
        print("Creating directories:")
        print(f"+ {self.desibase_dir}")
        print(f"+ {self.desibase_dir}/logs")
        print(f"+ {self.desibase_dir}/spectra-16")

        makedirs(self.desibase_dir, exist_ok=True)
        makedirs(f"{self.desibase_dir}/logs", exist_ok=True)
        makedirs(f"{self.desibase_dir}/spectra-16", exist_ok=True)

        return self.desibase_dir

    def create_script(
            self, nodes, nthreads, time, dep_jobid=None
    ):
        time_txt = timedelta(hours=time)

        command = (f"srun -N 1 -n 1 -c {nthreads} "
                   f"quickquasars -i \\$tfiles --nproc {nthreads} "
                   f"--outdir {self.desibase_dir}/spectra-16 {self.OPTS_QQ}")

        script_txt = utils.get_script_header(
            self.desibase_dir, f"ohio-qq-y1-{self.realization}", time_txt, nodes)

        script_txt += 'echo "get list of skewers to run ..."\n\n'

        script_txt += f"files=\\`ls -1 {self.transmissions_dir}/*/*/lya-transmission*.fits*\\`\n"
        script_txt += "nfiles=\\`echo \\$files | wc -w\\`\n"
        script_txt += f"nfilespernode=\\$(( \\$nfiles/{nodes} + 1))\n\n"

        script_txt += 'echo "n files =" \\$nfiles\n'
        script_txt += 'echo "n files per node =" \\$nfilespernode\n\n'

        script_txt += "first=1\n"
        script_txt += "last=\\$nfilespernode\n"
        script_txt += "for node in \\`seq $nodes\\` ; do\n"
        script_txt += "    echo 'starting node \\$node'\n\n"

        script_txt += "    # list of files to run\n"
        script_txt += "    if (( \\$node == $nodes )) ; then\n"
        script_txt += "        last=""\n"
        script_txt += "    fi\n"
        script_txt += "    echo \\${first}-\\${last}\n"
        script_txt += ("    tfiles=\\`echo \\$files | cut -d "
                       " -f \\${first}-\\${last}\\`\n")
        script_txt += "    first=\\$(( \\$first + \\$nfilespernode ))\n"
        script_txt += "    last=\\$(( \\$last + \\$nfilespernode ))\n"
        script_txt += f"    command={command}\n\n"

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

    def schedule(
            self, nodes, nthreads, time, batch, dep_jobid=None
    ):
        print("Setting up a quickquasars job...")
        self.create_directory()

        self.create_script(nodes, nthreads, time, dep_jobid)

        jobid = -1
        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)
            print(f"quickquasars job submitted with JobID: {jobid}")

        print("--------------------------------------------------")
        return jobid


class OhioTransmissionsJob():
    def __init__(
            self, rootdir, realization, version, release, survey, catalog,
            seed_base
    ):
        self.rootdir = rootdir
        self.realization = realization
        self.version = version
        self.release = release
        self.survey = survey
        self.catalog = catalog
        self.seed_base = seed_base
        self.seed = f"{self.realization}{seed_base}"

        self.interm_paths = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)

        self.transmissions_dir = None
        self.submitter_fname = None

    def inc_realization(self):
        self.realization += 1
        self.seed = f"{self.realization}{self.seed_base}"

        self.interm_paths = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)

        self.transmissions_dir = None
        self.submitter_fname = None

    def create_directory(self, create_dir=True):
        basedir = (f"{self.rootdir}/{self.interm_paths}")
        self.transmissions_dir = f"{basedir}/transmissions"

        if create_dir:
            print("Creating directories:")
            print(f"+ {self.transmissions_dir}")
            makedirs(self.transmissions_dir, exist_ok=True)

        return self.transmissions_dir

    def create_script(self, dep_jobid=None, time=0.2):
        time_txt = timedelta(hours=time)

        script_txt = utils.get_script_header(
            self.transmissions_dir, f"ohio-trans-y1-{self.realization}",
            time_txt, nodes=1)

        script_txt += 'echo "Generating transmission files using qsotools."\n\n'
        script_txt += (f"newGenDESILiteMocks.py {self.transmissions_dir} "
                       f"--master-file {self.catalog} --save-qqfile --nproc 128 "
                       f"--seed {self.seed}\n")

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.transmissions_dir, f"gen-trans-{self.realization}",
            dep_jobid=dep_jobid)

        print(f"newGenDESILiteMocks script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def schedule(self, batch, dep_jobid=None, create_dir=True):
        print("Setting up a qsotools transmission generation job...")
        self.create_directory(create_dir)

        self.create_script(dep_jobid)

        jobid = -1
        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)
            print(f"qsotools job submitted with JobID: {jobid}")

        print("--------------------------------------------------")
        return jobid


class QSOnicJob():
    def __init__(
            self, delta_dir, interm_path, desibase_dir, foldername,
            realization, wave1, wave2, forest_w1, forest_w2,
            coadd_arms=True, skip_resomat=False
    ):
        self.delta_dir = delta_dir
        self.interm_path = interm_path
        self.desibase_dir = desibase_dir
        self.foldername = foldername
        self.realization = realization

        self.wave1 = wave1
        self.wave2 = wave2
        self.forest_w1 = forest_w1
        self.forest_w2 = forest_w2
        self.coadd_arms = coadd_arms
        self.skip_resomat = skip_resomat

        if self.delta_dir:
            self.outdelta_dir = ospath_join(
                self.delta_dir, self.interm_path, self.foldername)
        else:
            self.outdelta_dir = None
        self.submitter_fname = None

    def inc_realization(self, new_interm_path, new_desibase_dir):
        self.realization += 1
        self.interm_path = new_interm_path
        self.desibase_dir = new_desibase_dir

        if self.delta_dir:
            self.outdelta_dir = ospath_join(
                self.delta_dir, self.interm_path, self.foldername)
        else:
            self.outdelta_dir = None

        self.submitter_fname = None

    def create_directory(self, create_dir=True):
        if not self.delta_dir:
            return None

        if create_dir:
            print("Creating directories:")
            print(f"+ {self.outdelta_dir}")
            print(f"+ {self.outdelta_dir}/results")

            makedirs(self.outdelta_dir, exist_ok=True)
            makedirs(f"{self.outdelta_dir}/results", exist_ok=True)

        return self.outdelta_dir

    def create_script(self, nodes=1, time=0.3, dep_jobid=None):
        if self.outdelta_dir is None:
            return None

        time_txt = timedelta(hours=time)
        nthreads = nodes * 128
        script_txt = utils.get_script_header(
            self.outdelta_dir, f"qsonic-{self.realization}", time_txt, nodes)

        command = f"srun -N {nodes} -n {nthreads} -c 2 qsonic-fit \\\n"
        command += f"-i {self.desibase_dir}/spectra-16 \\\n"
        command += f"--catalog {self.desibase_dir}/zcat.fits \\\n"
        command += f"-o {self.outdelta_dir}/Delta \\\n"
        command += f"--mock-analysis \\\n"
        command += f"--rfdwave 0.8 --skip 0.2 \\\n"
        command += f"--no-iterations 10 \\\n"
        command += f"--wave1 {self.wave1} --wave2 {self.wave2} \\\n"
        command += f"--forest-w1 {self.forest_w1} --forest-w2 {self.forest_w2}"
        if self.coadd_arms:
            command += " \\\n--coadd-arms"
        if self.skip_resomat:
            command += " \\\n--skip-resomat"

        command += "\n"

        script_txt += command

        self.submitter_fname = utils.save_submitter_script(
            script_txt, self.outdelta_dir, "qsonic-fit", dep_jobid=dep_jobid)

        print(f"QSOnic script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def schedule(self, batch, dep_jobid=None, create_dir=True):
        print("Setting up a QSOnic transmission generation job...")
        self.create_directory(create_dir)

        self.create_script(dep_jobid)

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
            self, rootdir, realization, version, release, survey, catalog,
            nexp, zmin_qso, cont_dwave, dla, bal, boring, seed_base_qq,
            qq_env_command, suffix,
            seed_base_qsotools, delta_dir, wave1, wave2, forest_w1, forest_w2,
            coadd_arms=True, skip_resomat=False
    ):
        self.tr_job = OhioTransmissionsJob(
            rootdir, realization, version, release,
            survey, catalog,
            seed_base_qsotools
        )

        self.qq_job = OhioQuickquasarsJob(
            rootdir, realization, version, release,
            survey, catalog,
            nexp, zmin_qso, cont_dwave, dla, bal,
            boring, seed_base_qq, qq_env_command, suffix
        )

        self.qsonic_job = QSOnicJob(
            delta_dir,
            self.qq_job.interm_path, self.qq_job.desibase_dir, self.qq_job.foldername,
            realization,
            wave1, wave2, forest_w1, forest_w2,
            coadd_arms=True, skip_resomat=False
        )

    def schedule(self, nodes, nthreads, time, batch, no_transmissions):
        jobid = -1

        jobid = self.tr_job.schedule(batch, create_dir=not no_transmissions)

        jobid = self.qq_job.schedule(
            nodes, nthreads, time, batch,
            dep_jobid=jobid
        )

        jobid = self.qsonic_job.schedule(batch, dep_jobid=jobid)

    def inc_realization(self):
        self.tr_job.inc_realization()
        self.qq_job.inc_realization()
        self.qsonic_job.inc_realization(self.qq_job.interm_path, self.qq_job.desibase_dir)
