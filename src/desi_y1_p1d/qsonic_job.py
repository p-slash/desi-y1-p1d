from os import makedirs
from os.path import join as ospath_join
from datetime import timedelta
from desi_y1_p1d import utils


class QSOnicJob():
    def __init__(
            self, delta_dir, desibase_dir, interm_path, foldername,
            realization, wave1, wave2, forest_w1, forest_w2,
            coadd_arms=True, skip_resomat=False
    ):
        self.delta_dir = delta_dir
        self.desibase_dir = desibase_dir
        self.interm_path = interm_path
        self.foldername
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
        if self.outdeltadir is None:
            return None

        time_txt = timedelta(hours=time)
        nthreads = nodes * 128
        script_txt = utils.get_script_header(
            self.outdeltadir, f"qsonic-{self.realization}", time_txt, nodes)

        command = f"srun -N {nodes} -n {nthreads} -c 2 qsonic-fit \\\n"
        command += f"-i {self.desibase_dir}/spectra-16 \\\n"
        command += f"--catalog {self.desibase_dir}/zcat.fits \\\n"
        command += f"-o {self.outdeltadir}/Delta \\\n"
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
            script_txt, self.outdeltadir, "qsonic-fit", dep_jobid=dep_jobid)

        print(f"QSOnic script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def schedule(self, batch, dep_jobid=None, create_dir=True):
        self.create_directory(create_dir)

        self.create_script(dep_jobid)

        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)

        return jobid
