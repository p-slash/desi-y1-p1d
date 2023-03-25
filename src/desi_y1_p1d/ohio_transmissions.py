from os import makedirs
from datetime import timedelta
from desi_y1_p1d import utils


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
        self.seed = f"{self.realization}{seed_base}"

        self.transmissions_dir = None
        self.submitter_fname = None

    def create_directory(self, create_dir=True):
        interm_paths = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)

        basedir = (f"{self.rootdir}/{interm_paths}")
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
        self.create_directory(create_dir)

        self.create_script(dep_jobid)

        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)

        return jobid
