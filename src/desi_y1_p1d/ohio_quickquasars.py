from os import makedirs
from datetime import timedelta

from desi_y1_p1d import utils


class OhioQuickquasarsJob():
    def __init__(
            self, rootdir, realization, version, release, survey, catalog,
            nexp, zmin_qso, cont_dwave, dla, bal, boring, seed_base, suffix=""
    ):
        self.rootdir = rootdir
        self.realization = realization
        self.version = version
        self.release = release
        self.survey = survey
        self.catalog = catalog
        self.exptime = f"{nexp}000"
        self.zmin_qso = zmin_qso
        self.cont_dwave = cont_dwave
        self.seed = f"{seed_base}{self.realization}"
        self.suffix = suffix

        self.interm_path = utils.get_folder_structure(
            self.realization, self.version, self.release, self.survey,
            self.catalog)

        self.set_sysopt()

        self.foldername = None
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
        basedir = (f"{self.rootdir}/{self.interm_paths}")

        self.foldername = f"desi-{self.version[1]}.{self.sysopt}-{self.nexp}{self.suffix}"
        self.desibase_dir = f"{basedir}/{foldername}"
        self.transmissions_dir = f"{basedir}/transmissions"

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
            self, nodes, nthreads, time, env_command, dep_jobid=None
    ):
        time_txt = timedelta(hours=time)

        command = (f"srun -N 1 -n 1 -c {nthreads} "
                   f"quickquasars -i \\$tfiles --nproc {nthreads} "
                   f"--outdir {self.desibase_dir}/spectra-16 {self.OPTS_QQ}")

        script_txt = utils.get_script_header(
            self.desibase_dir, f"ohio-qq-y1-{realization}", time_txt, nodes)

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
            env_command=env_command,
            dep_jobid=dep_jobid)

        print(f"Quickquasars script is saved as {self.submitter_fname}.")

        return self.submitter_fname

    def schedule(
            self, nodes, nthreads, time, batch, env_command, dep_jobid=None
    ):
        self.create_directory()

        self.create_script(nodes, nthreads, time, env_command, jobid)

        if batch and self.submitter_fname:
            jobid = utils.submit_script(self.submitter_fname)

        return jobid


