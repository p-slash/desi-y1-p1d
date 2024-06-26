from configparser import ConfigParser
import importlib.resources as imprsrc


class OhioMockSettings():
    all_settings = sorted([
        _.name[5:-4] for _ in
        imprsrc.files("desi_y1_p1d").joinpath("configs").glob("mock_*.ini")])

    @staticmethod
    def list_available_settings():
        print("Currently available settings are:")
        for setting in OhioMockSettings.all_settings:
            print(f"+ {setting}")

    def __init__(self, setting):
        assert (setting in OhioMockSettings.all_settings)

        self.settings = ConfigParser()
        self.settings.optionxform = str

        with imprsrc.files("desi_y1_p1d").joinpath(
                "configs", f"mock_{setting}.ini"
        ).open() as f:
            self.settings.read_file(f)

    def update_from_args(self, args):
        args_dict = vars(args)

        for prg, prg_dict in self.settings.items():
            prg_dict = _update_prg_dict(prg, prg_dict, args_dict)

    def print(self):
        for prg, prg_dict in self.settings.items():
            print(prg)
            for key, value in prg_dict.items():
                print(f"  {key}: {value}")
            print("---------------------")


class DesiDataSettings():
    all_settings = sorted([
        _.name[5:-4] for _ in
        imprsrc.files("desi_y1_p1d").joinpath("configs").glob("data_*.ini")])

    @staticmethod
    def list_available_settings():
        print("Currently available settings are:")
        for setting in DesiDataSettings.all_settings:
            print(f"+ {setting}")

    def __init__(self, setting):
        assert (setting in DesiDataSettings.all_settings)

        parser = ConfigParser()
        parser.optionxform = str

        with imprsrc.files("desi_y1_p1d").joinpath(
                "configs", f"data_{setting}.ini"
        ).open() as f:
            parser.read_file(f)

        self.settings = ConfigParser()
        self.settings.optionxform = str

        default_dict = {}
        for qsec in ["qsonic", "qmle"]:
            default_section = dict(parser[f"{qsec}.default"])

            for section in parser.sections():
                if not section.startswith(qsec):
                    continue

                default_dict[section] = default_section

        self.settings.read_dict(default_dict)
        with imprsrc.files("desi_y1_p1d").joinpath(
                "configs", f"data_{setting}.ini"
        ).open() as f:
            self.settings.read_file(f)
        self.settings.remove_section("qsonic.default")
        self.settings.remove_section("qmle.default")

    def update_from_args(self, args):
        args_dict = vars(args)

        for prg, prg_dict in self.settings.items():
            prg_dict = _update_prg_dict(prg, prg_dict, args_dict)

    def print(self):
        for prg, prg_dict in self.settings.items():
            print(prg)
            for key, value in prg_dict.items():
                print(f"  {key}: {value}")
            print("---------------------")


_key_map = {
    "quickquasars": {
        "base_seed": "base_seed_qq",
        "skip": "skip_qq",
        "suffix": "suffix_qq",
        "nodes": "nodes_qq",
        "nthreads": "nthreads_qq",
        "time": "time_qq"
    },

    "transmissions": {
        "base_seed": "base_seed_transmissions",
        "skip": "no_transmissions"
    },

    "qsonic": {
        "suffix": "suffix_qsonic",
        "skip": "skip_qsonics",
        "env_command": "env_command_qsonic"
    },

    "qmle": {
        "skip": "skip_qmles"
    }
}


def _map_prgkey_to_argkey(key, prg):
    jj = prg.find('.')
    if jj != -1:
        root_prg = prg[:jj]
    else:
        root_prg = prg

    if root_prg not in _key_map:
        return key

    if key not in _key_map[root_prg]:
        return key

    return _key_map[root_prg][key]


def _update_prg_dict(prg, prg_dict, args_dict):
    is_modified = False
    is_suffix_in = "suffix" in prg_dict
    current_suffix = prg_dict.get("suffix")
    omitted_keys = ["nodes", "nthreads", "batch", "skip", "time", "queue"]

    for key in prg_dict.keys():
        arg_key = _map_prgkey_to_argkey(key, prg)

        if arg_key not in args_dict:
            continue

        args_value = args_dict[arg_key]

        if args_value is None:
            continue

        if key == "skip" and isinstance(args_value, list):
            jj = prg.find('.') + 1
            do_skip = ("all" in args_value) or (prg[jj:] in args_value)
            prg_dict["skip"] = str(do_skip)
        elif key == "fit_extra_opts":
            prg_dict[key] += f" {args_value}"
        else:
            prg_dict[key] = str(args_value)

        is_modified = True & (key not in omitted_keys)

    if is_modified and is_suffix_in and prg_dict["suffix"] == current_suffix:
        print(f"Warning: you have changed default settings of {prg}, "
              "but didn't change the suffix to distinguish.")

    return prg_dict
