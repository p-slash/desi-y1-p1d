import json
from pkg_resources import resource_filename


class OhioMockSettings():
    all_settings = ["y1_iron_v0_nosyst"]

    @staticmethod
    def list_available_settings():
        print("Currently available settings are:")
        for setting in OhioMockSettings.all_settings:
            print(f"+ {setting}")

    def __init__(self, setting):
        assert (setting in OhioMockSettings.all_settings)
        fname = resource_filename('desi_y1_p1d', f'configs/mock_{setting}.json')
        with open(fname) as fp:
            self.settings = json.load(fp)

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
    all_settings = ["y1_iron_v0_nosyst"]

    @staticmethod
    def list_available_settings():
        print("Currently available settings are:")
        for setting in DesiDataSettings.all_settings:
            print(f"+ {setting}")

    def __init__(self, setting):
        assert (setting in DesiDataSettings.all_settings)
        fname = resource_filename('desi_y1_p1d', f'configs/data_{setting}.json')
        with open(fname) as fp:
            self.settings = json.load(fp)

    def update_from_args(self, args):
        args_dict = vars(args)

        for prg, prg_dict in self.settings.items():
            if prg != "qsonic":
                prg_dict = _update_prg_dict(prg, prg_dict, args_dict)
                continue
            for forest, forest_dict in prg_dict.items():
                forest_dict = _update_prg_dict(forest, forest_dict, args_dict)

    def print(self):
        for prg, prg_dict in self.settings.items():
            print("=====================")
            print(prg)
            if prg != "qsonic":
                for key, value in prg_dict.items():
                    print(f"  {key}: {value}")
                continue
            for forest, forest_dict in prg_dict.items():
                print(f"  {forest}")
                for key, value in forest_dict.items():
                    print(f"    {key}: {value}")
                print("---------------------")


_key_map = {
    "quickquasars": {
        "base_seed": "base_seed_qq",
        "skip": "skip_qq",
        "suffix": "suffix_qq"
    },

    "transmissions": {
        "base_seed": "base_seed_transmissions",
        "skip": "no_transmissions"
    },

    "qsonic": {
        "suffix": "suffix_qsonic"
    }
}


def _map_prgkey_to_argkey(key, prg):
    if prg not in _key_map:
        return key

    if key not in _key_map[prg]:
        return key

    return _key_map[prg][key]


def _update_prg_dict(prg, prg_dict, args_dict):
    is_modified = False
    is_suffix_in = "suffix" in prg_dict
    current_suffix = prg_dict.get("suffix")

    for key in prg_dict.keys():
        arg_key = _map_prgkey_to_argkey(key, prg)
        if arg_key not in args_dict:
            continue

        if not args_dict[arg_key]:
            continue

        prg_dict[key] = args_dict[arg_key]
        is_modified = True

    if is_modified and is_suffix_in and prg_dict["suffix"] == current_suffix:
        print(f"Warning: you have changed default settings of {prg}, "
              "but didn't change the suffix to distinguish.")

    return prg_dict
