desi_main_env = "source /global/common/software/desi/desi_environment.sh main"
all_settings = ["desi_y1_iron_v0_nosyst"]

desi_y1_iron_v0_nosyst = {
    "slurm": {
        "nodes": 1,
        "nthreads": 128,
        "time": 0.5,  # In hours
        "batch": False,
    },

    "ohio": {
        "version": "v1.2",
        "release": "iron",
        "survey": "main",
        "catalog": ("/global/cfs/cdirs/desi/survey/catalogs/Y1/QSO/iron/"
                    "QSO_cat_iron_main_dark_healpix_v0.fits")
    },

    "quickquasars": {
        "nexp": 1,
        "dla": "",
        "bal": 0,
        "boring": False,
        "zmin_qq": 1.8,
        "env_command_qq": desi_main_env,
        "base_seed": 62300,  # Realization number is concatenated to the right
        "cont_dwave": 2.0,
        "skip": False,
        "suffix": ""
    },

    "transmissions": {
        "base_seed": 332298,  # Realization number is concatenated to the left
        "skip": False
    },

    "qsonic": {
        "wave1": 3600., "wave2": 6600.,
        "forest_w1": 1040., "forest_w2": 1200.,
        "cont_order": 1,
        "suffix": ""
    }
}


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

    for key in prg_dict.keys():
        arg_key = _map_prgkey_to_argkey(key, prg)
        if arg_key not in args_dict:
            continue

        if not args_dict[arg_key]:
            continue

        prg_dict[key] = args_dict[arg_key]
        is_modified = True

    if is_modified and is_suffix_in and not prg_dict["suffix"]:
        print(f"Warning: you have changed default settings of {prg}, "
              "but didn't provide suffix to distinguish.")

    return prg_dict


def get_settings_from_args(sett, args):
    args_dict = vars(args)
    new_sett = sett.copy()

    for prg, prg_dict in new_sett.items():
        prg_dict = _update_prg_dict(prg, prg_dict, args_dict)

    return new_sett


def print_settings(sett):
    for prg, prg_dict in sett.items():
        print(prg)
        for key, value in prg_dict.items():
            print(f"  {key}: {value}")
        print("---------------------")
