import datetime

import yaml

from syscore.constants import arg_not_supplied
from syscore.fileutils import resolve_path_and_filename_for_package
from sysdata.config.configdata import Config
from sysdata.sim.db_futures_sim_data import dbFuturesSimData

# from sysdata.sim.csv_futures_sim_data import csvFuturesSimData
from syslogging.logger import get_logger
from systems.accounts.accounts_stage import Account
from systems.basesystem import System
from systems.diagoutput import systemDiag
from systems.forecast_combine import ForecastCombine
from systems.forecast_scale_cap import ForecastScaleCap
from systems.forecasting import Rules
from systems.portfolio import Portfolios
from systems.positionsizing import PositionSizing

from systems.provided.rob_system.rawdata import myFuturesRawData

# CONFIG = "systems.jani.dynamic_system_jani_v1.yaml"
# SAVED_SYSTEM = "systems.jani.dynamic_system_jani_v1.pck"

CONFIG = "systems.jani.static_estimation.yaml"
SAVED_SYSTEM = "systems.jani.static_estimation.pck"

log = get_logger("backtest")


def run_system(load_pickle=False, write_pickle=False, do_estimate=False):
    if load_pickle:
        log.info(f"Loading system from {SAVED_SYSTEM}")
        system = jani_system()
        system.cache.get_items_with_data()
        system.cache.unpickle(SAVED_SYSTEM)
        system.cache.get_items_with_data()
        write_pickle = False
    else:
        log.info(f"Building system from {CONFIG}")
        config = Config(CONFIG)
        if do_estimate:
            config.use_forecast_div_mult_estimates = True
            config.use_instrument_div_mult_estimates = True
            config.use_forecast_weight_estimates = True
            config.use_instrument_weight_estimates = True
            config.use_forecast_scale_estimates = True
        system = jani_system(config=config)

    acc_portfolio_percent = system.accounts.portfolio().percent

    log.info(f"Stats: {acc_portfolio_percent.stats()}")
    log.info(f"Stats as %: {acc_portfolio_percent.stats()}\n")

    # acc_portfolio_percent.curve().plot(legend=True)
    # show()

    if write_pickle:
        write_pickle_file(system)
    if do_estimate:
        write_estimate_file(system)

    return system


def write_pickle_file(system):
    log.info(f"Writing pickled system to {SAVED_SYSTEM}")
    system.cache.pickle(SAVED_SYSTEM)


def write_estimate_file(system):
    now = datetime.datetime.now()
    sysdiag = systemDiag(system)
    output_file = resolve_path_and_filename_for_package(
        f"systems.jani.estimate-{now.strftime('%Y-%m-%d_%H%M%S')}.yaml"
    )
    print(f"writing estimate params to: {output_file}")
    estimates_needed = [
        "instrument_div_multiplier",
        "forecast_div_multiplier",
        "forecast_scalars",
        "instrument_weights",
        "forecast_weights",
    ]

    sysdiag.yaml_config_with_estimated_parameters(output_file, estimates_needed)


def config_from_file(path_string):
    path = resolve_path_and_filename_for_package(path_string)
    with open(path) as file_to_parse:
        config_dict = yaml.load(file_to_parse, Loader=yaml.CLoader)
    return config_dict


def jani_system(
    data=arg_not_supplied,
    config=arg_not_supplied,
    trading_rules=arg_not_supplied,
):
    if data is arg_not_supplied:
        # data = csvFuturesSimData()
        data = dbFuturesSimData()

    if config is arg_not_supplied:
        config = Config("systems.jani.dynamic_system_jani_v1.yaml")

    rules = Rules(trading_rules)

    system = System(
        [
            Account(),
            Portfolios(),
            PositionSizing(),
            myFuturesRawData(),
            ForecastCombine(),
            ForecastScaleCap(),
            rules,
        ],
        data,
        config,
    )

    return system


if __name__ == "__main__":
    # run_system(load_pickle=True)
    # run_system()
    # run_system(do_estimate=True)
    run_system(load_pickle=False, write_pickle=False, do_estimate=True)
