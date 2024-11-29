import sys
import datetime

from syscore.constants import arg_not_supplied
from sysdata.config.configdata import Config
from sysdata.sim.db_futures_sim_data import dbFuturesSimData

# from sysdata.sim.csv_futures_sim_data import csvFuturesSimData
from syslogging.logger import get_logger
from sysproduction.strategy_code.run_dynamic_optimised_system import futures_system
from systems.basesystem import System
from systems.forecast_combine import ForecastCombine
from systems.forecast_scale_cap import ForecastScaleCap
from systems.forecasting import Rules
from systems.portfolio import Portfolios
from systems.positionsizing import PositionSizing
from systems.provided.dynamic_small_system_optimise.accounts_stage import (
    accountForOptimisedStage,
)
from systems.provided.dynamic_small_system_optimise.optimised_positions_stage import (
    optimisedPositions,
)
from systems.provided.rob_system.rawdata import myFuturesRawData
from systems.risk import Risk

# DEFAULT_CONFIG = "systems.jani.dynamic_system_jani_v1.yaml"
DEFAULT_CONFIG = "systems.jani.dynamic_system_jani_v1_quick.yaml"


log = get_logger("backtest")


def save_system(config_path=None):
    if config_path is None:
        config_path = DEFAULT_CONFIG

    log.info(f"Building system from {config_path}")
    config = Config(config_path)
    db_data = dbFuturesSimData()
    system = futures_system(config=config, data=db_data)
    portfolio_percent = system.accounts.portfolio().percent
    write_pickle_file(system, config_path)

    return system


def write_pickle_file(system, config_path):
    save_path = (
        f"{config_path.removesuffix('.yaml')}-"
        f"{datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')}.pck"
    )

    log.info(f"Config system file '{config_path}'")
    log.info(f"Pickled file '{save_path}'")
    system.cache.pickle(save_path)


def futures_do_system(
    data=arg_not_supplied,
    config=arg_not_supplied,
    trading_rules=arg_not_supplied,
):
    if data is arg_not_supplied:
        data = dbFuturesSimData()
        # data = csvFuturesSimData()

    if config is arg_not_supplied:
        config = Config("systems.jani.dynamic_system_jani_v1.yaml")

    if trading_rules is arg_not_supplied:
        rules = Rules()
    else:
        rules = Rules(trading_rules)

    system = System(
        [
            Risk(),
            accountForOptimisedStage(),
            optimisedPositions(),
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
    args = None
    my_args = sys.argv
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = DEFAULT_CONFIG
    save_system(config_path)
