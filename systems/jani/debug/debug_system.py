import sys

from syscore.constants import arg_not_supplied
from sysdata.config.configdata import Config
from sysdata.sim.db_futures_sim_data import dbFuturesSimData

# from sysdata.sim.csv_futures_sim_data import csvFuturesSimData

from syslogging.logger import get_logger
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

DEFAULT_CONFIG = "systems.jani.debug.debug_sim_config.yaml"


log = get_logger("backtest")


def debug_system(config_path=None):
    if config_path is None:
        config_path = DEFAULT_CONFIG

    log.info(f"Building system from {config_path}")
    config = Config(config_path)
    db_data = dbFuturesSimData()
    # db_data = csvFuturesSimData()

    # create system
    system = futures_do_system(config=config, data=db_data)

    # calculate static performance (ignore output)
    system.accounts.portfolio().percent

    # calculate optimised performance (ignore output)
    system.accounts.optimised_portfolio().percent

    log.info(f"Start date for data: {system.data.start_date_for_data()}")

    for instr in system.get_instrument_list():
        log.info(
            f"First date of raw prices for {instr}: "
            f"{system.data.get_raw_price(instr).first_valid_index()}"
        )

    # first date for non zero position in a static system
    for instr in system.get_instrument_list():
        rounded = system.portfolio.accounts_stage.get_buffered_position(
            instr, roundpositions=True
        )
        log.info(
            f"First date for non zero static position in {instr}: "
            f"{rounded.loc[rounded.ne(0)].first_valid_index()}"
        )

    optimised_positions = system.accounts.get_optimised_position_df()
    for instr in system.get_instrument_list():
        log.info(
            f"First date for non zero optimised position in {instr}: "
            f"{optimised_positions.loc[optimised_positions[instr].ne(0), instr].first_valid_index()}"
        )


def futures_do_system(
    data=arg_not_supplied,
    config=arg_not_supplied,
    trading_rules=arg_not_supplied,
):
    if data is arg_not_supplied:
        data = dbFuturesSimData()

    if config is arg_not_supplied:
        config = Config("systems.jani.debug.debug_sim_config.yaml")

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
    debug_system(config_path)
