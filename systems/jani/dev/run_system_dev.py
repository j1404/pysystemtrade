#
# development system backtest
#
import sys
import datetime
import pandas as pd
from matplotlib.pyplot import show

from dotenv import load_dotenv
load_dotenv()

from syscore.constants import arg_not_supplied
from sysdata.config.configdata import Config
from sysdata.sim.db_futures_sim_data import dbFuturesSimData

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

log = get_logger("backtest")

DEFAULT_CONFIG = "systems.jani.dev.sim_config_dev.yaml"

def futures_do_system(
    data=arg_not_supplied,
    config=arg_not_supplied,
    trading_rules=arg_not_supplied,
):
    if data is arg_not_supplied:
        data = dbFuturesSimData()

    if config is arg_not_supplied:
        config = Config(DEFAULT_CONFIG)

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


def run_system(
    system=futures_do_system(),
    portfolio=system.accounts.optimised_portfolio(),
    portfolio_percent=system.accounts.portfolio().percent,

    # performance
    system.config.use_SR_costs=False,
    perf_unrounded = system.accounts.portfolio(roundpositions=False).percent,
    perf_rounded = system.accounts.portfolio(roundpositions=True).percent,
    perf_optimised = system.accounts.optimised_portfolio().percent,

    performance = pd.concat([perf_unrounded.curve(), perf_rounded.curve(), perf_optimised.curve()], axis=1),
    performance.columns = ["unrounded", "rounded", "optimised"],

    print("Sim config file: ",DEFAULT_CONFIG),
    print("Start date: ",system.config.start_date),
    print("Notional trading capital: ",system.config.notional_trading_capital),
    print("Instruments: ",system.portfolio.get_instrument_list()),
    print(f"Stats as %: {portfolio_percent.stats()}"),

    performance.plot(figsize=(15,9), title="Performance"),
    show(),
    )


if __name__ == "__main__":
    run_system()

