#
# development system backtest
#
DEFAULT_CONFIG = "systems.jani.dev.sim_config_dev_single.yaml"

import sys
import datetime
import pandas as pd
from matplotlib.pyplot import show

from dotenv import load_dotenv

load_dotenv()

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

log = get_logger("backtest")


def create_system(config_path=None):
    if config_path is None:
        config_path = DEFAULT_CONFIG

    # log.info(f"Building system from {config_path}")
    config = Config(config_path)
    db_data = dbFuturesSimData()
    system = futures_do_system(config=config, data=db_data)


def futures_do_system(
    data=arg_not_supplied,
    config=arg_not_supplied,
    trading_rules=arg_not_supplied,
):
    if data is arg_not_supplied:
        data = dbFuturesSimData()
        # data = csvFuturesSimData()

    if config is arg_not_supplied:
        # config = Config("systems.jani.dynamic_system_jani_v1.yaml")
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


system = futures_do_system()
portfolio = system.accounts.optimised_portfolio()
portfolio_percent = system.accounts.portfolio().percent

# performance

system.config.use_SR_costs = False
perf_unrounded = system.accounts.portfolio(roundpositions=False).percent
perf_rounded = system.accounts.portfolio(roundpositions=True).percent
perf_optimised = system.accounts.optimised_portfolio().percent

performance = pd.concat(
    [perf_unrounded.curve(), perf_rounded.curve(), perf_optimised.curve()], axis=1
)
performance.columns = ["unrounded", "rounded", "optimised"]

print("Sim config file: ", DEFAULT_CONFIG)
print("Start date: ", system.config.start_date)
print("Notional trading capital: ", system.config.notional_trading_capital)
print("Instruments: ", system.portfolio.get_instrument_list())
print(f"Stats as %: {portfolio_percent.stats()}")


drawdown_series = perf_optimised.drawdown()
max_dd = drawdown_series.min()
print(f"Maximum Drawdown: {max_dd:.2%}")

# Print all available methods
# print("Available methods on optimised_portfolio:")
# methods = [m for m in dir(perf_optimised) if not m.startswith('_')]
# for m in sorted(methods):
#    print(f"  - {m}")

# Capital is a property, not a method - no ()
print("Capital over time:")
print(perf_optimised.capital)

print("\nCapital stats:")
print(f"Min: {perf_optimised.capital.min():.0f}")
print(f"Max: {perf_optimised.capital.max():.0f}")
print(f"Mean: {perf_optimised.capital.mean():.0f}")

# Margin utilization estimate
initial_capital = 40000
min_capital = perf_optimised.capital.min()
max_capital_drawdown_pct = (1 - (min_capital / initial_capital)) * 100
print(f"\nCapital drawdown from peak: {max_capital_drawdown_pct:.1f}%")

# Full reporting code
print(f"\n=== MARGIN & CAPITAL ===")
print(f"Initial Capital: {initial_capital:.0f} EUR")
print(f"Minimum Capital: {min_capital:.0f} EUR")
print(f"Maximum Capital: {perf_optimised.capital.max():.0f} EUR")
print(f"Max Drawdown in Capital: {max_capital_drawdown_pct:.1f}%")


performance.plot(figsize=(15, 9), title="Performance")
show()

# positions plot

for instr in system.portfolio.get_instrument_list():
    unrounded = system.portfolio.accounts_stage.get_buffered_position(
        instr, roundpositions=False
    )
    rounded = system.portfolio.accounts_stage.get_buffered_position(
        instr, roundpositions=True
    )
    optimised = system.accounts.get_optimised_position_df()[instr]
    pos = pd.concat([unrounded, rounded, optimised], axis=1)
    pos.columns = ["unrounded", "rounded", "optimised"]
    pos.plot(figsize=(15, 9), title=f"Positions {instr}")
    show()


if __name__ == "__main__":
    args = None
    my_args = sys.argv
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = DEFAULT_CONFIG
    create_system(config_path)
