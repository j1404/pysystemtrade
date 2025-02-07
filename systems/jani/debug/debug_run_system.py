#
# development system backtest
#
#DEFAULT_CONFIG = "systems.jani.debug.debug_sim_config.yaml"
DEFAULT_CONFIG = "systems.jani.debug.debug_sim_config_1970.yaml"

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
print("Notional trading capital: ",system.config.notional_trading_capital,system.config.base_currency)
print("Volatility target: ", system.config.percentage_vol_target)
print("Instruments: ", system.portfolio.get_instrument_list())
print(f"Stats as %: {portfolio_percent.stats()}")

performance.plot(figsize=(15, 9), title="Performance")

# summary stats
corr = pd.concat([perf_unrounded.curve(), perf_optimised.curve()], axis=1)
sharpe_gross = system.accounts.optimised_portfolio().gross.sharpe()
sharpe_net = system.accounts.optimised_portfolio().net.sharpe()
sr_cost_loss = sharpe_gross - sharpe_net
turnover = system.accounts.total_portfolio_level_turnover()
print(
    f"Unrounded v optimised portfolio returns correlation: {round(corr.corr().iloc[0, 1], 5)}"
)
print(f"Sharpe gross: {round(sharpe_gross, 3)}")
print(f"Sharpe net: {round(sharpe_net, 3)}")
print(
    f"Sharpe gross net difference: {round(sr_cost_loss, 3)} (or, in basis points: ~{round(sr_cost_loss * 100)})"
)
print(f"Portfolio level turnover: {round(turnover, 2)}")

# print performance plot
show()

# positions plots
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
    pos.plot(figsize=(15,9), title=f"Positions {instr}")
    show()


# costs v performance
#optimised = system.accounts.optimised_portfolio().percent.net
#costs = optimised.costs.curve()
#costs = costs * -10
#costs_v_perf = pd.concat([optimised.curve(), costs], axis=1)
#costs_v_perf.columns = ["Net performance %", "Costs (x -1.0)"]
#costs_v_perf.plot(figsize=(15, 9))
#show()


if __name__ == "__main__":
    args = None
    my_args = sys.argv
    if len(sys.argv) > 1:
        config_path = sys.argv[1]
    else:
        config_path = DEFAULT_CONFIG
    create_system(config_path)
