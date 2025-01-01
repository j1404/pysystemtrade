from sysinit.futures.clone_data_for_instrument import clone_data_for_instrument


## format is 'from' = 'to'
mapping_dict = dict(
    #    SP500="SP500_micro",
    #    CRUDE_W="CRUDE_W_mini",
    #    KOSPI="KOSPI_mini",
    #    EUR="EUR_micro",
    #    HANGENT="HANGENT_mini",
    # 	 KRWUSD="KRWUSD_mini",
    # 	 SOYBEAN="SOYBEAN_mini",
    # 	 VIX="VIX_mini",
    # 	 JPY="JPY_mini",
    # 	 GAS_US="GAS_US_mini",
    #    COPPER="COPPER-micro",
    # 	 GOLD="GOLD_micro",
    # 	 AUD="AUD_micro",
    #    NASDAQ="NASDAQ_micro",
    #    IBEX="IBEX_mini",
    HANG="HANG_mini",
)


if __name__ == "__main__":
    write_to_csv = False
    for instrument_from, instrument_to in mapping_dict.items():
        clone_data_for_instrument(
            instrument_from=instrument_from,
            instrument_to=instrument_to,
            write_to_csv=write_to_csv,
            ignore_duplication=True,
        )
