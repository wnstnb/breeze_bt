from strats.spxl_co_strat import StrategyWrapper

# The strategy will contain all instructions for which ticker and how to run.
strategy = StrategyWrapper()

# Returns a tuple with 3 elements: Timestamp, Side, and Price
print(strategy.get_position_instructions())