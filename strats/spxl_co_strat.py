import vectorbt as vbt
import pandas as pd
from numba import njit
from data_process.strategy_data import OHLCData

class StrategyWrapper:
    def __init__(self, symbol='SPXL', start_date='1/1/2024', interval='1d', take_profit_pct = 20, stop_loss_pct = 0.1):
        self.symbol = symbol
        self.start_date = start_date
        self.interval = interval
        self.take_profit_pct = take_profit_pct
        self.stop_loss_pct = stop_loss_pct
        self.data_process = OHLCData()
        self.orders = None
        self.price = self.data_process.produce_data(self.symbol, self.start_date, self.interval)
        self.run()

    def download_price(self):
        self.price = vbt.YFData.download(
            self.symbol,
            start=self.start_date,
            interval=self.interval
        ).get('Close')

    def run(self):

        EMA = vbt.IndicatorFactory.from_ta('EMAIndicator')
        ema13 = EMA.run(self.price, 2)
        ema48 = EMA.run(self.price, 3)
        entries = ema13.ema_indicator_crossed_above(ema48)
        exits = ema13.ema_indicator_crossed_below(ema48)

        # Stepped SL Strategy
        @njit
        def adjust_sl_func_nb(c):
            current_profit = (c.val_price_now - c.init_price) / c.init_price
            if current_profit >= 0.6:
                return 0.4, True
            if current_profit >= 0.40:
                return 0.05, True
            elif current_profit >= 0.2:
                return 0.01, True
            return c.curr_stop, c.curr_trail
        

        pf = vbt.Portfolio.from_signals(
            close=self.price,
            entries=entries,
            exits=exits,
            short_entries=exits,
            short_exits=entries,
            sl_stop=self.stop_loss_pct,
            adjust_sl_func_nb=adjust_sl_func_nb,
            tp_stop=self.take_profit_pct,
            freq='1D'
        )

        self.orders = pf.orders.records_readable

    def get_position_instructions(self):
        if self.orders is not None and len(self.orders) > 0:
            instr = self.orders.iloc[-1]
            return (self.symbol, instr['Timestamp'], instr['Side'], instr['Price'])
        return None
