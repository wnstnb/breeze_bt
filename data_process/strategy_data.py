import vectorbt as vbt

class OHLCData:
    def __init__(self):
        self.price = None

    def produce_data(self, symbol, start_date, interval):
        self.price = vbt.YFData.download(
            symbol,
            start=start_date,
            interval=interval
        ).get('Close')
        return self.price