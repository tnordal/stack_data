class Bar:
    def __init__(
        self,
        ticker: str,
        timestamp: int,
        open: float,
        high: float,
        low: float,
        close: float,
        adj_close: float,
        volume: int,
        _id: int = None
    ) -> None:
        self.id = _id
        self.ticker = ticker
        self.date = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.adj_close = adj_close
        self.volume = volume

