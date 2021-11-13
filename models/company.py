class Company:
    def __init__(
        self, name: str,
        ticker: str,
        _id: int = None
    ) -> None:
        self.id = _id
        self.name = name
        self.ticker = ticker
