class Company:
    def __init__(
        self, name: str,
        ticker: str,
        _id: int = None
    ) -> None:
        self.id = _id
        self.name = name
        self.ticker = ticker

    def save(self) -> None:
        pass


class Companies:
    def __init__(self) -> None:
        self.companies = []

    def add(self) -> None:
        pass

    def save(self) -> None:
        pass
