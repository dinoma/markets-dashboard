from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
from constants import MIN_MARKET_DATA_YEAR


class NavigationService:
    """Centralised service for market and year navigation with state validation."""

    def __init__(
        self,
        market_tickers: dict[str, str],
        initial_market: Optional[str] = None,
        initial_year: Optional[int] = None,
    ) -> None:
        """Initialise the navigation service.

        Args:
            market_tickers: Mapping of human-readable market names to ticker symbols.
            initial_market: Starting market name; defaults to the first entry.
            initial_year: Starting year; defaults to the current calendar year.
        """
        if initial_year is None:
            initial_year = datetime.now().year
        self.market_tickers = market_tickers
        self.markets = list(market_tickers.keys())
        self.current_market = initial_market if initial_market else self.markets[0]
        self.current_year = (
            initial_year if self.validate_year(initial_year) else datetime.now().year
        )

    def validate_market(self, market: str) -> bool:
        """Return True if *market* exists in the configured tickers."""
        return market in self.market_tickers

    def validate_year(self, year: int) -> bool:
        """Return True if *year* is within [MIN_MARKET_DATA_YEAR, current year]."""
        return MIN_MARKET_DATA_YEAR <= year <= datetime.now().year

    def next_market(self) -> tuple[str, str]:
        """Advance to the next market (wraps around) and return (name, ticker)."""
        current_index = self.markets.index(self.current_market)
        self.current_market = self.markets[(current_index + 1) % len(self.markets)]
        return self.current_market, self.market_tickers[self.current_market]

    def previous_market(self) -> tuple[str, str]:
        """Move to the previous market (wraps around) and return (name, ticker)."""
        current_index = self.markets.index(self.current_market)
        self.current_market = self.markets[(current_index - 1) % len(self.markets)]
        return self.current_market, self.market_tickers[self.current_market]

    def set_market(self, market: str) -> tuple[Optional[str], Optional[str]]:
        """Set the current market to *market* if valid.

        Returns:
            (name, ticker) on success, or (None, None) if market is unknown.
        """
        if self.validate_market(market):
            self.current_market = market
            return self.current_market, self.market_tickers[self.current_market]
        return None, None

    def next_year(self) -> int:
        """Increment the current year (clamped to the allowed upper bound)."""
        if self.validate_year(self.current_year + 1):
            self.current_year += 1
        return self.current_year

    def previous_year(self) -> int:
        """Decrement the current year (clamped to the allowed lower bound)."""
        if self.validate_year(self.current_year - 1):
            self.current_year -= 1
        return self.current_year

    def set_year(self, year: int) -> Optional[int]:
        """Set the current year to *year* if valid.

        Returns:
            The new year on success, or None if *year* is out of range.
        """
        if self.validate_year(year):
            self.current_year = year
            return self.current_year
        return None

    def get_current_state(self) -> dict[str, Any]:
        """Return the current market/year state as a plain dictionary."""
        return {
            "market": self.current_market,
            "ticker": self.market_tickers[self.current_market],
            "year": self.current_year,
        }
