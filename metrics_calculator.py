from __future__ import annotations

import numpy as np
import pandas as pd
from constants import TRADING_DAYS_PER_YEAR


class MetricsCalculator:
    """Centralised financial metrics calculations using vectorised operations."""

    @staticmethod
    def calculate_sharpe_ratio(
        daily_returns: pd.Series, risk_free_rate: float = 0.0
    ) -> float:
        """Return the annualised Sharpe ratio.

        Returns 0.0 when *daily_returns* is empty or has zero standard deviation
        to avoid division-by-zero errors.
        """
        if daily_returns.empty or daily_returns.std() == 0:
            return 0.0
        return (
            (daily_returns.mean() - risk_free_rate)
            / daily_returns.std()
            * np.sqrt(TRADING_DAYS_PER_YEAR)
        )

    @staticmethod
    def calculate_sortino_ratio(
        daily_returns: pd.Series, risk_free_rate: float = 0.0
    ) -> float:
        """Return the annualised Sortino ratio using downside deviation.

        Returns 0.0 when there are no negative returns or their standard
        deviation is zero.
        """
        negative_returns = daily_returns[daily_returns < risk_free_rate]
        if negative_returns.empty or negative_returns.std() == 0:
            return 0.0
        return (
            (daily_returns.mean() - risk_free_rate)
            / negative_returns.std()
            * np.sqrt(TRADING_DAYS_PER_YEAR)
        )

    @staticmethod
    def calculate_maximum_drawdown(cumulative_returns: pd.Series) -> float:
        """Return the maximum drawdown (positive value) from a cumulative returns series."""
        drawdown = cumulative_returns - cumulative_returns.cummax()
        return abs(drawdown.min())

    @staticmethod
    def calculate_volatility(daily_returns: pd.Series) -> float:
        """Return annualised volatility (standard deviation scaled by √T)."""
        return daily_returns.std() * np.sqrt(TRADING_DAYS_PER_YEAR)

    @staticmethod
    def calculate_calmar_ratio(
        daily_returns: pd.Series, max_drawdown: float
    ) -> float:
        """Return the Calmar ratio (annualised return divided by max drawdown)."""
        annualized_return = daily_returns.mean() * TRADING_DAYS_PER_YEAR
        return annualized_return / abs(max_drawdown)

    @staticmethod
    def calculate_expected_return(daily_returns: pd.Series) -> float:
        """Return the annualised expected return (mean daily return × trading days)."""
        return daily_returns.mean() * TRADING_DAYS_PER_YEAR

    @classmethod
    def calculate_risk_metrics(
        cls,
        daily_returns: pd.Series,
        cumulative_returns: pd.Series,
    ) -> dict[str, float]:
        """Return a comprehensive risk-metrics package.

        Args:
            daily_returns: Series of per-period returns.
            cumulative_returns: Cumulative (compounded) returns series used
                for drawdown calculation.

        Returns:
            Dictionary with keys: Sharpe Ratio, Sortino Ratio, Max Drawdown,
            Volatility, Calmar Ratio, Annualized Expected Return.
        """
        max_drawdown = cls.calculate_maximum_drawdown(cumulative_returns)
        return {
            "Sharpe Ratio": cls.calculate_sharpe_ratio(daily_returns),
            "Sortino Ratio": cls.calculate_sortino_ratio(daily_returns),
            "Max Drawdown": max_drawdown,
            "Volatility": cls.calculate_volatility(daily_returns),
            "Calmar Ratio": cls.calculate_calmar_ratio(daily_returns, max_drawdown),
            "Annualized Expected Return": cls.calculate_expected_return(daily_returns),
        }
