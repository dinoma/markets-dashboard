from __future__ import annotations

from typing import Any, Optional
import pandas as pd
from exceptions import DataProcessingError, DataValidationError


class DataProcessor:
    """Validate, clean, and transform raw market DataFrames."""

    def __init__(self, validation_rules: Optional[dict[str, Any]] = None) -> None:
        self.validation_rules = validation_rules or {
            "required_columns": {"date"},
            "date_format": "%Y-%m-%d",
            "numeric_columns": ["open", "high", "low", "close"],
        }

    def process(self, raw_data: pd.DataFrame) -> pd.DataFrame:
        """Run validate → clean → transform and return the result.

        Raises:
            DataProcessingError: If any stage fails.
        """
        try:
            self.validate_structure(raw_data)
            cleaned = self.clean_data(raw_data)
            return self.transform_data(cleaned)
        except (DataValidationError, DataProcessingError, ValueError, KeyError, AttributeError) as e:
            raise DataProcessingError(f"Data processing failed: {str(e)}") from e

    def validate_structure(self, data: pd.DataFrame) -> pd.DataFrame:
        """Raise DataValidationError if *data* is not a DataFrame or is missing required columns."""
        if not isinstance(data, pd.DataFrame):
            raise DataValidationError("Input must be a pandas DataFrame")
        missing = set(self.validation_rules["required_columns"]) - set(data.columns)
        if missing:
            raise DataValidationError(f"Missing required columns: {missing}")
        return data

    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Drop duplicate dates, parse the date column, and forward-fill numeric gaps."""
        cleaned = data.copy()
        cleaned = cleaned.drop_duplicates(subset=["date"])
        cleaned["date"] = pd.to_datetime(
            cleaned["date"],
            format=self.validation_rules["date_format"],
            errors="coerce",
        )
        for col in self.validation_rules["numeric_columns"]:
            if col in cleaned.columns:
                cleaned[col] = cleaned[col].ffill().bfill()
        return cleaned

    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Add *daily_return* column (when OHLC columns present) and sort by date."""
        transformed = data.copy()
        if {"open", "close"}.issubset(transformed.columns):
            transformed["daily_return"] = (
                (transformed["close"] - transformed["open"]) / transformed["open"]
            )
        return transformed.sort_values(by="date")


class OHLCProcessor:
    """Compute chart display metadata (date ranges, rangebreaks) from an OHLC DataFrame."""

    def __init__(self, ohlc_df: pd.DataFrame) -> None:
        if not isinstance(ohlc_df, pd.DataFrame):
            raise DataValidationError("Input must be a pandas DataFrame")
        self.ohlc_df: pd.DataFrame = ohlc_df.copy() if ohlc_df is not None else pd.DataFrame()
        self.dt_breaks: list[str] = []

    def calculate_date_ranges(self) -> OHLCProcessor:
        """Populate *dt_all*, *dt_obs*, and *dt_breaks* from the date column.

        Returns self to support method chaining.

        Raises:
            DataProcessingError: If date parsing fails.
        """
        if self.ohlc_df.empty or "date" not in self.ohlc_df.columns:
            self.dt_all: pd.DatetimeIndex = pd.DatetimeIndex([])
            self.dt_obs: list[str] = []
            self.dt_breaks = []
            return self

        try:
            dates = pd.to_datetime(self.ohlc_df["date"], errors="coerce")
            valid_dates = dates.dropna()

            if valid_dates.empty:
                self.dt_all = pd.DatetimeIndex([])
                self.dt_obs = []
                self.dt_breaks = []
                return self

            self.dt_all = pd.date_range(start=valid_dates.min(), end=valid_dates.max())
            self.dt_obs = valid_dates.dt.strftime("%Y-%m-%d").tolist()
            self.dt_breaks = [
                d for d in self.dt_all.strftime("%Y-%m-%d").tolist()
                if d not in self.dt_obs
            ]
            return self
        except (ValueError, TypeError, pd.errors.ParserError) as e:
            raise DataProcessingError(f"Error calculating date ranges: {str(e)}") from e

    def get_rangebreaks(self) -> list[dict[str, list[str]]]:
        """Return a Plotly *rangebreaks* config list that excludes missing trading dates."""
        return [{"values": self.dt_breaks}] if self.dt_breaks else []
