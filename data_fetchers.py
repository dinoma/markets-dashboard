from __future__ import annotations

import logging
import os
import re
from datetime import timedelta
from enum import Enum
from functools import lru_cache
from typing import Any, Optional
from urllib.parse import urlparse

import pandas as pd
from dateutil import parser
from sqlalchemy import create_engine, exc as sa_exc, text

from app.config import db_path_str
from data_processor import DataProcessingError

logger = logging.getLogger(__name__)


class TableType(Enum):
    OHLC = "ohlc"
    OHLC_SEASONALITY = "ohlc_seasonality"
    COT = "cot"
    CORRELATION = "correlation"


class TableNameFactory:
    """Centralised factory for creating and validating table names."""

    @staticmethod
    def get_ohlc_table(market: str) -> str:
        """Return the OHLC table name for *market*."""
        return f"{market.lower().replace(' ', '_')}_ohlc"

    @staticmethod
    def get_seasonality_table(market: str, years: int) -> str:
        """Return the seasonality table name for *market* over *years* years."""
        return f"{market.lower().replace(' ', '_')}_ohlc_seasonality_{years}_years"

    @staticmethod
    def get_cot_table(market: str, report_type: str, table_suffix: str) -> str:
        """Return the COT table name for *market*, *report_type*, and *table_suffix*."""
        return f"{market.lower().replace(' ', '_')}_cot_{report_type}_{table_suffix}"

    @staticmethod
    def get_correlation_table(timeframe: str, unit: str) -> str:
        """Return the correlation table name for *timeframe* and *unit*."""
        return f"correlation_{timeframe}_{unit}"

    @classmethod
    def validate_table_name(cls, table_name: str) -> bool:
        """Return True if *table_name* matches a known safe pattern."""
        patterns = [
            r"^[a-z0-9_]+_ohlc$",
            r"^[a-z0-9_]+_ohlc_seasonality_\d+_years$",
            r"^[a-z0-9_]+_cot_(disaggregated|legacy|tff)_(combined|futures_only)(_calc)?$",
            r"^correlation_\d+_(days|years)$",
        ]
        return any(re.match(pattern, table_name) for pattern in patterns)


db_url = os.environ.get(db_path_str).replace("postgres://", "postgresql+psycopg2://", 1)
engine = create_engine(db_url)


@lru_cache(maxsize=10)
def fetch_ohlc_data_cached(
    market: str, start_date: str, end_date: str
) -> pd.DataFrame:
    """Return cached OHLC data for *market* between *start_date* and *end_date*."""
    return OHLCDataFetcher.fetch_ohlc_data_by_range(market, start_date, end_date)


@lru_cache(maxsize=10)
def fetch_active_subplot_data(
    market: str,
    year: int,
    subplot: str,
    table_suffix: str,
    report_type: str,
) -> pd.DataFrame:
    """Return cached subplot data for the given parameters."""
    config = ReportDataFetcher.CONFIG_REGISTRY.get(subplot, {}).get(report_type)
    if not config:
        return pd.DataFrame()
    return ReportDataFetcher(config).fetch(market, year, table_suffix, report_type)


class BaseDataFetcher:
    """Base class providing shared DB fetch and validation utilities."""

    @staticmethod
    def fetch_active_subplot_data(
        market: str,
        year: int,
        subplot: str,
        table_suffix: str,
        report_type: str,
    ) -> pd.DataFrame:
        """Fetch subplot data, returning an empty DataFrame when config is missing."""
        config = ReportDataFetcher.CONFIG_REGISTRY.get(subplot, {}).get(report_type)
        if not config:
            return pd.DataFrame()
        return ReportDataFetcher(config).fetch(market, year, table_suffix, report_type)

    @staticmethod
    def fetch_seasonal_data_cached(
        market: str, years: int, base_year: int
    ) -> pd.DataFrame:
        """Return seasonal data for *market* over *years* years aligned to *base_year*."""
        return SeasonalDataFetcher.fetch_seasonal_data(market, years, base_year)

    @staticmethod
    def fetch_ohlc_data_by_range(
        market: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Return OHLC data for *market* within [*start_date*, *end_date*]."""
        table_name = TableNameFactory.get_ohlc_table(market)
        BaseDataFetcher.validate_table_name(table_name)
        logger.debug("Fetching OHLC from %s for %s to %s", table_name, start_date, end_date)
        query = f"SELECT * FROM {table_name} WHERE date BETWEEN :start_date AND :end_date"
        params = {"start_date": f"{start_date} 00:00:00", "end_date": f"{end_date} 23:59:59"}
        return OHLCDataFetcher.fetch_data(query, params)

    @staticmethod
    def fetch_data(
        query: str, params: Optional[dict[str, Any]] = None
    ) -> pd.DataFrame:
        """Execute *query* with *params* and return the result as a DataFrame.

        Raises:
            ValueError: If *params* is not a dict, or the table name is unsafe.
            RuntimeError: If a SQLAlchemy error occurs.
        """
        from sqlalchemy import bindparam

        if params and not isinstance(params, dict):
            raise ValueError("Query parameters must be in dictionary format")
        BaseDataFetcher.validate_table_name_from_query(query.lower())

        with engine.connect() as connection:
            try:
                stmt = text(query)
                if params:
                    stmt = stmt.bindparams(
                        *[bindparam(key, value) for key, value in params.items()]
                    )
                df = pd.read_sql(stmt, connection)
            except sa_exc.SQLAlchemyError as e:
                raise RuntimeError(f"Database error: {str(e)}") from e

        return BaseDataFetcher.common_processing(df)

    @staticmethod
    def common_processing(df: pd.DataFrame) -> pd.DataFrame:
        """Parse date columns, sort, and coerce OHLC columns to float.

        Raises:
            DataProcessingError: If *df* is not a DataFrame or processing fails.
        """
        if not isinstance(df, pd.DataFrame):
            raise DataProcessingError("Input must be a pandas DataFrame")
        if df.empty:
            return df

        try:
            processed = df.copy()
            for col in ("date", "report_date_as_yyyy_mm_dd"):
                if col in processed.columns:
                    processed[col] = pd.to_datetime(processed[col], errors="coerce")
            if "date" in processed.columns:
                processed = processed.sort_values(by="date")
            if "report_date_as_yyyy_mm_dd" in processed.columns:
                processed = processed.sort_values(by="report_date_as_yyyy_mm_dd")
            for col in ("open", "high", "low", "close"):
                if col in processed.columns:
                    processed[col] = (
                        processed[col]
                        .astype(str)
                        .str.replace(",", "")
                        .replace("", "0")
                        .astype(float)
                    )
            return processed
        except (ValueError, TypeError, AttributeError) as e:
            raise DataProcessingError(f"Error in common processing: {str(e)}") from e

    @staticmethod
    def validate_table_name(table_name: str) -> bool:
        """Raise ValueError if *table_name* does not match a safe pattern."""
        pattern = re.compile(
            r"^[a-z0-9_]+_ohlc$"
            r"|^[a-z0-9_]+_ohlc_seasonality_\d+_years$"
            r"|^[a-z0-9_]+_cot_(disaggregated|legacy|tff)_(combined|futures_only)(_calc)?$"
            r"|^correlation_\d+_(days|years)$"
        )
        if not pattern.match(table_name):
            raise ValueError(f"Invalid table name: {table_name}")
        return True

    @staticmethod
    def validate_table_name_from_query(query: str) -> None:
        """Validate all table names referenced in *query*."""
        for _, table in re.findall(r"\b(from|join)\s+([a-z0-9_]+)", query, re.IGNORECASE):
            BaseDataFetcher.validate_table_name(table)


class SeasonalDataFetcher(BaseDataFetcher):
    """Data fetcher for seasonal market data."""

    @staticmethod
    def fetch_seasonal_data(
        market: str, years: int, current_year: int
    ) -> pd.DataFrame:
        """Return seasonal data for *market* over *years* years, dates aligned to *current_year*."""
        table_name = TableNameFactory.get_seasonality_table(market, years)
        BaseDataFetcher.validate_table_name(table_name)
        query = "SELECT * FROM {} ORDER BY day_of_year ASC".format(table_name)
        df = SeasonalDataFetcher.fetch_data(query)

        if not df.empty:
            df["day_of_year"] = df["day_of_year"].astype(int)
            base_date = parser.parse(f"{current_year}-01-01")
            df["date"] = df["day_of_year"].apply(
                lambda x: (base_date + timedelta(days=x - 1)).strftime("%Y-%m-%d")
            )
        return df


class OHLCDataFetcher(BaseDataFetcher):
    """Data fetcher for OHLC (Open, High, Low, Close) data."""

    @staticmethod
    def fetch_ohlc_data(market: str, year: int) -> pd.DataFrame:
        """Return OHLC data for *market* in *year*."""
        table_name = TableNameFactory.get_ohlc_table(market)
        BaseDataFetcher.validate_table_name(table_name)
        logger.debug("Fetching OHLC from %s for year %s", table_name, year)
        query = "SELECT * FROM {} WHERE Date BETWEEN :start_date AND :end_date".format(table_name)
        params = {
            "start_date": f"{year}-01-01 00:00:00",
            "end_date": f"{year}-12-31 23:59:59",
        }
        return OHLCDataFetcher.fetch_data(query, params)

    @staticmethod
    def fetch_ohlc_data_by_range(
        market: str, start_date: str, end_date: str
    ) -> pd.DataFrame:
        """Return OHLC data for *market* within [*start_date*, *end_date*]."""
        table_name = TableNameFactory.get_ohlc_table(market)
        BaseDataFetcher.validate_table_name(table_name)
        logger.debug("Fetching OHLC from %s for %s to %s", table_name, start_date, end_date)
        query = "SELECT * FROM {} WHERE date BETWEEN :start_date AND :end_date".format(table_name)
        params = {"start_date": f"{start_date} 00:00:00", "end_date": f"{end_date} 23:59:59"}
        return OHLCDataFetcher.fetch_data(query, params)


class ReportDataFetcher(BaseDataFetcher):
    """Generic data fetcher for COT report data with configurable column sets."""

    CONFIG_REGISTRY: dict[str, dict[str, dict[str, Any]]] = {
        "Open Interest": {
            "legacy": {"columns": "report_date_as_yyyy_mm_dd, open_interest_all"},
            "disaggregated": {"columns": "report_date_as_yyyy_mm_dd, open_interest_all"},
            "tff": {"columns": "report_date_as_yyyy_mm_dd, open_interest_all"},
        },
        "OI Percentages": {
            "legacy": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, pct_of_oi_noncomm_long_all, "
                    "pct_of_oi_noncomm_short_all, pct_of_oi_comm_long_all, "
                    "pct_of_oi_comm_short_all"
                ),
                "numeric_cols": [
                    "pct_of_oi_noncomm_long_all", "pct_of_oi_noncomm_short_all",
                    "pct_of_oi_comm_long_all", "pct_of_oi_comm_short_all",
                ],
            },
            "disaggregated": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, pct_of_oi_m_money_long_all, "
                    "pct_of_oi_m_money_short_all, pct_of_oi_prod_merc_long, "
                    "pct_of_oi_prod_merc_short, pct_of_oi_swap_long_all, "
                    "pct_of_oi_swap_short_all"
                ),
                "numeric_cols": [
                    "pct_of_oi_m_money_long_all", "pct_of_oi_m_money_short_all",
                    "pct_of_oi_prod_merc_long", "pct_of_oi_prod_merc_short",
                    "pct_of_oi_swap_long_all", "pct_of_oi_swap_short_all",
                ],
            },
            "tff": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, pct_of_oi_lev_money_long, "
                    "pct_of_oi_lev_money_short, pct_of_oi_asset_mgr_long, "
                    "pct_of_oi_asset_mgr_short, pct_of_oi_dealer_long_all, "
                    "pct_of_oi_dealer_short_all"
                ),
                "numeric_cols": [
                    "pct_of_oi_lev_money_long", "pct_of_oi_lev_money_short",
                    "pct_of_oi_asset_mgr_long", "pct_of_oi_asset_mgr_short",
                    "pct_of_oi_dealer_long_all", "pct_of_oi_dealer_short_all",
                ],
            },
        },
        "Positions Change": {
            "legacy": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, pct_change_noncomm_long, "
                    "pct_change_noncomm_short, pct_change_comm_long, "
                    "pct_change_comm_short"
                ),
                "numeric_cols": [
                    "pct_change_noncomm_long", "pct_change_noncomm_short",
                    "pct_change_comm_long", "pct_change_comm_short",
                ],
            },
            "disaggregated": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, pct_change_m_money_long, "
                    "pct_change_m_money_short, pct_change_prod_merc_long, "
                    "pct_change_prod_merc_short, pct_change_swap_long, "
                    "pct_change_swap_short"
                ),
                "numeric_cols": [
                    "pct_change_m_money_long", "pct_change_m_money_short",
                    "pct_change_prod_merc_long", "pct_change_prod_merc_short",
                    "pct_change_swap_long", "pct_change_swap_short",
                ],
            },
            "tff": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, pct_change_lev_money_long, "
                    "pct_change_lev_money_short, pct_change_asset_mgr_long, "
                    "pct_change_asset_mgr_short, pct_change_dealer_long, "
                    "pct_change_dealer_short"
                ),
                "numeric_cols": [
                    "pct_change_lev_money_long", "pct_change_lev_money_short",
                    "pct_change_asset_mgr_long", "pct_change_asset_mgr_short",
                    "pct_change_dealer_long", "pct_change_dealer_short",
                ],
            },
        },
        "Net Positions": {
            "legacy": {
                "columns": "report_date_as_yyyy_mm_dd, noncomm_net_positions, comm_net_positions",
                "numeric_cols": ["report_date_as_yyyy_mm_dd", "noncomm_net_positions", "comm_net_positions"],
            },
            "disaggregated": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, "
                    "m_money_net_positions, prod_merc_net_positions, swap_net_positions"
                ),
                "numeric_cols": [
                    "report_date_as_yyyy_mm_dd",
                    "m_money_net_positions", "prod_merc_net_positions", "swap_net_positions",
                ],
            },
            "tff": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, "
                    "lev_money_net_positions, asset_mgr_net_positions, dealer_net_positions"
                ),
                "numeric_cols": [
                    "report_date_as_yyyy_mm_dd",
                    "lev_money_net_positions", "asset_mgr_net_positions", "dealer_net_positions",
                ],
            },
        },
        "Net Positions Change": {
            "legacy": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, pct_change_noncomm_net_positions, "
                    "pct_change_comm_net_positions"
                ),
                "numeric_cols": [
                    "report_date_as_yyyy_mm_dd",
                    "pct_change_noncomm_net_positions", "pct_change_comm_net_positions",
                ],
            },
            "disaggregated": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, "
                    "pct_change_m_money_net_positions, pct_change_prod_merc_net_positions, "
                    "pct_change_swap_net_positions"
                ),
                "numeric_cols": [
                    "report_date_as_yyyy_mm_dd",
                    "pct_change_m_money_net_positions",
                    "pct_change_prod_merc_net_positions",
                    "pct_change_swap_net_positions",
                ],
            },
            "tff": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, "
                    "pct_change_lev_money_net_positions, pct_change_asset_mgr_net_positions, "
                    "pct_change_dealer_net_positions"
                ),
                "numeric_cols": [
                    "report_date_as_yyyy_mm_dd",
                    "pct_change_lev_money_net_positions",
                    "pct_change_asset_mgr_net_positions",
                    "pct_change_dealer_net_positions",
                ],
            },
        },
        "26W Index": {
            "legacy": {
                "columns": "report_date_as_yyyy_mm_dd, noncomm_26w_index, comm_26w_index",
                "numeric_cols": ["report_date_as_yyyy_mm_dd", "noncomm_26w_index", "comm_26w_index"],
            },
            "disaggregated": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, "
                    "m_money_26w_index, prod_merc_26w_index, swap_26w_index"
                ),
                "numeric_cols": [
                    "report_date_as_yyyy_mm_dd",
                    "m_money_26w_index", "prod_merc_26w_index", "swap_26w_index",
                ],
            },
            "tff": {
                "columns": (
                    "report_date_as_yyyy_mm_dd, "
                    "lev_money_26w_index, asset_mgr_26w_index, dealer_26w_index"
                ),
                "numeric_cols": [
                    "report_date_as_yyyy_mm_dd",
                    "lev_money_26w_index", "asset_mgr_26w_index", "dealer_26w_index",
                ],
            },
        },
    }

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def fetch(
        self,
        market: str,
        year: int,
        table_suffix: str,
        report_type: str,
    ) -> pd.DataFrame:
        """Return COT report data for *market* and *year* using this fetcher's column config."""
        table_name = TableNameFactory.get_cot_table(market, report_type, table_suffix)
        BaseDataFetcher.validate_table_name(table_name)
        query = f"""
        SELECT {self.config['columns']}
        FROM {table_name}
        WHERE report_date_as_yyyy_mm_dd BETWEEN :start_date AND :end_date
        """
        params = {"start_date": f"{year}-01-01", "end_date": f"{year + 1}-01-01"}
        df = self.fetch_data(query, params)

        if not df.empty:
            if "report_date_as_yyyy_mm_dd" in df.columns:
                df["date"] = df["report_date_as_yyyy_mm_dd"]
            else:
                logger.warning("'report_date_as_yyyy_mm_dd' column missing; 'date' column not created")
        return df


class CorrelationDataFetcher(BaseDataFetcher):
    """Data fetcher for cross-market correlation data."""

    @staticmethod
    def fetch_correlation_data(table_name: str) -> pd.DataFrame:
        """Return all rows from *table_name* as a DataFrame."""
        BaseDataFetcher.validate_table_name(table_name)
        query = "SELECT * FROM {}".format(table_name)
        df = CorrelationDataFetcher.fetch_data(query)
        if df.empty:
            logger.warning("No data found in %s", table_name)
        else:
            logger.debug("Fetched correlation data from %s: shape=%s", table_name, df.shape)
        return df
