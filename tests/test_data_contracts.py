import pytest
import pandas as pd
import numpy as np
from datetime import datetime
from pydantic import ValidationError

from data_contracts import (
    _validate_dataframe,
    FetchingContract,
    ProcessingContract,
    AnalysisContract,
    VisualizationContract,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ohlc_df(**kwargs):
    """Build a minimal valid OHLC DataFrame."""
    defaults = {
        'date': pd.to_datetime(['2024-01-01', '2024-01-02']),
        'open': [100.0, 101.0],
        'high': [105.0, 106.0],
        'low': [99.0, 100.0],
        'close': [103.0, 104.0],
    }
    defaults.update(kwargs)
    return pd.DataFrame(defaults)


def _fetching_kwargs(**kwargs):
    defaults = dict(market='ES', start_date='2024-01-01', end_date='2024-12-31')
    defaults.update(kwargs)
    return defaults


# ---------------------------------------------------------------------------
# _validate_dataframe
# ---------------------------------------------------------------------------

class TestValidateDataframe:
    def test_none_returns_none(self):
        assert _validate_dataframe(None, 'raw_data') is None

    def test_valid_df_passes_through(self):
        df = _ohlc_df()
        result = _validate_dataframe(df, 'raw_data')
        assert isinstance(result, pd.DataFrame)
        assert list(result.columns) == list(df.columns)

    def test_serialized_dict_deserialized(self):
        df = _ohlc_df()
        serialized = df.reset_index().to_dict(orient='split')
        serialized['_is_dataframe'] = True
        result = _validate_dataframe(serialized, 'raw_data')
        assert isinstance(result, pd.DataFrame)

    def test_plain_dict_converted(self):
        d = {
            'date': ['2024-01-01', '2024-01-02'],
            'open': [1.0, 2.0],
            'high': [1.5, 2.5],
            'low': [0.5, 1.5],
            'close': [1.2, 2.2],
        }
        result = _validate_dataframe(d, 'raw_data')
        assert isinstance(result, pd.DataFrame)
        assert 'open' in result.columns

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="must be a pandas DataFrame"):
            _validate_dataframe(42, 'raw_data')

    def test_missing_columns_filled_with_defaults(self):
        df = pd.DataFrame({'date': pd.to_datetime(['2024-01-01'])})
        result = _validate_dataframe(df, 'raw_data')
        for col in ('open', 'high', 'low', 'close'):
            assert col in result.columns
            assert result[col].iloc[0] == 0.0

    def test_date_string_coerced_to_datetime(self):
        df = _ohlc_df()
        df['date'] = df['date'].astype(str)
        result = _validate_dataframe(df, 'raw_data')
        assert pd.api.types.is_datetime64_any_dtype(result['date'])

    def test_bad_date_column_raises(self):
        df = _ohlc_df()
        df['date'] = 'not-a-date'
        with pytest.raises(ValueError, match="'date' column"):
            _validate_dataframe(df, 'raw_data')


# ---------------------------------------------------------------------------
# FetchingContract
# ---------------------------------------------------------------------------

class TestFetchingContract:
    def test_valid_creation_no_data(self):
        c = FetchingContract(**_fetching_kwargs())
        assert c.market == 'ES'
        assert c.raw_data is None

    def test_market_normalized_to_uppercase(self):
        c = FetchingContract(**_fetching_kwargs(market='es'))
        assert c.market == 'ES'

    def test_empty_market_raises(self):
        with pytest.raises(ValidationError):
            FetchingContract(**_fetching_kwargs(market=''))

    def test_invalid_date_format_raises(self):
        with pytest.raises(ValidationError):
            FetchingContract(**_fetching_kwargs(start_date='not-a-date'))

    def test_raw_data_dataframe_accepted(self):
        c = FetchingContract(**_fetching_kwargs(raw_data=_ohlc_df()))
        assert isinstance(c.raw_data, pd.DataFrame)

    def test_to_dict_roundtrip(self):
        c = FetchingContract(**_fetching_kwargs(raw_data=_ohlc_df()))
        d = c.to_dict()
        assert d['market'] == 'ES'
        assert d['raw_data']['_is_dataframe'] is True

    def test_from_dict_with_list_raw_data(self):
        data = _fetching_kwargs()
        data['raw_data'] = [{'date': '2024-01-01', 'open': 1, 'high': 2, 'low': 0, 'close': 1}]
        c = FetchingContract.from_dict(data)
        assert isinstance(c.raw_data, pd.DataFrame)


# ---------------------------------------------------------------------------
# ProcessingContract
# ---------------------------------------------------------------------------

class TestProcessingContract:
    def test_valid_creation(self):
        c = ProcessingContract(raw_data=_ohlc_df())
        assert isinstance(c.raw_data, pd.DataFrame)

    def test_raw_data_required(self):
        with pytest.raises((ValidationError, TypeError)):
            ProcessingContract()

    def test_serialized_dict_accepted_as_raw_data(self):
        df = _ohlc_df()
        serialized = df.reset_index().to_dict(orient='split')
        serialized['_is_dataframe'] = True
        c = ProcessingContract(raw_data=serialized)
        assert isinstance(c.raw_data, pd.DataFrame)


# ---------------------------------------------------------------------------
# AnalysisContract
# ---------------------------------------------------------------------------

class TestAnalysisContract:
    def test_valid_creation(self):
        c = AnalysisContract(processed_data=_ohlc_df())
        assert isinstance(c.processed_data, pd.DataFrame)

    def test_processed_data_required(self):
        with pytest.raises((ValidationError, TypeError)):
            AnalysisContract()

    def test_missing_columns_filled(self):
        df = pd.DataFrame({'date': pd.to_datetime(['2024-01-01'])})
        c = AnalysisContract(processed_data=df)
        assert 'open' in c.processed_data.columns


# ---------------------------------------------------------------------------
# VisualizationContract
# ---------------------------------------------------------------------------

class TestVisualizationContract:
    def test_valid_creation(self):
        c = VisualizationContract(analysis_results={'key': 'value'})
        assert c.analysis_results == {'key': 'value'}

    def test_empty_results_raises(self):
        with pytest.raises(ValidationError):
            VisualizationContract(analysis_results={})

    def test_to_dict_serializable(self):
        c = VisualizationContract(
            analysis_results={'key': 'value'},
            summaries={'15y': 'text'},
        )
        d = c.to_dict()
        assert d['analysis_results'] == {'key': 'value'}
        assert d['summaries'] == {'15y': 'text'}
