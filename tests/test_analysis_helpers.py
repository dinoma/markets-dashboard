"""Tests for module-level helper functions in callbacks.py.

callbacks.py imports callback_helpers which contains a pre-existing syntax error
incompatible with Python < 3.12.  We stub that module (and other heavy
dependencies) at the sys.modules level before importing the helpers under test.
"""
import sys
import types
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Stub heavy / broken modules before touching callbacks.py
# ---------------------------------------------------------------------------

def _make_stub(name):
    mod = types.ModuleType(name)
    sys.modules[name] = mod
    return mod


_STUBS = [
    'callback_helpers',
    'app', 'app.config', 'app.error_logging',
    'navigation_service',
    'state_managers',
    'queues',
    'real_data_fetcher',
    'range_filter',
    'layout_definitions',
    'data_fetchers',
    'visualizers', 'visualizers.table_visualizer', 'visualizers.distribution_visualizer',
    'plotly', 'plotly.subplots',
    'dotenv',
    'sqlalchemy', 'sqlalchemy.exc',
]

_stub_mods = {}
for _name in _STUBS:
    if _name not in sys.modules:
        _stub_mods[_name] = _make_stub(_name)

# Minimal attribute setup for stubs that are accessed at import time
_cfg = sys.modules.get('app.config') or _stub_mods.get('app.config')
for _attr in ('CANDLESTICK_CONFIG', 'SEASONALITY_CONFIG', 'POSITION_CHANGE_CONFIG',
              'COLORS', 'TRACE_CONFIG', 'market_tickers', 'DEFAULT_MARKET', 'db_path_str'):
    if not hasattr(_cfg, _attr):
        setattr(_cfg, _attr, {} if _attr not in ('DEFAULT_MARKET', 'db_path_str') else 'TEST')

_cfg_cls = MagicMock()
_cfg.Config = _cfg_cls

_err = sys.modules.get('app.error_logging') or _stub_mods.get('app.error_logging')
_err.log_error = MagicMock()

_nav = sys.modules.get('navigation_service') or _stub_mods.get('navigation_service')
_NavSvc = MagicMock(return_value=MagicMock())
_nav.NavigationService = _NavSvc

_dotenv = sys.modules.get('dotenv') or _stub_mods.get('dotenv')
_dotenv.load_dotenv = MagicMock()

_cb_helpers = sys.modules.get('callback_helpers') or _stub_mods.get('callback_helpers')
_cb_helpers.AnnotationManager = MagicMock()
_cb_helpers.add_trace = MagicMock()
_cb_helpers.calculate_risk_metrics = MagicMock()
_cb_helpers.create_cumulative_return_charts = MagicMock()
_cb_helpers.get_market_by_index = MagicMock()
_cb_helpers.perform_analysis = MagicMock()
_cb_helpers.update_risk_metrics_summary = MagicMock()

_rl = sys.modules.get('real_data_fetcher') or _stub_mods.get('real_data_fetcher')
for _cls in ('RealDataFetcher', 'SubplotFetcher', 'SeasonalityFetcher', 'OHLCFetcher'):
    setattr(_rl, _cls, MagicMock())

_df = sys.modules.get('data_fetchers') or _stub_mods.get('data_fetchers')
_df.fetch_ohlc_data_cached = MagicMock()
_df.fetch_active_subplot_data = MagicMock()

_rf = sys.modules.get('range_filter') or _stub_mods.get('range_filter')
_rf.RangeFilter = MagicMock()

_ld = sys.modules.get('layout_definitions') or _stub_mods.get('layout_definitions')
_ld.format_market_name = MagicMock()

_tv = sys.modules.get('visualizers.table_visualizer') or _stub_mods.get('visualizers.table_visualizer')
_tv.TableVisualizer = MagicMock()
_dv = sys.modules.get('visualizers.distribution_visualizer') or _stub_mods.get('visualizers.distribution_visualizer')
_dv.DistributionChartVisualizer = MagicMock()

_sm = sys.modules.get('state_managers') or _stub_mods.get('state_managers')
for _cls in ('RangeManager', 'ViewportHandler', 'InteractionTracker'):
    setattr(_sm, _cls, MagicMock())

_qu = sys.modules.get('queues') or _stub_mods.get('queues')
for _cls in ('QueueManager', 'FetchingQueue', 'ProcessingQueue', 'AnalysisQueue', 'VisualizationQueue'):
    setattr(_qu, _cls, MagicMock())

_ps = sys.modules.get('plotly.subplots') or _stub_mods.get('plotly.subplots')
_ps.make_subplots = MagicMock()

# ---------------------------------------------------------------------------
# Now import helpers under test
# ---------------------------------------------------------------------------

try:
    from callbacks import (
        _empty_analysis_outputs,
        _fetch_ohlc_for_years,
        _run_analysis,
        _prepare_day_trading_tables,
    )
    _IMPORT_OK = True
except Exception as exc:
    _IMPORT_OK = False
    _IMPORT_EXC = exc

pytestmark = pytest.mark.skipif(
    not _IMPORT_OK,
    reason=f"callbacks.py not importable: {'' if _IMPORT_OK else _IMPORT_EXC}",
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stats_df(years=None):
    """Build a minimal day-trading stats DataFrame."""
    years = years or [2022, 2023, 2024]
    rows = [{'year': str(y), 'trades': 10, 'wins': 6} for y in years]
    rows.append({'year': 'Total', 'trades': 30, 'wins': 18})
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# _empty_analysis_outputs
# ---------------------------------------------------------------------------

class TestEmptyAnalysisOutputs:
    def test_returns_57_elements(self):
        outputs = _empty_analysis_outputs()
        assert len(outputs) == 57

    def test_first_element_is_empty_list(self):
        outputs = _empty_analysis_outputs()
        assert outputs[0] == []

    def test_second_and_third_are_no_data_strings(self):
        outputs = _empty_analysis_outputs()
        assert outputs[1] == "No data available"
        assert outputs[2] == "No data available"

    def test_day_trading_table_slots_are_empty_lists(self):
        outputs = _empty_analysis_outputs()
        # slots 13-16 (0-indexed) are the 4 day-trading table data slots
        for idx in range(13, 17):
            assert outputs[idx] == []


# ---------------------------------------------------------------------------
# _fetch_ohlc_for_years
# ---------------------------------------------------------------------------

class TestFetchOhlcForYears:
    def test_returns_empty_df_when_no_data(self):
        with patch('callbacks.fetch_ohlc_data_cached', return_value=pd.DataFrame()):
            result = _fetch_ohlc_for_years('ES', [1, 2], 3, 1, 12, 31)
        assert isinstance(result, pd.DataFrame)
        assert result.empty

    def test_concatenates_data_across_years(self):
        df_year = pd.DataFrame({'date': ['2023-03-01'], 'close': [100.0]})
        with patch('callbacks.fetch_ohlc_data_cached', return_value=df_year):
            result = _fetch_ohlc_for_years('ES', [1, 2], 3, 1, 12, 31)
        assert len(result) == 2  # one row per year offset

    def test_fallback_skips_invalid_years(self):
        call_log = []

        def fake_fetch(market, start, end):
            call_log.append(start[:4])
            return pd.DataFrame()

        with patch('callbacks.fetch_ohlc_data_cached', side_effect=fake_fetch):
            _fetch_ohlc_for_years('ES', [0], 3, 1, 12, 31)

        # All attempted years should be >= MIN_MARKET_DATA_YEAR
        from constants import MIN_MARKET_DATA_YEAR
        for year_str in call_log:
            assert int(year_str) >= MIN_MARKET_DATA_YEAR


# ---------------------------------------------------------------------------
# _run_analysis
# ---------------------------------------------------------------------------

class TestRunAnalysis:
    def test_delegates_to_perform_analysis(self):
        df = pd.DataFrame({'date': pd.to_datetime(['2024-01-01']), 'close': [100.0]})
        expected = {'yearly_results': [], '15_year_summary': {}}
        with patch('callbacks.perform_analysis', return_value=expected) as mock_pa:
            result = _run_analysis(df, '2024-03-01', '2024-12-31', 'Long')
        mock_pa.assert_called_once_with('2024-03-01', '2024-12-31', 'Long', df)
        assert result is expected


# ---------------------------------------------------------------------------
# _prepare_day_trading_tables
# ---------------------------------------------------------------------------

class TestPrepareDayTradingTables:
    def _mock_analysis_results(self):
        df = _stats_df()
        return {
            'day_trading_stats': df.copy(),
            'day_trading_stats_1': df.copy(),
            'day_trading_stats_weekday': df.copy(),
            'day_trading_stats_1_weekday': df.copy(),
        }

    def test_returns_four_items(self):
        tv = MagicMock()
        tv.render_day_trading_stats.return_value = []
        result = _prepare_day_trading_tables(self._mock_analysis_results(), tv)
        assert len(result) == 4

    def test_total_row_appended_at_end(self):
        captured = []

        def capture(df):
            captured.append(df.copy())
            return []

        tv = MagicMock()
        tv.render_day_trading_stats.side_effect = capture
        _prepare_day_trading_tables(self._mock_analysis_results(), tv)

        # First two DataFrames (stats and stats_1) should have Total last
        for df in captured[:2]:
            assert df.iloc[-1]['year'] == 'Total'

    def test_numeric_years_sorted_descending(self):
        captured = []

        def capture(df):
            captured.append(df.copy())
            return []

        tv = MagicMock()
        tv.render_day_trading_stats.side_effect = capture
        _prepare_day_trading_tables(self._mock_analysis_results(), tv)

        for df in captured[:2]:
            numeric_rows = df[df['year'] != 'Total']
            years = numeric_rows['year'].astype(int).tolist()
            assert years == sorted(years, reverse=True)
