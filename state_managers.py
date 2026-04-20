from __future__ import annotations

from typing import Any, Optional
import pandas as pd
from plotly import graph_objects as go
from constants import OHLC_Q1_THRESHOLD, OHLC_Q2_THRESHOLD, RANGE_BUFFER_DAYS, HOVER_DISTANCE_PX


class InteractionTracker:
    """Track and manage user hover/click interactions with chart elements.

    Attributes:
        hover_state: Most-recent hover position and data points.
        click_state: Most-recent click position and data points.
    """

    def __init__(self) -> None:
        self.hover_state: dict[str, Any] = {}
        self.click_state: dict[str, Any] = {}

    def configure_hover(self, fig: go.Figure) -> go.Figure:
        """Apply unified hover settings to *fig* and return it."""
        fig.update_layout(
            hovermode="x unified",
            hoversubplots="axis",
            spikedistance=-1,
            hoverdistance=HOVER_DISTANCE_PX,
            hoverlabel=dict(
                bgcolor="#1e1e1e",
                font_size=12,
                font_family="'Press Start 2P', monospace",
            ),
        )
        return fig

    def handle_hover(self, hover_data: Optional[dict[str, Any]]) -> None:
        """Update :attr:`hover_state` from a Plotly *hoverData* event payload."""
        if hover_data:
            point = hover_data.get("points", [{}])[0]
            self.hover_state = {
                "x": point.get("x"),
                "y": point.get("y"),
                "points": hover_data.get("points", []),
            }

    def configure_trace_hover(self, fig: go.Figure) -> go.Figure:
        """Apply consistent hover style to every trace in *fig* and return it."""
        fig.update_traces(
            hoverinfo="x+y",
            hoverlabel=dict(
                bgcolor="#1e1e1e",
                font_size=12,
                font_family="'Press Start 2P', monospace",
            ),
            xaxis="x1",
        )
        return fig

    def handle_click(self, click_data: Optional[dict[str, Any]]) -> None:
        """Update :attr:`click_state` from a Plotly *clickData* event payload."""
        if click_data:
            point = click_data.get("points", [{}])[0]
            self.click_state = {
                "x": point.get("x"),
                "y": point.get("y"),
                "points": click_data.get("points", []),
            }


class ViewportHandler:
    """Handle chart viewport (zoom/pan) interactions.

    Coordinates between Plotly relayout events and :class:`RangeManager`
    constraints.  Maintains viewport state between updates and processes
    explicit reset requests.

    Args:
        range_manager: Responsible for axis-range calculations and clamping.
    """

    def __init__(self, range_manager: RangeManager) -> None:
        self.range_manager = range_manager
        self.reset_required: bool = False

    def handle_relayout(
        self,
        relayout_data: Optional[dict[str, Any]],
        reset_required: bool,
    ) -> tuple[list, list]:
        """Dispatch a relayout event and return the resulting (x_range, y_range).

        If *reset_required* is True the viewport is reset to its initial state
        regardless of *relayout_data*.
        """
        self.reset_required = reset_required
        if self.reset_required:
            return self.reset_viewport()
        if relayout_data:
            return self.update_viewport(relayout_data)
        return self.get_current_viewport()

    def update_viewport(self, relayout_data: dict[str, Any]) -> tuple[list, list]:
        """Clamp the new x-range from *relayout_data* and derive the y-range."""
        x_range = self.range_manager.update_x_range(relayout_data)
        y_range = self.range_manager.update_y_range(x_range)
        return x_range, y_range

    def reset_viewport(self) -> tuple[list, list]:
        """Return the initial (x_range, y_range) and clear the reset flag."""
        self.reset_required = False
        return self.range_manager.get_initial_ranges()

    def get_current_viewport(self) -> tuple[list, list]:
        """Return the current viewport ranges without modification."""
        return (
            self.range_manager.initial_x_range,
            self.range_manager.initial_y_range,
        )


class RangeManager:
    """Compute and constrain chart axis ranges from OHLC data.

    Args:
        ohlc_df: DataFrame with at minimum *date*, *low*, and *high* columns.
    """

    def __init__(self, ohlc_df: pd.DataFrame) -> None:
        self.ohlc_df = ohlc_df
        self.buffer_days = RANGE_BUFFER_DAYS
        self._init_ranges()

    def _init_ranges(self) -> None:
        """Compute :attr:`initial_x_range` and :attr:`initial_y_range` from the data."""
        n = len(self.ohlc_df)
        if n < OHLC_Q1_THRESHOLD:
            end_date = pd.Timestamp(f"{self.ohlc_df['date'].dt.year.iloc[0]}-03-31")
        elif n < OHLC_Q2_THRESHOLD:
            end_date = pd.Timestamp(f"{self.ohlc_df['date'].dt.year.iloc[0]}-06-30")
        else:
            end_date = self.ohlc_df["date"].iloc[-1]

        offset = pd.DateOffset(days=self.buffer_days)
        self.initial_x_range: list = [
            self.ohlc_df["date"].iloc[0] - offset,
            end_date + offset,
        ]
        self.initial_y_range: list = [
            self.ohlc_df["low"].min(),
            self.ohlc_df["high"].max(),
        ]

    def get_initial_ranges(self) -> tuple[list, list]:
        """Return the initial (x_range, y_range) tuple."""
        return self.initial_x_range, self.initial_y_range

    def clamp_x_range(
        self, x_range_start: pd.Timestamp, x_range_end: pd.Timestamp
    ) -> list:
        """Return *[start, end]* clamped to the allowable x bounds."""
        return [
            max(self.initial_x_range[0], x_range_start),
            min(self.initial_x_range[1], x_range_end),
        ]

    def compute_y_range(self, filtered_df: pd.DataFrame) -> list:
        """Return *[min_low, max_high]* clamped to the initial y bounds."""
        return [
            max(self.initial_y_range[0], filtered_df["low"].min()),
            min(self.initial_y_range[1], filtered_df["high"].max()),
        ]

    def update_x_range(self, relayout_data: dict[str, Any]) -> list:
        """Extract and clamp the x-range from a Plotly *relayoutData* dict."""
        if "xaxis.range[0]" in relayout_data and "xaxis.range[1]" in relayout_data:
            return self.clamp_x_range(
                pd.Timestamp(relayout_data["xaxis.range[0]"]),
                pd.Timestamp(relayout_data["xaxis.range[1]"]),
            )
        return self.initial_x_range

    def update_y_range(self, x_range: list) -> list:
        """Derive the y-range for the visible data within *x_range*."""
        mask = (self.ohlc_df["date"] >= x_range[0]) & (
            self.ohlc_df["date"] <= x_range[1]
        )
        return self.compute_y_range(self.ohlc_df[mask])
