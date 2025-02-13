import logging
import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

class DistributionChartVisualizer:
    """Handles rendering and styling of distribution charts in the app."""
    
    def __init__(self):
        """Initialize the visualizer with default settings."""
        self.logger = logging.getLogger(__name__)
        self.default_styles = {
            'plot_bgcolor': "#1e1e1e",
            'paper_bgcolor': "#1e1e1e",
            'font': {
                'family': "'Press Start 2P', monospace",
                'size': 10,
                'color': 'white'
            }
        }

    def _validate_data(self, data):
        """Validate input data for distribution charts.
        
        Args:
            data (pd.DataFrame or list): Data to validate.
            
        Returns:
            bool: True if data is valid, False otherwise.
        """
        if data is None:
            self.logger.warning("Input data is None")
            return False
            
        if isinstance(data, pd.DataFrame):
            if data.empty:
                self.logger.warning("Input DataFrame is empty")
                return False
        elif isinstance(data, list):
            if not data:
                self.logger.warning("Input list is empty")
                return False
        else:
            self.logger.warning(f"Invalid data type: {type(data)}")
            return False
            
        return True

    def _create_empty_chart(self, message="No data available"):
        """Create an empty chart with a message.
        
        Args:
            message (str): Message to display on the empty chart.
            
        Returns:
            go.Figure: Empty figure with the message.
        """
        fig = go.Figure()
        fig.update_layout(**self.default_styles)
        fig.add_annotation(
            text=message,
            x=0.5,
            y=0.5,
            showarrow=False,
            font=dict(size=20, color="white")
        )
        return fig

    def calculate_percentiles(self, data, column):
        """Calculate key percentiles for distribution analysis.
        
        Args:
            data (pd.DataFrame): Input data
            column (str): Column name to calculate percentiles on
            
        Returns:
            dict: Percentile values keyed by percentile
        """
        if not self._validate_data(data) or column not in data.columns:
            return {}
            
        return {
            '10': np.percentile(data[column], 10),
            '25': np.percentile(data[column], 25),
            '50': np.percentile(data[column], 50),
            '75': np.percentile(data[column], 75),
            '90': np.percentile(data[column], 90)
        }

    def render_return_distribution(self, data, years=15):
        """Render a return distribution chart.
        
        Args:
            data (pd.DataFrame or list): Data to render.
            years (int): Number of years to include.
            
        Returns:
            go.Figure: The rendered chart.
        """
        if not self._validate_data(data):
            return self._create_empty_chart("Input data is invalid or empty")
            
        # Convert data to DataFrame if it's a list
        if isinstance(data, list):
            try:
                data = pd.DataFrame(data)
                self.logger.info(f"Converted list data to DataFrame with shape: {data.shape}")
            except Exception as e:
                self.logger.error(f"Failed to convert list to DataFrame: {e}")
                return self._create_empty_chart("Data conversion failed")
                
        # Log the received data structure for debugging
        self.logger.info(f"Received data with columns: {data.columns.tolist()}")
        self.logger.info(f"First few rows:\n{data.head()}")
            
        # Determine the correct column name for returns
        return_column = None
        possible_columns = [
            'percent_change', 'returns', 'change', 'pct_change', 
            'close', 'price', 'Closing Percentage', 'closing_percentage'
        ]
        for col in possible_columns:
            if col in data.columns:
                return_column = col
                self.logger.info(f"Using column '{col}' for returns")
                break
                
        if return_column is None:
            self.logger.warning(f"No valid return column found in data. Available columns: {data.columns.tolist()}")
            return self._create_empty_chart(f"No return data found. Available columns: {', '.join(data.columns)}")
            
        # Rename the column to 'returns' for consistency
        if return_column != 'returns':
            data = data.rename(columns={return_column: 'returns'})
            return_column = 'returns'
            
        # Calculate percentiles
        percentiles = self.calculate_percentiles(data, return_column)
        
        # Create figure
        fig = go.Figure()
        
        # Add histogram trace
        fig.add_trace(go.Histogram(
            x=data[return_column],
            name='Returns',
            marker_color='#1f77b4',  # Consistent blue color
            opacity=0.8,
            nbinsx=50,
            histnorm='percent',
            marker_line_width=0
        ))
        
        # Add percentile lines
        for pct, value in percentiles.items():
            fig.add_vline(
                x=value,
                line_dash="dot",
                line_color="#ff7f0e",  # Consistent orange color
                line_width=1.5,
                annotation_text=f"{pct}%",
                annotation_position="top right",
                annotation_font_size=12,
                annotation_font_color="#ff7f0e",
                annotation_bgcolor="rgba(0,0,0,0.7)"
            )
            
        # Update layout
        fig.update_layout(
            title={
                'text': f"{years}-Year Return Distribution",
                'x': 0.5,
                'y': 0.95,
                'xanchor': 'center',
                'yanchor': 'top',
                'font': {
                    'size': 14,
                    'color': 'white'
                }
            },
            xaxis_title="Return (%)",
            yaxis_title="Frequency (%)",
            showlegend=False,
            margin=dict(l=50, r=50, t=80, b=50),
            **self.default_styles
        )
        
        # Update axis styling
        fig.update_xaxes(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.1)',
            zeroline=False,
            title_font=dict(size=12)
        )
        
        fig.update_yaxes(
            showgrid=True,
            gridcolor='rgba(255,255,255,0.1)',
            zeroline=False,
            title_font=dict(size=12)
        )
        
        return fig

    def render_stop_loss_distribution(self, data, years=15):
        """Render a stop-loss distribution chart.
        
        Args:
            data (pd.DataFrame): Data to render.
            years (int): Number of years to include.
            
        Returns:
            go.Figure: The rendered chart.
        """
        if not self._validate_data(data):
            return self._create_empty_chart()
        # Placeholder for implementation
        return go.Figure()

    def render_high_low_distribution(self, data, type="D-UP"):
        """Render a high-low distribution chart.
        
        Args:
            data (pd.DataFrame): Data to render.
            type (str): Type of distribution (e.g., "D-UP", "PD-H").
            
        Returns:
            go.Figure: The rendered chart.
        """
        if not self._validate_data(data):
            return self._create_empty_chart()
        # Placeholder for implementation
        return go.Figure()
