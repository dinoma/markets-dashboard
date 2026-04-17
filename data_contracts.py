import logging
from typing import Dict, Any, Optional
from pydantic import BaseModel, field_validator, ValidationError, ConfigDict
from datetime import datetime
import pandas as pd
import numpy as np
import json
from pydantic_core import core_schema

logger = logging.getLogger(__name__)

class FetchingContract(BaseModel):
    """Standardized contract for data fetching stage with enhanced debugging"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_default=True,
        extra='forbid',
        str_strip_whitespace=True,
        str_min_length=1
    )
    
    market: str
    start_date: datetime
    end_date: datetime
    raw_data: Optional[pd.DataFrame] = None
    metadata: Dict[str, Any] = {}
    
    def __init__(self, **data):
        logger.debug("FetchingContract init keys: %s", list(data.keys()))
        try:
            super().__init__(**data)
            logger.debug("FetchingContract created: market=%s start=%s end=%s", data.get('market'), data.get('start_date'), data.get('end_date'))
        except (ValidationError, ValueError) as e:
            logger.error("FetchingContract creation failed: %s", e)
            raise

    def _debug_print(self):
        """Log contract details at DEBUG level."""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug("FetchingContract: market=%s start=%s end=%s metadata=%s", self.market, self.start_date, self.end_date, self.metadata)
        if self.raw_data is not None:
            logger.debug("raw_data shape=%s columns=%s", self.raw_data.shape, self.raw_data.columns.tolist())
        else:
            logger.debug("raw_data=None")

    @field_validator('market')
    @classmethod
    def validate_market(cls, value: str) -> str:
        """Validate and normalize market name"""
        if not value:
            raise ValueError("Market cannot be empty")
        return value.upper().strip()

    @field_validator('start_date', 'end_date', mode='before')
    @classmethod
    def parse_dates(cls, value) -> datetime:
        """Parse and validate dates"""
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value)
            except ValueError as e:
                raise ValueError(f"Invalid date format: {value}. Expected ISO format (YYYY-MM-DD)")
        return value

    @field_validator('raw_data', mode='before')
    @classmethod
    def validate_raw_data(cls, value) -> Optional[pd.DataFrame]:
        """Validate and convert raw data to DataFrame"""
        if value is None:
            return None

        # Handle serialized DataFrame
        if isinstance(value, dict) and value.get('_is_dataframe'):
            try:
                value = pd.DataFrame(**{
                    'data': value['data'],
                    'index': value['index'],
                    'columns': value['columns']
                })
                if 'index' in value:
                    value = value.set_index('index')
                return value
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(f"Could not deserialize DataFrame: {str(e)}") from e

        # Convert dict to DataFrame
        if isinstance(value, dict):
            try:
                if all(isinstance(v, dict) for v in value.values()):
                    value = pd.DataFrame.from_dict(value, orient='index')
                elif all(isinstance(v, (list, pd.Series)) for v in value.values()):
                    value = pd.DataFrame(value)
                else:
                    value = pd.DataFrame([value], index=[0])
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(f"Could not convert dict to DataFrame: {str(e)}") from e

        if not isinstance(value, pd.DataFrame):
            raise ValueError(f"raw_data must be a pandas DataFrame, got {type(value)}")

        # Fill any missing OHLC columns with defaults
        required_columns = {'date', 'open', 'high', 'low', 'close'}
        missing = required_columns - set(value.columns)
        if missing:
            logger.warning("raw_data missing columns %s — filling with defaults", missing)
            for col in missing:
                if col == 'date':
                    value['date'] = value.index if isinstance(value.index, pd.DatetimeIndex) else pd.to_datetime(value.index)
                else:
                    value[col] = 0.0
            if not required_columns.issubset(value.columns):
                raise ValueError(f"Could not create missing columns: {missing}")

        # Coerce date column
        if 'date' in value.columns and not pd.api.types.is_datetime64_any_dtype(value['date']):
            try:
                value['date'] = pd.to_datetime(value['date'])
            except (ValueError, TypeError) as e:
                raise ValueError(f"Could not convert 'date' column to datetime: {str(e)}") from e

        return value

    def to_dict(self) -> dict:
        """Convert contract to dict with DataFrame serialization"""
        data = self.model_dump()
        # Handle special types
        if isinstance(self.start_date, datetime):
            data['start_date'] = self.start_date.isoformat()
        if isinstance(self.end_date, datetime):
            data['end_date'] = self.end_date.isoformat()
        
        # Serialize DataFrame with numpy types converted to native Python types
        if isinstance(self.raw_data, pd.DataFrame):
            data['raw_data'] = self.raw_data.reset_index().to_dict(orient='split')
            data['raw_data']['_is_dataframe'] = True
            
        # Convert numpy types to native Python types
        return json.loads(json.dumps(data, default=self._json_serializer))

    @staticmethod
    def _json_serializer(obj):
        """Handle non-serializable types"""
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, pd.DataFrame):
            return obj.reset_index().to_dict(orient='split')
        if isinstance(obj, (np.generic, np.int64, np.int32, np.float64, np.float32)):
            return float(obj)
        if isinstance(obj, (int, float)):
            return obj
        raise TypeError(f"Type {type(obj)} not serializable")

    def __setstate__(self, state):
        """Custom deserialization for stored state"""
        # Convert ISO strings back to datetimes
        state['start_date'] = datetime.fromisoformat(state['start_date'])
        state['end_date'] = datetime.fromisoformat(state['end_date'])
        
        # Convert dict back to DataFrame if needed
        raw_data = state.get('raw_data')
        if isinstance(raw_data, dict) and raw_data.get('_is_dataframe'):
            state['raw_data'] = pd.DataFrame(**{
                'data': raw_data['data'],
                'index': raw_data['index'],
                'columns': raw_data['columns']
            })
            if 'index' in state['raw_data']:
                state['raw_data'] = state['raw_data'].set_index('index')
        super().__setstate__(state)

    @classmethod
    def from_dict(cls, data: dict) -> 'FetchingContract':
        """Create contract from dict with DataFrame deserialization"""
        if 'raw_data' in data and isinstance(data['raw_data'], list):
            data['raw_data'] = pd.DataFrame(data['raw_data'])
        return cls(**data)

class ProcessingContract(BaseModel):
    """Standardized contract for data processing stage with enhanced debugging"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_default=True,
        extra='forbid',
        str_strip_whitespace=True,
        str_min_length=1
    )
    
    raw_data: pd.DataFrame
    processed_data: Optional[pd.DataFrame] = None
    validation_rules: Dict[str, Any] = {}
    cleaning_steps: Dict[str, Any] = {}
    transformation_steps: Dict[str, Any] = {}
    
    def __init__(self, **data):
        logger.debug("ProcessingContract init keys: %s", list(data.keys()))
        try:
            super().__init__(**data)
            logger.debug("ProcessingContract created")
        except (ValidationError, ValueError) as e:
            logger.error("ProcessingContract creation failed: %s", e)
            raise

    def _debug_print(self):
        """Log contract details at DEBUG level."""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug("ProcessingContract: validation_rules=%s cleaning=%s transform=%s", self.validation_rules, self.cleaning_steps, self.transformation_steps)
        if self.raw_data is not None:
            logger.debug("raw_data shape=%s columns=%s", self.raw_data.shape, self.raw_data.columns.tolist())
        else:
            logger.debug("raw_data=None")

    @field_validator('raw_data', mode='before')
    @classmethod
    def validate_raw_data(cls, value) -> Optional[pd.DataFrame]:
        """Validate and convert raw data to DataFrame"""
        if value is None:
            return None

        if isinstance(value, dict) and value.get('_is_dataframe'):
            try:
                value = pd.DataFrame(**{
                    'data': value['data'],
                    'index': value['index'],
                    'columns': value['columns']
                })
                if 'index' in value:
                    value = value.set_index('index')
                return value
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(f"Could not deserialize DataFrame: {str(e)}") from e

        if isinstance(value, dict):
            try:
                if all(isinstance(v, dict) for v in value.values()):
                    value = pd.DataFrame.from_dict(value, orient='index')
                elif all(isinstance(v, (list, pd.Series)) for v in value.values()):
                    value = pd.DataFrame(value)
                else:
                    value = pd.DataFrame([value], index=[0])
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(f"Could not convert dict to DataFrame: {str(e)}") from e

        if not isinstance(value, pd.DataFrame):
            raise ValueError(f"raw_data must be a pandas DataFrame, got {type(value)}")

        required_columns = {'date', 'open', 'high', 'low', 'close'}
        missing = required_columns - set(value.columns)
        if missing:
            logger.warning("raw_data missing columns %s — filling with defaults", missing)
            for col in missing:
                if col == 'date':
                    value['date'] = value.index if isinstance(value.index, pd.DatetimeIndex) else pd.to_datetime(value.index)
                else:
                    value[col] = 0.0
            if not required_columns.issubset(value.columns):
                raise ValueError(f"Could not create missing columns: {missing}")

        if 'date' in value.columns and not pd.api.types.is_datetime64_any_dtype(value['date']):
            try:
                value['date'] = pd.to_datetime(value['date'])
            except (ValueError, TypeError) as e:
                raise ValueError(f"Could not convert 'date' column to datetime: {str(e)}") from e

        return value

    def to_dict(self) -> dict:
        """Convert contract to dict with DataFrame serialization"""
        data = self.model_dump()
        # Handle special types
        if isinstance(self.raw_data, pd.DataFrame):
            data['raw_data'] = self.raw_data.reset_index().to_dict(orient='split')
            data['raw_data']['_is_dataframe'] = True
            
        # Convert numpy types to native Python types
        return json.loads(json.dumps(data, default=self._json_serializer))

    @staticmethod
    def _json_serializer(obj):
        """Handle non-serializable types"""
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
        if isinstance(obj, pd.DataFrame):
            return obj.reset_index().to_dict(orient='split')
        if isinstance(obj, np.generic):
            return obj.item()
        if isinstance(obj, (np.integer, np.int_, np.intc, np.intp, np.int8, np.int16, 
                           np.int32, np.int64, np.uint8, np.uint16, np.uint32, np.uint64)):
            return int(obj)
        if isinstance(obj, (np.float_, np.float16, np.float32, np.float64)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, (np.ndarray, np.void)):
            return obj.tolist()
        if isinstance(obj, (np.recarray, np.record)):
            return obj.tolist()
        raise TypeError(f"Type {type(obj)} not serializable. Value: {obj}")

    def __setstate__(self, state):
        """Custom deserialization for stored state"""
        # Convert dict back to DataFrame if needed
        raw_data = state.get('raw_data')
        if isinstance(raw_data, dict) and raw_data.get('_is_dataframe'):
            state['raw_data'] = pd.DataFrame(**{
                'data': raw_data['data'],
                'index': raw_data['index'],
                'columns': raw_data['columns']
            })
            if 'index' in state['raw_data']:
                state['raw_data'] = state['raw_data'].set_index('index')
        super().__setstate__(state)

    @classmethod
    def from_dict(cls, data: dict) -> 'ProcessingContract':
        """Create contract from dict with DataFrame deserialization"""
        if 'raw_data' in data and isinstance(data['raw_data'], list):
            data['raw_data'] = pd.DataFrame(data['raw_data'])
        return cls(**data)

class AnalysisContract(BaseModel):
    """Standardized contract for analysis stage with enhanced debugging"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_default=True,
        extra='forbid',
        str_strip_whitespace=True,
        str_min_length=1
    )
    
    processed_data: pd.DataFrame
    analysis_results: Dict[str, Any] = {}
    metrics: Dict[str, float] = {}
    optimal_values: Dict[str, float] = {}
    risk_metrics: Dict[str, float] = {}
    
    def __init__(self, **data):
        logger.debug("AnalysisContract init keys: %s", list(data.keys()))
        try:
            super().__init__(**data)
            logger.debug("AnalysisContract created")
        except (ValidationError, ValueError) as e:
            logger.error("AnalysisContract creation failed: %s", e)
            raise

    def _debug_print(self):
        """Log contract details at DEBUG level."""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug("AnalysisContract: metrics=%s optimal=%s risk=%s", self.metrics, self.optimal_values, self.risk_metrics)
        if self.processed_data is not None:
            logger.debug("processed_data shape=%s columns=%s", self.processed_data.shape, self.processed_data.columns.tolist())
        else:
            logger.debug("processed_data=None")

    @field_validator('processed_data', mode='before')
    @classmethod
    def validate_processed_data(cls, value) -> Optional[pd.DataFrame]:
        """Validate and convert processed data to DataFrame"""
        if value is None:
            return None

        if isinstance(value, dict) and value.get('_is_dataframe'):
            try:
                value = pd.DataFrame(**{
                    'data': value['data'],
                    'index': value['index'],
                    'columns': value['columns']
                })
                if 'index' in value:
                    value = value.set_index('index')
                return value
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(f"Could not deserialize DataFrame: {str(e)}") from e

        if isinstance(value, dict):
            try:
                if all(isinstance(v, dict) for v in value.values()):
                    value = pd.DataFrame.from_dict(value, orient='index')
                elif all(isinstance(v, (list, pd.Series)) for v in value.values()):
                    value = pd.DataFrame(value)
                else:
                    value = pd.DataFrame([value], index=[0])
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(f"Could not convert dict to DataFrame: {str(e)}") from e

        if not isinstance(value, pd.DataFrame):
            raise ValueError(f"processed_data must be a pandas DataFrame, got {type(value)}")

        required_columns = {'date', 'open', 'high', 'low', 'close'}
        missing = required_columns - set(value.columns)
        if missing:
            logger.warning("processed_data missing columns %s — filling with defaults", missing)
            for col in missing:
                if col == 'date':
                    value['date'] = value.index if isinstance(value.index, pd.DatetimeIndex) else pd.to_datetime(value.index)
                else:
                    value[col] = 0.0
            if not required_columns.issubset(value.columns):
                raise ValueError(f"Could not create missing columns: {missing}")

        if 'date' in value.columns and not pd.api.types.is_datetime64_any_dtype(value['date']):
            try:
                value['date'] = pd.to_datetime(value['date'])
            except (ValueError, TypeError) as e:
                raise ValueError(f"Could not convert 'date' column to datetime: {str(e)}") from e

        return value

    def to_dict(self) -> dict:
        """Convert contract to dict with DataFrame serialization"""
        data = self.model_dump()
        # Handle special types
        if isinstance(self.processed_data, pd.DataFrame):
            # Convert DataFrame to dict with proper serialization
            processed_data_dict = self.processed_data.reset_index().to_dict(orient='split')
            # Convert numpy types to native Python types
            processed_data_dict['data'] = [
                [self._json_serializer(item) for item in row] 
                for row in processed_data_dict['data']
            ]
            data['processed_data'] = processed_data_dict
            data['processed_data']['_is_dataframe'] = True
            
        # Handle numpy types and other non-serializable objects
        return json.loads(json.dumps(data, default=self._json_serializer))

    @staticmethod
    def _json_serializer(obj):
        """Handle non-serializable types including numpy/pandas types"""
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
            
        if isinstance(obj, pd.DataFrame):
            return obj.reset_index().to_dict(orient='split')
            
        # Handle numpy types
        if isinstance(obj, np.generic):
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.bool_):
                return bool(obj)
            return obj.item()
            
        # Handle native Python types that might wrap numpy types
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
            
        # Handle collections that might contain numpy types
        if isinstance(obj, (list, tuple)):
            return [AnalysisContract._json_serializer(item) for item in obj]
            
        if isinstance(obj, dict):
            return {k: AnalysisContract._json_serializer(v) for k, v in obj.items()}
            
        # Handle numpy arrays
        if isinstance(obj, np.ndarray):
            return obj.tolist()
            
        raise TypeError(f"Type {type(obj)} not serializable. Value: {repr(obj)}")

    def __setstate__(self, state):
        """Custom deserialization for stored state"""
        # Convert dict back to DataFrame if needed
        processed_data = state.get('processed_data')
        if isinstance(processed_data, dict) and processed_data.get('_is_dataframe'):
            state['processed_data'] = pd.DataFrame(**{
                'data': processed_data['data'],
                'index': processed_data['index'],
                'columns': processed_data['columns']
            })
            if 'index' in state['processed_data']:
                state['processed_data'] = state['processed_data'].set_index('index')
        super().__setstate__(state)

    @classmethod
    def from_dict(cls, data: dict) -> 'AnalysisContract':
        """Create contract from dict with DataFrame deserialization"""
        if 'processed_data' in data and isinstance(data['processed_data'], list):
            data['processed_data'] = pd.DataFrame(data['processed_data'])
        return cls(**data)

class VisualizationContract(BaseModel):
    """Standardized contract for visualization stage with enhanced debugging"""
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_default=True,
        extra='forbid',
        str_strip_whitespace=True,
        str_min_length=1
    )
    
    analysis_results: Dict[str, Any]
    charts: Dict[str, Any] = {}
    tables: Dict[str, Any] = {}
    summaries: Dict[str, str] = {}
    layout_config: Dict[str, Any] = {}
    
    def __init__(self, **data):
        logger.debug("VisualizationContract init keys: %s", list(data.keys()))
        try:
            super().__init__(**data)
            logger.debug("VisualizationContract created")
        except (ValidationError, ValueError) as e:
            logger.error("VisualizationContract creation failed: %s", e)
            raise

    def _debug_print(self):
        """Log contract details at DEBUG level."""
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug("VisualizationContract: charts=%s tables=%s summaries=%s layout=%s", list(self.charts.keys()), list(self.tables.keys()), list(self.summaries.keys()), self.layout_config)

    @field_validator('analysis_results')
    @classmethod
    def validate_analysis_results(cls, value):
        """Validate analysis results"""
        if not value:
            raise ValueError("Analysis results cannot be empty")
        return value

    def to_dict(self) -> dict:
        """Convert contract to dict with proper serialization"""
        data = self.model_dump()
        # Convert numpy types to native Python types
        return json.loads(json.dumps(data, default=self._json_serializer))

    @staticmethod
    def _json_serializer(obj):
        """Handle non-serializable types including Plotly figures"""
        # Handle basic types
        if isinstance(obj, (datetime, pd.Timestamp)):
            return obj.isoformat()
            
        # Handle pandas DataFrames
        if isinstance(obj, pd.DataFrame):
            return obj.reset_index().to_dict(orient='split')
            
        # Handle numpy types
        if isinstance(obj, np.generic):
            if isinstance(obj, np.integer):
                return int(obj)
            if isinstance(obj, np.floating):
                return float(obj)
            if isinstance(obj, np.bool_):
                return bool(obj)
            return obj.item()
            
        # Handle Plotly figures
        if hasattr(obj, 'to_dict'):  # Check if object has to_dict method
            try:
                fig_dict = obj.to_dict()
                # Add marker to identify this as a Plotly figure
                fig_dict['_is_plotly_figure'] = True
                return fig_dict
            except Exception as e:
                raise TypeError(f"Could not serialize Plotly figure: {str(e)}")
                
        # Handle native Python types
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
            
        # Handle collections that might contain numpy types
        if isinstance(obj, (list, tuple)):
            return [VisualizationContract._json_serializer(item) for item in obj]
            
        if isinstance(obj, dict):
            return {k: VisualizationContract._json_serializer(v) for k, v in obj.items()}
            
        # Handle numpy arrays
        if isinstance(obj, np.ndarray):
            return obj.tolist()
            
        raise TypeError(f"Type {type(obj)} not serializable. Value: {repr(obj)}")

    def __setstate__(self, state):
        """Custom deserialization for stored state"""
        super().__setstate__(state)

    @classmethod
    def from_dict(cls, data: dict) -> 'VisualizationContract':
        """Create contract from dict with Plotly figure deserialization"""
        # Handle Plotly figures in charts
        if 'charts' in data:
            for chart_name, chart_data in data['charts'].items():
                if isinstance(chart_data, dict) and chart_data.get('_is_plotly_figure'):
                    try:
                        import plotly.graph_objects as go
                        data['charts'][chart_name] = go.Figure(chart_data)
                    except (ValueError, TypeError) as e:
                        logger.error("Error deserializing Plotly figure %s: %s", chart_name, e)
                        data['charts'][chart_name] = None
                        
        return cls(**data)

class UIRenderingContract(BaseModel):
    """Standardized contract for UI rendering stage"""
    visual_components: Dict[str, Any]
    layout: Dict[str, Any]
    interaction_config: Dict[str, Any] = {}
    
    @field_validator('visual_components')
    def validate_visual_components(cls, value):
        if not value:
            raise ValueError("Visual components cannot be empty")
        return value

# Conversion utilities
def convert_fetching_to_processing(fetching_contract: FetchingContract) -> ProcessingContract:
    """Convert fetching contract to processing contract"""
    return ProcessingContract(
        raw_data=fetching_contract.raw_data,
        validation_rules={},
        cleaning_steps={},
        transformation_steps={}
    )

def convert_processing_to_analysis(processing_contract: ProcessingContract) -> AnalysisContract:
    """Convert processing contract to analysis contract"""
    return AnalysisContract(
        processed_data=processing_contract.processed_data,
        analysis_results={},
        metrics={},
        optimal_values={},
        risk_metrics={}
    )

def convert_analysis_to_visualization(analysis_contract: AnalysisContract) -> VisualizationContract:
    """Convert analysis contract to visualization contract"""
    return VisualizationContract(
        analysis_results=analysis_contract.analysis_results,
        charts={},
        tables={},
        summaries={},
        layout_config={}
    )

def convert_visualization_to_ui(visualization_contract: VisualizationContract) -> UIRenderingContract:
    """Convert visualization contract to UI rendering contract"""
    return UIRenderingContract(
        visual_components={
            'charts': visualization_contract.charts,
            'tables': visualization_contract.tables,
            'summaries': visualization_contract.summaries
        },
        layout=visualization_contract.layout_config
    )
