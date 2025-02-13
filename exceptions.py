from datetime import datetime

class DataFetcherError(Exception):
    """Base exception class for DataFetcher related errors."""
    def __init__(self, message="DataFetcher error occurred", component=None, context=None):
        super().__init__(message)
        self.timestamp = datetime.now().isoformat()
        self.component = component
        self.context = context or {}
        
    def get_metadata(self):
        """Return error metadata as a dictionary"""
        return {
            'timestamp': self.timestamp,
            'component': self.component,
            'context': self.context,
            'message': str(self)
        }

class DataFetchFailedError(DataFetcherError):
    """Exception raised when data fetching fails."""
    def __init__(self, message="Data fetching failed.", original_exception=None, component=None, context=None):
        super().__init__(message, component, context)
        self.original_exception = original_exception
        
    def get_metadata(self):
        """Return error metadata including original exception details"""
        metadata = super().get_metadata()
        if self.original_exception:
            metadata['original_exception'] = str(self.original_exception)
        return metadata

class CacheError(DataFetcherError):
    """Exception raised for cache-related errors."""
    pass

class DataProcessingError(Exception):
    """Base exception class for data processing errors."""
    def __init__(self, message="Data processing failed", details=None, component=None, context=None):
        super().__init__(message)
        self.timestamp = datetime.now().isoformat()
        self.component = component
        self.context = context or {}
        self.details = details
        
    def get_metadata(self):
        """Return error metadata as a dictionary"""
        return {
            'timestamp': self.timestamp,
            'component': self.component,
            'context': self.context,
            'details': self.details,
            'message': str(self)
        }
        
    def __str__(self):
        if self.details:
            return f"{self.message}: {self.details}"
        return self.message

class DataValidationError(DataProcessingError):
    """Exception raised when data validation fails."""
    def __init__(self, message="Data validation failed", invalid_data=None):
        super().__init__(message)
        self.invalid_data = invalid_data
        
    def __str__(self):
        if self.invalid_data:
            return f"{self.message}: Invalid data - {self.invalid_data}"
        return self.message

class AnalysisError(Exception):
    """Base exception class for analysis related errors."""
    def __init__(self, message="Analysis failed", details=None, component=None, context=None):
        super().__init__(message)
        self.timestamp = datetime.now().isoformat()
        self.component = component
        self.context = context or {}
        self.details = details
        
    def get_metadata(self):
        """Return error metadata as a dictionary"""
        return {
            'timestamp': self.timestamp,
            'component': self.component,
            'context': self.context,
            'details': self.details,
            'message': str(self)
        }
        
    def __str__(self):
        if self.details:
            return f"{self.message}: {self.details}"
        return self.message
