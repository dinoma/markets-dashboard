from typing import Optional, Any
import logging
from dash import html, dcc
from .error_templates import ErrorTemplate
from .error_logging import log_error
import datetime

class ErrorBoundary(html.Div):
    """Error boundary component to catch and display errors instead of crashing"""
    
    def __init__(self, children, fallback=None, **kwargs):
        super().__init__(**kwargs)
        self.children = children
        self.fallback = fallback or ErrorTemplate()
        self.has_error = False
        self.error_info = None
        
    def component_did_catch(self, error, info):
        """Catch errors in child components"""
        self.has_error = True
        self.error_info = {
            'error': error,
            'info': info,
            'timestamp': datetime.now().isoformat(),
            'component': self.__class__.__name__
        }
        log_error(self.error_info)
        self.children = self.fallback.render(self.error_info)
        
    def reset(self):
        """Reset the error boundary to its initial state"""
        self.has_error = False
        self.error_info = None
        self.children = self.original_children
