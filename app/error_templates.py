from dash import html, dcc

class ErrorTemplate:
    """Base template for error display"""
    
    def render(self, error_info=None):
        """Render the error display"""
        error_details = {
            'message': str(error_info.get('error', 'Unknown error')),
            'component': error_info.get('component', 'Unknown component'),
            'timestamp': error_info.get('timestamp', 'Unknown time'),
            'stack': error_info.get('info', {}).get('componentStack', 'No stack trace')
        }
        
        return html.Div([
            html.H3("Something went wrong"),
            html.P("We're sorry, but an unexpected error occurred."),
            dcc.Loading(
                id="error-retry-loading",
                type="default",
                children=html.Button(
                    "Try Again",
                    id="error-retry-button",
                    n_clicks=0
                )
            ),
            html.Details([
                html.Summary("Technical Details"),
                html.Div([
                    html.P(f"Component: {error_details['component']}"),
                    html.P(f"Time: {error_details['timestamp']}"),
                    html.Hr(),
                    html.P("Error Message:"),
                    html.Pre(error_details['message']),
                    html.Hr(),
                    html.P("Component Stack:"),
                    html.Pre(error_details['stack'])
                ])
            ])
        ])
