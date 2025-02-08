from data_fetcher_interface import IDataFetcher
import logging

class TestDataFetcher(IDataFetcher):
    def __init__(self):
        # Initialize mock or static data for testing purposes
        self.test_data = {
            'param1': {'key': 'value1'},
            'param2': {'key': 'value2'},
            # Add more mock data as needed
        }
        logging.info("TestDataFetcher initialized with predefined test data.")

    def fetch_data(self, params):
        """
        Retrieves mock data based on provided parameters for testing.
        
        Args:
            params (dict): Parameters required to fetch the data.
        
        Returns:
            data: The mock data corresponding to the parameters.
        """
        logging.debug(f"Fetching test data with params: {params}")
        key = params.get('key')
        data = self.test_data.get(key, {})
        logging.debug(f"Returning test data: {data}")
        return data

    def clear_cache(self):
        """Clears the cached test data if applicable."""
        # If caching is implemented in TestDataFetcher, clear it here
        # For now, we'll assume there's no caching
        logging.info("TestDataFetcher has no cache to clear.")
