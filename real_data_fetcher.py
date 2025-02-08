from data_fetcher_interface import IDataFetcher
import logging
import time

class RealDataFetcher(IDataFetcher):
    def __init__(self, cache_duration=300):
        """
        Initializes the RealDataFetcher with an empty cache and sets the cache duration.
        
        Args:
            cache_duration (int): Duration in seconds before cache expires. Default is 300 seconds (5 minutes).
        """
        self.cache = {}
        self.cache_timestamps = {}
        self.cache_duration = cache_duration  # Cache duration in seconds
        logging.info(f"RealDataFetcher initialized with empty cache and cache duration of {self.cache_duration} seconds.")

    def fetch_data(self, params):
        """
        Fetches data from the actual data source based on provided parameters.
        Utilizes caching to store and retrieve data, with cache invalidation based on time.
        
        Args:
            params (dict): Parameters required to fetch the data.
        
        Returns:
            data: The fetched data.
        """
        logging.debug(f"Fetching data with params: {params}")
        cache_key = self._generate_cache_key(params)
        current_time = time.time()
        
        # Check if data is in cache and not expired
        if cache_key in self.cache:
            cached_time = self.cache_timestamps.get(cache_key, 0)
            if (current_time - cached_time) < self.cache_duration:
                logging.debug("Data found in cache and is still valid.")
                return self.cache[cache_key]
            else:
                # Cache expired
                logging.debug("Cached data has expired. Removing from cache.")
                self.cache.pop(cache_key, None)
                self.cache_timestamps.pop(cache_key, None)
        
        # Fetch data from the source since it's not in cache or cache expired
        data = self._fetch_from_source(params)
        self.cache[cache_key] = data
        self.cache_timestamps[cache_key] = current_time
        logging.debug("Data fetched from source and cached.")
        return data

    def _fetch_from_source(self, params):
        """
        Internal method to fetch data from the actual data source.
        
        Args:
            params (dict): Parameters required to fetch the data.
        
        Returns:
            data: The fetched data.
        """
        # Implement the actual data fetching logic here.
        # This could be a database query, API call, etc.
        logging.info("Fetching data from the real data source.")
        data = {}  # Replace with real data fetching logic
        # Simulate data fetching delay
        time.sleep(1)
        # Example fetched data
        data = {"example_key": "example_value"}
        return data

    def clear_cache(self):
        """
        Clears the cached data by emptying the cache and cache timestamps.
        """
        self.cache.clear()
        self.cache_timestamps.clear()
        logging.info("Cache has been cleared.")
    
    def _generate_cache_key(self, params):
        """
        Generates a unique cache key based on the provided parameters.
        Assumes that params is a dictionary with hashable values.
        
        Args:
            params (dict): Parameters used to fetch data.
        
        Returns:
            tuple: A hashable tuple representing the cache key.
        """
        # Convert the params dictionary into a sorted tuple of key-value pairs
        # This ensures that the cache key is consistent for the same parameters
        return tuple(sorted(params.items()))
