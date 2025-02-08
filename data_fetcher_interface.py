from abc import ABC, abstractmethod

class IDataFetcher(ABC):
    @abstractmethod
    def fetch_data(self, params):
        """Retrieves data based on provided parameters."""
        pass

    @abstractmethod
    def clear_cache(self):
        """Clears the cached data if applicable."""
        pass
