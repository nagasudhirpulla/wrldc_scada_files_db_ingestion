import unittest
from src.appConf import getJsonConfig


class TestOutagesRepo(unittest.TestCase):
    def test_getJsonConfig(self) -> None:
        """tests the outages fetching function
        """
        jsonConf = getJsonConfig()
        self.assertTrue("idSrvDiscoUrl" in jsonConf)
        self.assertTrue("accessTokenFetchAudience" in jsonConf)
