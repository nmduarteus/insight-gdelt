import os
import sys
import unittest

sys.path.append(os.path.expanduser('~/insight/code/config/'))
from config import config


class TestConfig_output(unittest.TestCase):

    def test_config_output_type(self):
        params = config("gdelt")
        self.assertIsInstance(params,dict)