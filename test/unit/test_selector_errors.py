from dbt.graph import cli
import dbt.exceptions
import textwrap
import yaml
import unittest
from pprint import pprint
from dbt.config.selectors import (
    selector_config_from_data
)

from dbt.contracts.selection import SelectorFile


def get_selector_dict(txt: str) -> dict:
    txt = textwrap.dedent(txt)
    dct = yaml.safe_load(txt)
    return dct



class SelectorUnitTest(unittest.TestCase):

    def test_parse_multiple_excludes(self):
        dct = get_selector_dict('''\
            selectors:
                - name: mult_excl
                  definition:
                    union:
                      - method: tag
                        value: nightly
                      - exclude:
                         - method: tag
                           value: hourly
                      - exclude:
                         - method: tag
                           value: daily
            ''')
        with self.assertRaises(dbt.exceptions.DbtSelectorsError):
            selectors = selector_config_from_data(dct)

    def test_parse_set_op_plus(self):
        dct = get_selector_dict('''\
            selectors:
                - name: union_plus
                  definition:
                    - union:
                       - method: tag
                         value: nightly
                       - exclude:
                          - method: tag
                            value: hourly
                    - method: tag
                      value: foo
            ''')
        with self.assertRaises(dbt.exceptions.DbtSelectorsError):
            selectors = selector_config_from_data(dct)

    def test_parse_multiple_methods(self):
        dct = get_selector_dict('''\
            selectors:
                - name: mult_methods
                  definition:
                    - tag:hourly
                    - tag:nightly
                    - fqn:start
            ''')
        with self.assertRaises(dbt.exceptions.DbtSelectorsError):
            selectors = selector_config_from_data(dct)

    def test_parse_set_with_method(self):
        dct = get_selector_dict('''\
                selectors:
                  - name: mixed_syntaxes
                    definition:
                      key: value
                      method: tag
                      value: foo
                      union:
                        - method: tag
                          value: m1234
                        - exclude:
                          - method: tag
                            value: m5678
            ''')
        with self.assertRaises(dbt.exceptions.DbtSelectorsError):
            selectors = selector_config_from_data(dct)


