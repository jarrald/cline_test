import os
import unittest
from unittest.mock import patch

from libs.mesh import config


class ConfigTest(unittest.TestCase):
    def test_missing_required_env_raises(self) -> None:
        with patch.dict(os.environ, {}, clear=True):
            with self.assertRaises(config.ConfigError):
                config.env("REQUIRED_THING")

    def test_parsing_helpers(self) -> None:
        with patch.dict(
            os.environ,
            {"N": "42", "F": "1.5", "B": "true", "L": "a, b ,,c", "J": '{"x": 1}'},
            clear=True,
        ):
            self.assertEqual(config.env_int("N"), 42)
            self.assertEqual(config.env_float("F"), 1.5)
            self.assertTrue(config.env_bool("B"))
            self.assertEqual(config.env_list("L"), ["a", "b", "c"])
            self.assertEqual(config.env_json("J"), {"x": 1})
            self.assertEqual(config.env_int("MISSING", 7), 7)

    def test_invalid_number_is_reported(self) -> None:
        with patch.dict(os.environ, {"BAD": "abc"}, clear=True):
            with self.assertRaises(config.ConfigError):
                config.env_int("BAD")

    def test_pod_identity(self) -> None:
        with patch.dict(os.environ, {"POD_NAME": "order-service-2", "POD_ORDINAL": "2", "POD_COUNT": "3"}, clear=True):
            self.assertEqual(config.pod_identity(), ("order-service-2", 2, 3))


if __name__ == "__main__":
    unittest.main()
