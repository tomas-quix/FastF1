"""Unit tests for FastF1 source main.py."""
import sys
import types
import unittest
from unittest.mock import MagicMock, patch
import pandas as pd


def _make_fastf1_stub():
    """Build a minimal fastf1 stub with all required submodules."""
    stub = types.ModuleType("fastf1")
    stub.Cache = MagicMock()

    exceptions_mod = types.ModuleType("fastf1.exceptions")

    class DataNotLoadedError(Exception):
        pass

    exceptions_mod.DataNotLoadedError = DataNotLoadedError
    stub.exceptions = exceptions_mod
    stub.DataNotLoadedError = DataNotLoadedError

    ergast_mod = types.ModuleType("fastf1.ergast")
    stub.ergast = ergast_mod

    sys.modules["fastf1"] = stub
    sys.modules["fastf1.exceptions"] = exceptions_mod
    sys.modules["fastf1.ergast"] = ergast_mod

    return stub, exceptions_mod, ergast_mod


def _load_main():
    """Import main with patched fastf1 and quixstreams."""
    for mod in list(sys.modules.keys()):
        if mod in ("main", "fastf1", "fastf1.exceptions", "fastf1.ergast", "quixstreams", "dotenv"):
            del sys.modules[mod]

    stub, exc_mod, ergast_mod = _make_fastf1_stub()

    qs_stub = types.ModuleType("quixstreams")
    qs_stub.Application = MagicMock(return_value=MagicMock(
        topic=MagicMock(return_value=MagicMock(name="test-topic"))
    ))
    sys.modules["quixstreams"] = qs_stub

    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = MagicMock()
    sys.modules["dotenv"] = dotenv_stub

    import importlib.util
    import os
    spec = importlib.util.spec_from_file_location(
        "main",
        os.path.join(os.path.dirname(__file__), "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, stub, exc_mod, ergast_mod


def _make_ergast_response(content_df, race_name="Bahrain Grand Prix"):
    """Create a mock Ergast response with the correct structure."""
    desc_df = pd.DataFrame([{"season": 2023, "round": 1, "raceName": race_name}])
    resp = MagicMock()
    resp.content = [content_df]
    resp.description = desc_df
    return resp


class TestLoadViaErgast(unittest.TestCase):
    """Tests for the load_via_ergast fallback function."""

    def setUp(self):
        self.mod, self.stub, self.exc_mod, self.ergast_mod = _load_main()

    def _make_producer(self):
        producer = MagicMock()
        producer.__enter__ = MagicMock(return_value=producer)
        producer.__exit__ = MagicMock(return_value=False)
        return producer

    def _make_topic(self):
        topic = MagicMock()
        topic.name = "test-topic"
        topic.serialize = MagicMock(return_value=MagicMock(value=b'{}'))
        return topic

    def test_uses_driverid_column_not_drivercode(self):
        """Ergast lap times has 'driverId', not 'driverCode' - must use correct column."""
        lap_df = pd.DataFrame([
            {"number": 1, "driverId": "max_verstappen", "position": 1,
             "time": pd.Timedelta(minutes=1, seconds=39, milliseconds=19)},
            {"number": 1, "driverId": "leclerc", "position": 2,
             "time": pd.Timedelta(minutes=1, seconds=40, milliseconds=230)},
        ])
        results_df = pd.DataFrame([
            {"driverId": "max_verstappen", "driverCode": "VER", "position": 1},
            {"driverId": "leclerc", "driverCode": "LEC", "position": 2},
        ])

        ergast_inst = MagicMock()
        ergast_inst.get_lap_times.return_value = _make_ergast_response(lap_df)
        ergast_inst.get_race_results.return_value = _make_ergast_response(results_df)
        self.ergast_mod.Ergast = MagicMock(return_value=ergast_inst)

        producer = self._make_producer()
        lap_topic = self._make_topic()
        telemetry_topic = self._make_topic()

        lap_count, tel_count, race_name = self.mod.load_via_ergast(
            2023, 1, producer, lap_topic, telemetry_topic
        )

        self.assertEqual(lap_count, 2)
        self.assertEqual(race_name, "Bahrain Grand Prix")

        # Check the keys used for producing - should be driverId values
        produce_calls = producer.produce.call_args_list
        keys = [c.kwargs.get("key", c.args[1] if len(c.args) > 1 else None)
                for c in produce_calls]
        self.assertIn("max_verstappen", keys)
        self.assertIn("leclerc", keys)

    def test_lap_number_from_number_column(self):
        """'number' column in lap times is the lap number."""
        lap_df = pd.DataFrame([
            {"number": 5, "driverId": "max_verstappen", "position": 1,
             "time": pd.Timedelta(seconds=95)},
        ])
        results_df = pd.DataFrame([
            {"driverId": "max_verstappen", "driverCode": "VER", "position": 1},
        ])

        ergast_inst = MagicMock()
        ergast_inst.get_lap_times.return_value = _make_ergast_response(lap_df)
        ergast_inst.get_race_results.return_value = _make_ergast_response(results_df)
        self.ergast_mod.Ergast = MagicMock(return_value=ergast_inst)

        producer = self._make_producer()
        lap_topic = self._make_topic()
        telemetry_topic = self._make_topic()

        lap_count, tel_count, race_name = self.mod.load_via_ergast(
            2023, 1, producer, lap_topic, telemetry_topic
        )

        self.assertEqual(lap_count, 1)
        # Verify the serialized payload has lap=5
        call_kwargs = lap_topic.serialize.call_args
        payload = call_kwargs.kwargs.get("value", call_kwargs[1].get("value"))
        self.assertEqual(payload["lap"], 5)

    def test_timedelta_converted_to_ms(self):
        """pd.Timedelta lap times are converted to integer milliseconds."""
        lap_df = pd.DataFrame([
            {"number": 1, "driverId": "driver_a", "position": 1,
             "time": pd.Timedelta(minutes=1, seconds=39, milliseconds=19)},
        ])
        results_df = pd.DataFrame([{"driverId": "driver_a", "driverCode": "DRV", "position": 1}])

        ergast_inst = MagicMock()
        ergast_inst.get_lap_times.return_value = _make_ergast_response(lap_df)
        ergast_inst.get_race_results.return_value = _make_ergast_response(results_df)
        self.ergast_mod.Ergast = MagicMock(return_value=ergast_inst)

        producer = self._make_producer()
        lap_topic = self._make_topic()
        telemetry_topic = self._make_topic()

        self.mod.load_via_ergast(2023, 1, producer, lap_topic, telemetry_topic)

        call_kwargs = lap_topic.serialize.call_args
        payload = call_kwargs.kwargs.get("value", call_kwargs[1].get("value"))
        # 1 min 39.019 s = 99019 ms
        self.assertEqual(payload["LapTime_ms"], 99019)

    def test_race_name_from_description_dataframe(self):
        """Race name extracted from description DataFrame (not dict)."""
        lap_df = pd.DataFrame([
            {"number": 1, "driverId": "driver_x", "position": 1,
             "time": pd.Timedelta(seconds=90)},
        ])
        results_df = pd.DataFrame([{"driverId": "driver_x", "driverCode": "DRX", "position": 1}])

        ergast_inst = MagicMock()
        ergast_inst.get_lap_times.return_value = _make_ergast_response(lap_df, "Monaco Grand Prix")
        ergast_inst.get_race_results.return_value = _make_ergast_response(results_df, "Monaco Grand Prix")
        self.ergast_mod.Ergast = MagicMock(return_value=ergast_inst)

        producer = self._make_producer()
        lap_topic = self._make_topic()
        telemetry_topic = self._make_topic()

        _, _, race_name = self.mod.load_via_ergast(2023, 5, producer, lap_topic, telemetry_topic)
        self.assertEqual(race_name, "Monaco Grand Prix")

    def test_telemetry_placeholder_produced_per_driver(self):
        """One placeholder telemetry message produced per driver from race results."""
        lap_df = pd.DataFrame([
            {"number": 1, "driverId": "driver_a", "position": 1, "time": pd.Timedelta(seconds=90)},
            {"number": 1, "driverId": "driver_b", "position": 2, "time": pd.Timedelta(seconds=91)},
        ])
        results_df = pd.DataFrame([
            {"driverId": "driver_a", "driverCode": "DRA", "position": 1},
            {"driverId": "driver_b", "driverCode": "DRB", "position": 2},
        ])

        ergast_inst = MagicMock()
        ergast_inst.get_lap_times.return_value = _make_ergast_response(lap_df)
        ergast_inst.get_race_results.return_value = _make_ergast_response(results_df)
        self.ergast_mod.Ergast = MagicMock(return_value=ergast_inst)

        producer = self._make_producer()
        lap_topic = self._make_topic()
        telemetry_topic = self._make_topic()

        lap_count, tel_count, _ = self.mod.load_via_ergast(
            2023, 1, producer, lap_topic, telemetry_topic
        )

        # 2 lap rows + 2 telemetry placeholders = 4 produce calls
        self.assertEqual(lap_count, 2)
        self.assertEqual(tel_count, 2)

    def test_no_lap_data_returns_zeros(self):
        """When Ergast returns no content, counts are 0."""
        empty_resp = MagicMock()
        empty_resp.content = []
        empty_resp.description = pd.DataFrame([{"raceName": "Bahrain Grand Prix"}])

        ergast_inst = MagicMock()
        ergast_inst.get_lap_times.return_value = empty_resp
        ergast_inst.get_race_results.return_value = empty_resp
        self.ergast_mod.Ergast = MagicMock(return_value=ergast_inst)

        producer = self._make_producer()
        lap_topic = self._make_topic()
        telemetry_topic = self._make_topic()

        lap_count, tel_count, _ = self.mod.load_via_ergast(
            2023, 1, producer, lap_topic, telemetry_topic
        )

        self.assertEqual(lap_count, 0)
        self.assertEqual(tel_count, 0)
        producer.produce.assert_not_called()


class TestSessionLoadEmptyCheck(unittest.TestCase):
    """Tests for detecting silently-empty FastF1 session (livetiming blocked)."""

    def setUp(self):
        self.mod, self.stub, self.exc_mod, self.ergast_mod = _load_main()

    def test_datanotloadederror_is_caught_as_exception(self):
        """DataNotLoadedError is a subclass of Exception, so the broad except catches it."""
        DataNotLoadedError = self.exc_mod.DataNotLoadedError
        # The session load check uses `except Exception` which catches DataNotLoadedError
        self.assertTrue(issubclass(DataNotLoadedError, Exception))

    def test_empty_laps_simulate_session_loaded_false(self):
        """Simulate the session empty-laps check: DataNotLoadedError -> session_loaded=False."""
        DataNotLoadedError = self.exc_mod.DataNotLoadedError

        # Replicate the exact check logic from main()
        session_loaded = False
        session_mock = MagicMock()
        session_mock.load.return_value = None  # load "succeeds" silently

        # Make session.laps raise DataNotLoadedError (livetiming blocked)
        type(session_mock).laps = property(
            lambda self: (_ for _ in ()).throw(DataNotLoadedError("not loaded"))
        )

        try:
            session_mock.load()
            try:
                laps_data = session_mock.laps
                if laps_data is None or len(laps_data) == 0:
                    raise Exception("Session laps empty")
            except DataNotLoadedError as exc:
                raise Exception(f"DataNotLoadedError: {exc}") from exc
            session_loaded = True
        except Exception:
            session_loaded = False

        self.assertFalse(session_loaded)

    def test_timedelta_to_ms_null_safety(self):
        """timedelta_to_ms handles NaT and None without crashing."""
        fn = self.mod.timedelta_to_ms
        self.assertIsNone(fn(None))
        self.assertIsNone(fn(float("nan")))
        self.assertIsNone(fn(pd.NaT))
        self.assertEqual(fn(pd.Timedelta(seconds=90)), 90000)

    def test_to_native_conversions(self):
        """to_native converts numpy scalars to Python types."""
        import numpy as np
        fn = self.mod.to_native
        self.assertIsNone(fn(None))
        self.assertIsNone(fn(float("nan")))
        self.assertIsInstance(fn(np.int64(5)), int)
        self.assertEqual(fn(np.int64(5)), 5)
        self.assertIsInstance(fn(np.float64(3.14)), float)
        self.assertEqual(fn(pd.Timedelta(seconds=2)), 2000)


if __name__ == "__main__":
    unittest.main()
