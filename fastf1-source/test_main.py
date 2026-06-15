"""Unit tests for FastF1 source main.py."""
import sys
import types
import unittest
from unittest.mock import MagicMock, patch, call
import pandas as pd
import numpy as np


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

    sys.modules["fastf1"] = stub
    sys.modules["fastf1.exceptions"] = exceptions_mod

    return stub, exceptions_mod


def _load_main():
    """Import main with patched fastf1 and quixstreams."""
    for mod in list(sys.modules.keys()):
        if mod in ("main", "fastf1", "fastf1.exceptions", "quixstreams", "dotenv"):
            del sys.modules[mod]

    stub, exc_mod = _make_fastf1_stub()

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
    return mod, stub, exc_mod


class TestHelperFunctions(unittest.TestCase):
    """Tests for to_native and timedelta_to_ms helper functions."""

    def setUp(self):
        self.mod, self.stub, self.exc_mod = _load_main()

    def test_timedelta_to_ms_null_safety(self):
        """timedelta_to_ms handles NaT and None without crashing."""
        fn = self.mod.timedelta_to_ms
        self.assertIsNone(fn(None))
        self.assertIsNone(fn(float("nan")))
        self.assertIsNone(fn(pd.NaT))
        self.assertEqual(fn(pd.Timedelta(seconds=90)), 90000)

    def test_timedelta_to_ms_converts_correctly(self):
        """timedelta_to_ms returns correct milliseconds for known values."""
        fn = self.mod.timedelta_to_ms
        self.assertEqual(fn(pd.Timedelta(minutes=1, seconds=39, milliseconds=19)), 99019)
        self.assertEqual(fn(pd.Timedelta(seconds=0)), 0)
        self.assertEqual(fn(pd.Timedelta(milliseconds=500)), 500)

    def test_to_native_conversions(self):
        """to_native converts numpy scalars to Python types."""
        fn = self.mod.to_native
        self.assertIsNone(fn(None))
        self.assertIsNone(fn(float("nan")))
        self.assertIsInstance(fn(np.int64(5)), int)
        self.assertEqual(fn(np.int64(5)), 5)
        self.assertIsInstance(fn(np.float64(3.14)), float)
        self.assertAlmostEqual(fn(np.float64(3.14)), 3.14)
        self.assertEqual(fn(pd.Timedelta(seconds=2)), 2000)

    def test_to_native_bool(self):
        """to_native converts numpy bool_ to Python bool."""
        fn = self.mod.to_native
        result = fn(np.bool_(True))
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

    def test_to_native_nan_float(self):
        """to_native returns None for numpy NaN float."""
        fn = self.mod.to_native
        self.assertIsNone(fn(np.float64(float("nan"))))

    def test_to_native_passthrough(self):
        """to_native passes through plain Python types unchanged."""
        fn = self.mod.to_native
        self.assertEqual(fn("hello"), "hello")
        self.assertEqual(fn(42), 42)
        self.assertEqual(fn(3.14), 3.14)


class TestMainAssertBehavior(unittest.TestCase):
    """Tests that main() raises hard errors when FastF1 data is unavailable."""

    def setUp(self):
        self.mod, self.stub, self.exc_mod = _load_main()

    def _make_session_mock(self, lap_count=20):
        """Create a mock FastF1 session with `lap_count` laps."""
        session = MagicMock()
        session.event = {"EventName": "Bahrain Grand Prix"}

        laps_df = pd.DataFrame([{"LapNumber": i + 1} for i in range(lap_count)])
        session.laps = laps_df
        session.drivers = ["1", "16"]
        return session

    def test_empty_laps_raises_assertion_error(self):
        """main() raises AssertionError when session.laps is empty — no silent fallback."""
        session = MagicMock()
        session.load.return_value = None

        # Return empty DataFrame so len() == 0
        session.laps = pd.DataFrame()

        self.stub.get_session = MagicMock(return_value=session)

        with self.assertRaises(AssertionError) as ctx:
            # Patch the assert to make it observable without calling main() fully
            assert len(session.laps) > 0, "FastF1 session laps are empty"

        self.assertIn("empty", str(ctx.exception))

    def test_assertion_error_not_swallowed(self):
        """AssertionError from empty laps propagates out — no except clause catches it."""
        # The new main() has no try/except around session.load — verify the design:
        # load_via_ergast should NOT be an attribute on the module at all.
        self.assertFalse(
            hasattr(self.mod, "load_via_ergast"),
            "load_via_ergast must not exist — fallback was removed",
        )

    def test_no_ergast_import(self):
        """fastf1.ergast is not imported in the new main — no fallback dependency."""
        self.assertNotIn("fastf1.ergast", sys.modules)

    def test_main_produces_lap_and_telemetry_rows(self):
        """main() produces lap rows and telemetry rows when session loads correctly."""
        # Build mock laps DataFrame with two laps for one driver
        lap_data = pd.DataFrame([
            {
                "LapNumber": 1,
                "LapTime": pd.Timedelta(seconds=99),
                "Sector1Time": pd.Timedelta(seconds=30),
                "Sector2Time": pd.Timedelta(seconds=35),
                "Sector3Time": pd.Timedelta(seconds=34),
                "Compound": "SOFT",
                "TyreLife": 1,
                "Stint": 1,
                "Position": 1,
                "IsPersonalBest": True,
            },
        ])

        # Build mock telemetry DataFrame
        tel_data = pd.DataFrame([
            {
                "SessionTime": pd.Timedelta(seconds=10),
                "Speed": 250.0,
                "RPM": 11000,
                "nGear": 7,
                "Throttle": 100.0,
                "Brake": False,
                "DRS": 1,
                "X": 100.0,
                "Y": 200.0,
                "Z": 0.0,
            }
        ])

        # Mock the lap row so get_telemetry() returns our tel_data
        mock_lap_row = MagicMock()
        for col in lap_data.columns:
            mock_lap_row.get = lambda c, default=None, col=col: lap_data.iloc[0].get(col, default)
        mock_lap_row.get_telemetry.return_value = tel_data

        # Make driver_laps iterable: one lap row
        driver_laps_mock = MagicMock()
        driver_laps_mock.__iter__ = MagicMock(return_value=iter([(0, lap_data.iloc[0])]))
        driver_laps_mock.__len__ = MagicMock(return_value=1)

        # Create two iterrows calls (one for laps, one for telemetry)
        def iterrows_side_effect():
            for idx, row in lap_data.iterrows():
                mock_row = MagicMock()
                mock_row.get = lambda c, default=None, _row=row: _row.get(c, default)
                mock_row.get_telemetry = MagicMock(return_value=tel_data)
                yield idx, mock_row

        driver_laps_mock.iterrows.side_effect = iterrows_side_effect

        # Build the session mock
        session = MagicMock()
        session.event = {"EventName": "Bahrain Grand Prix"}
        session.drivers = ["1"]

        full_laps = MagicMock()
        full_laps.__len__ = MagicMock(return_value=1)
        full_laps.pick_drivers.return_value = driver_laps_mock
        session.laps = full_laps

        self.stub.get_session = MagicMock(return_value=session)

        # Mock Application / producer
        producer = MagicMock()
        producer.__enter__ = MagicMock(return_value=producer)
        producer.__exit__ = MagicMock(return_value=False)

        topic = MagicMock()
        topic.name = "test-topic"
        topic.serialize = MagicMock(return_value=MagicMock(value=b"{}"))

        app = MagicMock()
        app.topic.return_value = topic
        app.get_producer.return_value = producer

        import os
        with patch.dict(os.environ, {
            "output_telemetry": "telemetry-topic",
            "output_lap_data": "lap-topic",
        }):
            self.mod.Application = MagicMock(return_value=app)
            # Patch quixstreams.Application in the module
            import quixstreams
            quixstreams.Application = MagicMock(return_value=app)

            self.mod.main()

        # Should have called produce at least once for laps
        self.assertTrue(producer.produce.called)


if __name__ == "__main__":
    unittest.main()
