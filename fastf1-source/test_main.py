"""Unit tests for SessionNotAvailableError handling in main.py."""
import importlib
import sys
import types
import unittest
from unittest.mock import MagicMock, patch, call


def _make_fastf1_stub(raise_on_get_session=None, raise_on_load=None):
    """Build a minimal fastf1 stub that optionally raises on get_session or load."""
    stub = types.ModuleType("fastf1")
    stub.Cache = MagicMock()

    exceptions_mod = types.ModuleType("fastf1.exceptions")

    class DataNotLoadedError(Exception):
        pass

    class SessionNotAvailableError(Exception):
        pass

    exceptions_mod.DataNotLoadedError = DataNotLoadedError
    exceptions_mod.SessionNotAvailableError = SessionNotAvailableError
    stub.DataNotLoadedError = DataNotLoadedError
    stub.SessionNotAvailableError = SessionNotAvailableError
    stub.exceptions = exceptions_mod

    fake_session = MagicMock()
    fake_session.drivers = ["1"]
    fake_session.laps = MagicMock()
    fake_session.laps.empty = True
    fake_session.pos_data = {}

    if raise_on_load:
        fake_session.load.side_effect = raise_on_load
    if raise_on_get_session:
        stub.get_session = MagicMock(side_effect=raise_on_get_session)
    else:
        stub.get_session = MagicMock(return_value=fake_session)

    return stub, exceptions_mod


def _load_main(fastf1_stub, exceptions_mod):
    """Import main with patched fastf1 and quixstreams."""
    # Remove cached module if present
    for mod in list(sys.modules.keys()):
        if mod in ("main", "fastf1", "fastf1.exceptions", "quixstreams", "dotenv"):
            del sys.modules[mod]

    sys.modules["fastf1"] = fastf1_stub
    sys.modules["fastf1.exceptions"] = exceptions_mod

    qs_stub = types.ModuleType("quixstreams")
    qs_stub.Application = MagicMock(return_value=MagicMock(
        topic=MagicMock(return_value=MagicMock(name="test-topic"))
    ))
    sys.modules["quixstreams"] = qs_stub

    dotenv_stub = types.ModuleType("dotenv")
    dotenv_stub.load_dotenv = MagicMock()
    sys.modules["dotenv"] = dotenv_stub

    import importlib.util, os
    spec = importlib.util.spec_from_file_location(
        "main",
        os.path.join(os.path.dirname(__file__), "main.py"),
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class TestStreamSessionNotAvailable(unittest.TestCase):
    def _get_exc_classes(self, mod):
        return mod.SessionNotAvailableError, mod.DataNotLoadedError

    def test_returns_false_when_get_session_raises_session_not_available(self):
        stub, exc_mod = _make_fastf1_stub()
        mod = _load_main(stub, exc_mod)
        stub.get_session.side_effect = mod.SessionNotAvailableError("no data")

        producer = MagicMock()
        result = mod.stream_session(producer, "Bahrain Grand Prix", "R", 2025, round_num="1")

        self.assertFalse(result)
        producer.produce.assert_not_called()

    def test_returns_false_when_load_raises_session_not_available(self):
        stub, exc_mod = _make_fastf1_stub()
        mod = _load_main(stub, exc_mod)
        session_mock = MagicMock()
        session_mock.load.side_effect = mod.SessionNotAvailableError("no data")
        stub.get_session.return_value = session_mock

        producer = MagicMock()
        result = mod.stream_session(producer, "Bahrain Grand Prix", "Q", 2025, round_num="1")

        self.assertFalse(result)
        producer.produce.assert_not_called()

    def test_other_exceptions_propagate_from_get_session(self):
        stub, exc_mod = _make_fastf1_stub()
        mod = _load_main(stub, exc_mod)
        stub.get_session.side_effect = RuntimeError("network failure")

        producer = MagicMock()
        with self.assertRaises(RuntimeError):
            mod.stream_session(producer, "Bahrain Grand Prix", "R", 2025, round_num="1")

    def test_other_exceptions_propagate_from_load(self):
        stub, exc_mod = _make_fastf1_stub()
        mod = _load_main(stub, exc_mod)
        session_mock = MagicMock()
        session_mock.load.side_effect = ValueError("bad data")
        stub.get_session.return_value = session_mock

        producer = MagicMock()
        with self.assertRaises(ValueError):
            mod.stream_session(producer, "Bahrain Grand Prix", "R", 2025, round_num="1")

    def test_warning_log_format(self):
        stub, exc_mod = _make_fastf1_stub()
        mod = _load_main(stub, exc_mod)
        stub.get_session.side_effect = mod.SessionNotAvailableError("no data")

        producer = MagicMock()
        with self.assertLogs("main", level="WARNING") as cm:
            mod.stream_session(producer, "Bahrain Grand Prix", "R", 2025, round_num="3")

        self.assertTrue(
            any("No data available for session 2025 Round 3 [R] - skipping" in line for line in cm.output),
            f"Expected warning not found in: {cm.output}",
        )

    def test_returns_true_when_session_loads_but_no_laps(self):
        """Session loaded fine but laps raised DataNotLoadedError -> still True."""
        stub, exc_mod = _make_fastf1_stub()
        mod = _load_main(stub, exc_mod)
        session_mock = MagicMock()
        session_mock.drivers = []
        type(session_mock).laps = property(
            lambda self: (_ for _ in ()).throw(mod.DataNotLoadedError("no laps"))
        )
        stub.get_session.return_value = session_mock

        producer = MagicMock()
        result = mod.stream_session(producer, "Bahrain Grand Prix", "R", 2025, round_num="1")
        self.assertTrue(result)


if __name__ == "__main__":
    unittest.main()
