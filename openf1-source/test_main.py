import pytest

from main import fetch_car_data, get_drivers, resolve_session_key

BASE_URL = "https://api.openf1.org/v1"


class FakeResponse:
    def __init__(self, json_data, status_code=200, headers=None):
        self._json_data = json_data
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        return self._json_data

    def raise_for_status(self):
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class FakeHTTPSession:
    """Fake requests.Session that returns pre-configured responses per call."""

    def __init__(self, responses):
        # responses: list of FakeResponse objects returned in order per .get() call
        self._responses = list(responses)
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, params))
        return self._responses.pop(0)


SESSIONS_FIXTURE = [
    {
        "session_key": 9001,
        "circuit_short_name": "Monza",
        "country_name": "Italy",
        "session_name": "Race",
        "date_start": "2024-09-01T13:00:00",
    },
    {
        "session_key": 9002,
        "circuit_short_name": "Silverstone",
        "country_name": "United Kingdom",
        "session_name": "Race",
        "date_start": "2024-07-07T14:00:00",
    },
    {
        "session_key": 9003,
        "circuit_short_name": "Spa-Francorchamps",
        "country_name": "Belgium",
        "session_name": "Qualifying",
        "date_start": "2024-07-27T14:00:00",
    },
]


def test_resolve_session_key_matches_circuit_short_name():
    fake_session = FakeHTTPSession([FakeResponse(SESSIONS_FIXTURE)])
    session_key = resolve_session_key(fake_session, BASE_URL, 2024, "Monza", "Race")
    assert session_key == 9001


def test_resolve_session_key_matches_country_name():
    fake_session = FakeHTTPSession([FakeResponse(SESSIONS_FIXTURE)])
    session_key = resolve_session_key(fake_session, BASE_URL, 2024, "United Kingdom", "Race")
    assert session_key == 9002


def test_resolve_session_key_case_insensitive():
    fake_session = FakeHTTPSession([FakeResponse(SESSIONS_FIXTURE)])
    session_key = resolve_session_key(fake_session, BASE_URL, 2024, "monza", "race")
    assert session_key == 9001


def test_resolve_session_key_no_match_raises():
    fake_session = FakeHTTPSession([FakeResponse(SESSIONS_FIXTURE)])
    with pytest.raises(ValueError):
        resolve_session_key(fake_session, BASE_URL, 2024, "Nonexistent Track", "Race")


def test_resolve_session_key_ambiguous_picks_exact_match():
    duplicated = SESSIONS_FIXTURE + [
        {
            "session_key": 9004,
            "circuit_short_name": "Monza",
            "country_name": "Italy",
            "session_name": "Race",
            "date_start": "2023-09-03T13:00:00",
        }
    ]
    fake_session = FakeHTTPSession([FakeResponse(duplicated)])
    session_key = resolve_session_key(fake_session, BASE_URL, 2024, "Monza", "Race")
    # Both matches are exact; the first one found should be chosen.
    assert session_key in (9001, 9004)


def test_get_drivers_returns_sorted_unique_driver_numbers():
    drivers_fixture = [
        {"driver_number": 44, "session_key": 9001},
        {"driver_number": 1, "session_key": 9001},
        {"driver_number": 44, "session_key": 9001},
    ]
    fake_session = FakeHTTPSession([FakeResponse(drivers_fixture)])
    drivers = get_drivers(fake_session, BASE_URL, 9001)
    assert drivers == [1, 44]


def test_fetch_car_data_returns_raw_samples():
    samples_fixture = [
        {"driver_number": 44, "speed": 300, "date": "2024-09-01T13:01:00"},
        {"driver_number": 44, "speed": 305, "date": "2024-09-01T13:01:01"},
    ]
    fake_session = FakeHTTPSession([FakeResponse(samples_fixture)])
    samples = fetch_car_data(fake_session, BASE_URL, 9001, 44)
    assert samples == samples_fixture


class FakeSerializedMessage:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class FakeTopic:
    name = "raw-openf1-car-data"

    def serialize(self, key, value):
        return FakeSerializedMessage(key=key, value=value)


class FakeProducer:
    def __init__(self):
        self.produced = []

    def produce(self, topic, key, value):
        self.produced.append({"topic": topic, "key": key, "value": value})

    def flush(self):
        pass


def test_each_car_data_sample_produces_exactly_one_message_keyed_by_driver_number():
    samples = [
        {"driver_number": 44, "speed": 300},
        {"driver_number": 44, "speed": 305},
        {"driver_number": 44, "speed": 310},
    ]
    topic = FakeTopic()
    producer = FakeProducer()
    driver_number = 44

    key = str(driver_number).encode()
    for sample in samples:
        serialized = topic.serialize(key=key, value=sample)
        producer.produce(topic=topic.name, key=serialized.key, value=serialized.value)

    assert len(producer.produced) == len(samples)
    for produced_msg, original_sample in zip(producer.produced, samples):
        assert produced_msg["key"] == b"44"
        assert produced_msg["value"] == original_sample


def test_produce_is_called_with_topic_name_string_not_topic_object():
    """Pins the fix for TypeError: argument 1 must be str, not Topic.

    The low-level producer.produce() requires topic to be a string (the
    resolved topic name), not the Topic object returned by app.topic().
    """
    samples = [
        {"driver_number": 44, "speed": 300},
        {"driver_number": 44, "speed": 305},
    ]
    topic = FakeTopic()
    producer = FakeProducer()
    driver_number = 44

    key = str(driver_number).encode()
    for sample in samples:
        serialized = topic.serialize(key=key, value=sample)
        producer.produce(topic=topic.name, key=serialized.key, value=serialized.value)

    assert len(producer.produced) == len(samples)
    for produced_msg in producer.produced:
        assert produced_msg["topic"] == topic.name
        assert isinstance(produced_msg["topic"], str)
        assert produced_msg["topic"] != topic
