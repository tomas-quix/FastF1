import os
from dotenv import load_dotenv
from quixstreams import Application

load_dotenv()


def make_config_entry(entity_type: str):
    """Return a transform function for the given entity type."""

    def transform(value):
        if entity_type == "sessions":
            config_key = f"openf1/sessions/{value.get('session_key', 'unknown')}"
        elif entity_type == "drivers":
            config_key = f"openf1/drivers/{value.get('driver_number', 'unknown')}"
        elif entity_type == "stints":
            config_key = (
                f"openf1/stints/"
                f"{value.get('driver_number', 'unknown')}/"
                f"{value.get('stint_number', 0)}"
            )
        elif entity_type == "pit":
            config_key = (
                f"openf1/pit/"
                f"{value.get('driver_number', 'unknown')}/"
                f"{value.get('lap_number', value.get('pit_duration', 'unknown'))}"
            )
        elif entity_type == "race_control":
            config_key = (
                f"openf1/race-control/"
                f"{value.get('lap_number', 'unknown')}/"
                f"{value.get('category', 'unknown')}"
            )
        elif entity_type == "weather":
            config_key = (
                f"openf1/weather/"
                f"{value.get('date', value.get('meeting_key', 'unknown'))}"
            )
        elif entity_type == "team_radio":
            config_key = (
                f"openf1/team-radio/"
                f"{value.get('driver_number', 'unknown')}/"
                f"{value.get('date', 'unknown')}"
            )
        else:
            config_key = f"openf1/{entity_type}/unknown"

        return {"key": config_key, "value": value}

    return transform


def main():
    app = Application(
        consumer_group="openf1-metadata-sink",
        auto_offset_reset="earliest",
    )

    # Input topics
    sessions_topic = app.topic(os.environ["input_sessions"], value_deserializer="json")
    drivers_topic = app.topic(os.environ["input_drivers"], value_deserializer="json")
    stints_topic = app.topic(os.environ["input_stints"], value_deserializer="json")
    pit_topic = app.topic(os.environ["input_pit"], value_deserializer="json")
    race_control_topic = app.topic(
        os.environ["input_race_control"], value_deserializer="json"
    )
    weather_topic = app.topic(os.environ["input_weather"], value_deserializer="json")
    team_radio_topic = app.topic(
        os.environ["input_team_radio"], value_deserializer="json"
    )

    # Output topic
    output_topic = app.topic(os.environ["output_config"], value_serializer="json")

    # One SDF per input topic — all fan into the same output topic
    sdf_sessions = app.dataframe(sessions_topic)
    sdf_sessions = sdf_sessions.apply(make_config_entry("sessions"))
    sdf_sessions = sdf_sessions.to_topic(output_topic)

    sdf_drivers = app.dataframe(drivers_topic)
    sdf_drivers = sdf_drivers.apply(make_config_entry("drivers"))
    sdf_drivers = sdf_drivers.to_topic(output_topic)

    sdf_stints = app.dataframe(stints_topic)
    sdf_stints = sdf_stints.apply(make_config_entry("stints"))
    sdf_stints = sdf_stints.to_topic(output_topic)

    sdf_pit = app.dataframe(pit_topic)
    sdf_pit = sdf_pit.apply(make_config_entry("pit"))
    sdf_pit = sdf_pit.to_topic(output_topic)

    sdf_race_control = app.dataframe(race_control_topic)
    sdf_race_control = sdf_race_control.apply(make_config_entry("race_control"))
    sdf_race_control = sdf_race_control.to_topic(output_topic)

    sdf_weather = app.dataframe(weather_topic)
    sdf_weather = sdf_weather.apply(make_config_entry("weather"))
    sdf_weather = sdf_weather.to_topic(output_topic)

    sdf_team_radio = app.dataframe(team_radio_topic)
    sdf_team_radio = sdf_team_radio.apply(make_config_entry("team_radio"))
    sdf_team_radio = sdf_team_radio.to_topic(output_topic)

    app.run()


if __name__ == "__main__":
    main()
