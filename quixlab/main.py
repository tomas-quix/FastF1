import quixlab as ql

canvas = ql.Canvas(title="My Notebook", lake_tree_open=['car_telemetry', 'car_telemetry/year=2023', 'car_telemetry/year=2023/circuit=Monza', 'car_telemetry/year=2023/circuit=Monza/session_type=Race', 'car_telemetry/year=2023/circuit=Singapore', 'car_telemetry/year=2023/circuit=Singapore/session_type=Race', 'car_telemetry/year=2023/circuit=Singapore/session_type=Race/session_name=Race'])


@canvas.dataset(position=(-23, 240), size=(770, 645), code_height=200)
def car_telemetry(selection):
    return ql.sql(f"""SELECT *
    FROM car_telemetry
    WHERE year = 2023
      AND circuit = '{selection.selected_circuit}'
      AND session_type = 'Race'
      AND session_name = 'Race'
      AND driver_acronym = '{selection.selected_driver}'
    ORDER BY ts_ms""")


@canvas.cell(position=(1104, 237), size=(977, 948), code_height=378)
def cell_1(car_telemetry):
    import plotly.express as px

    df = car_telemetry.copy()
    df["lap_time_s"] = df.groupby("lap_number")["ts_ms"].transform(lambda s: (s - s.min()) / 1000)
    fig = px.line(
        df.sort_values(["lap_number", "lap_time_s"]),
        x="lap_time_s",
        height=500,
        y="speed",
        color="lap_number",
        labels={"lap_time_s": "Time since lap start (s)", "speed": "Speed (km/h)", "lap_number": "Lap"},
        title="Speed overlay by lap",
    )
    fig.update_layout(legend_title_text="Lap")
    return fig


@canvas.cell(position=(-116, -632), size=(963, 700), code_height=333)
def selection():
    drivers = ql.partition_values("car_telemetry", "driver_acronym")
    circuits = ql.partition_values("car_telemetry", "circuit")

    selected_driver = ql.ui.dropdown(drivers, label="Driver")
    selected_circuit = ql.ui.dropdown(circuits, label="Circuit")

    return selected_circuit, selected_driver


if __name__ == "__main__":
    canvas.serve()
