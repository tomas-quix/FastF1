import quixlab as ql

canvas = ql.Canvas(title="My Notebook", lake_tree_open=['ac_telemetry_prod', 'ac_telemetry_prod/environment=prague_office', 'car_telemetry', 'ac_telemetry_prod/environment=prague_office/test_rig=fanatec_csl_dd', 'ac_telemetry_prod/environment=prague_office/test_rig=fanatec_csl_dd/experiment=tyre_pressure', 'ac_telemetry_prod/environment=prague_office/test_rig=fanatec_csl_dd/experiment=tyre_pressure/driver=tomas neubauer', 'ac_telemetry_prod/environment=prague_office/test_rig=fanatec_csl_dd/experiment=tyre_pressure/driver=tomas neubauer/track=Spa', 'ac_telemetry_prod/environment=prague_office/test_rig=fanatec_csl_dd/experiment=tyre_pressure/driver=tomas neubauer/track=Spa/carModel=porsche_991ii_gt3_r', 'ac_telemetry_prod/environment=prague_office/test_rig=fanatec_csl_dd/experiment=tyre_pressure/driver=tomas neubauer/track=Spa/carModel=porsche_991ii_gt3_r/session_id=2026-06-17T16:04:17.019Z'])


@canvas.dataset(position=(-20, 208), size=(770, 645), code_height=200)
def car_telemetry(selection):
    return ql.sql(f"""SELECT ts_ms, speed, lap_duration, lap_number, is_pit_out_lap
    FROM car_telemetry
    WHERE year = 2023
      AND circuit = '{selection.selected_circuit}'
      AND session_type = 'Race'
      AND session_name = 'Race'
      AND driver_acronym = '{selection.selected_driver}'
    ORDER BY ts_ms""")


@canvas.cell(position=(-116, -632), size=(963, 700), code_height=200)
def selection():
    drivers = ql.partition_values("car_telemetry", "driver_acronym")
    circuits = ql.partition_values("car_telemetry", "circuit")

    selected_driver = ql.ui.dropdown(drivers, label="Driver")
    selected_circuit = ql.ui.dropdown(circuits, label="Circuit")

    return selected_circuit, selected_driver


@canvas.notebook(position=(1128, 85), size=(1228, 847), code_height=200, viz={'outputCell': 0})
def cell_1(car_telemetry):
    # %%
    import plotly.express as px

    df = car_telemetry.copy()

    # Keep only valid race laps: must have a recorded lap time and not be an in/out lap through the pits
    valid = df[df["lap_duration"].notna() & ~df["is_pit_out_lap"].fillna(False).astype(bool)]

    # Pick the 5 fastest laps by lap_duration
    best_lap_numbers = (
        valid.groupby("lap_number")["lap_duration"].first().nsmallest(5).index
    )
    best = valid[valid["lap_number"].isin(best_lap_numbers)].copy()

    best["lap_time_s"] = best.groupby("lap_number")["ts_ms"].transform(lambda s: (s - s.min()) / 1000)

    fig = px.line(
        best.sort_values(["lap_number", "lap_time_s"]),
        x="lap_time_s",
        height=500,
        y="speed",
        color="lap_number",
        labels={"lap_time_s": "Time since lap start (s)", "speed": "Speed (km/h)", "lap_number": "Lap"},
        title="Speed overlay - 5 fastest valid race laps",
    )
    fig.update_layout(legend_title_text="Lap")
    return fig


@canvas.dataset(position=(2903, -1212), size=(1060, 605), code_height=200)
def ac_telemetry_prod():
    return ql.sql("""SELECT *
    FROM ac_telemetry_prod
    WHERE environment = 'prague_office'
      AND test_rig = 'fanatec_csl_dd'
      AND experiment = 'tyre_pressure'
      AND driver = 'tomas neubauer'
      AND track = 'Spa'
      AND carModel = 'porsche_991ii_gt3_r'
      AND session_id = '2026-06-17T16:04:17.019Z'
    ORDER BY timestamp_ms""")


@canvas.cell(position=(4297, -1258), size=(988, 688), code_height=200, viz={'type': 'line', 'x': 'timestamp_ms', 'y': ['gas', 'rpms']})
def cell_2(ac_telemetry_prod):
    return ac_telemetry_prod


@canvas.cell(position=(4124, -494), size=(755, 550), code_height=200)
def cell_3(ac_telemetry_prod):
    import plotly.express as px

    df = ac_telemetry_prod.groupby("lap")["rpms"].agg(min_rpm="min", max_rpm="max").reset_index()
    fig = px.line(df, x="lap", y=["min_rpm", "max_rpm"], markers=True, labels={"value": "RPM", "lap": "Lap", "variable": "Metric"}, title="Min/Max RPM per Lap")
    fig.show()


@canvas.cell(position=(6101, -1280), size=(647, 444), code_height=200, viz={'storagePath': 'quixdev-fastf1-dev', 'storageType': 'folder'})
def quixdev_fastf1_dev():
    ql.StorageFolder("quixdev-fastf1-dev")


@canvas.stream(position=(5853, -2138), size=(560, 420), code_height=200)
def stream_4():
    return ql.topic("formulae-raw", workspace="quixdev-fastf1-dev", offset="earliest", limit=200)


@canvas.cell(position=(6473, -2138), size=(560, 420), code_height=200)
def cell_5(stream_4):
    df = stream_4.df
    return df.tail(20)


if __name__ == "__main__":
    canvas.serve()
