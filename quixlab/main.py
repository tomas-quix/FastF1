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

    # %%
    print(best.shape)
    # %%
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


@canvas.cell(position=(1240, -1024), size=(1229, 956), code_height=200)
def cell_2(selection, car_telemetry):
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
    return selection.title, selection.selected_circuit, selection.selected_driver, fig


@canvas.stream(position=(3740, -934), size=(577, 576), code_height=318)
def stream_3():
    return ql.topic("formulae-raw", workspace="quixdev-fastf1-dev", offset="earliest", limit=200, consumer_group="quixlab-formulae-raw-6b8oc6")


@canvas.cell(position=(4491, -934), size=(837, 589), code_height=200)
def cell_4(stream_3):
    df = stream_3.df
    return df.tail(20)


@canvas.chat(position=(4746, -448), size=(380, 480), code_height=0, viz={'storage': 'local', 'topic': 'general'})
def general_w2v2():
    pass


@canvas.plugin(position=(6758, -235), size=(700, 500), code_height=0, viz={'url': 'https://backup-manager-quixdev-acquixbridge-prod.deployments-dev.quix.io', 'pluginId': '9d0fb0a0-8c43-41eb-84b0-ca38afd8b46a', 'pluginName': 'MongoDB Backup Manager'})
def plugin_6():
    pass


@canvas.cell(position=(6808, -1280), size=(560, 420), code_height=200)
def cell_3(quixdev_fastf1_dev):
    return quixdev_fastf1_dev.folders['data-lake'].folders.


if __name__ == "__main__":
    canvas.serve()
