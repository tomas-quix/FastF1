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


@canvas.cell(position=(1104, 237), size=(940, 715), code_height=200, viz={'type': 'line', 'x': 'ts_ms', 'y': ['speed']})
def cell_1(car_telemetry):
    return car_telemetry


@canvas.cell(position=(-116, -632), size=(963, 700), code_height=333)
def selection():
    drivers = ql.partition_values("car_telemetry", "driver_acronym")
    circuits = ql.partition_values("car_telemetry", "circuit")

    selected_driver = ql.ui.dropdown(drivers, label="Driver")
    selected_circuit = ql.ui.dropdown(circuits, label="Circuit")

    return selected_circuit, selected_driver


if __name__ == "__main__":
    canvas.serve()
