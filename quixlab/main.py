import quixlab as ql

canvas = ql.Canvas(title="My Notebook", lake_tree_open=['car_telemetry', 'car_telemetry/year=2023', 'car_telemetry/year=2023/circuit=Monza', 'car_telemetry/year=2023/circuit=Monza/session_type=Race', 'car_telemetry/year=2023/circuit=Singapore', 'car_telemetry/year=2023/circuit=Singapore/session_type=Race', 'car_telemetry/year=2023/circuit=Singapore/session_type=Race/session_name=Race'])


@canvas.dataset(position=(179, 221), size=(770, 645), code_height=200)
def car_telemetry(selection):
    return ql.sql(f"""SELECT *
    FROM car_telemetry
    WHERE year = 2023
      AND circuit = 'Monza'
      AND session_type = 'Race'
      AND session_name = 'Race'
      AND driver_acronym = '{selection}'
    ORDER BY ts_ms""")


@canvas.cell(position=(1104, 237), size=(757, 612), code_height=200, viz={'type': 'line', 'x': 'ts_ms', 'y': ['speed']})
def cell_1(car_telemetry):
    return car_telemetry


@canvas.cell(position=(-116, -632), size=(963, 700), code_height=333)
def selection():
    drivers = ql.partition_values("car_telemetry", "driver_acronym")

    selected_driver = ql.ui.dropdown(drivers, label="Driver")

    return selected_driver


if __name__ == "__main__":
    canvas.serve()
