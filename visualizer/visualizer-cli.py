import os
import sys
import json
from collections import defaultdict
from argparse import ArgumentParser, Namespace
from datetime import datetime
from typing import Dict, List, Iterator

from kafka import KafkaConsumer
import plotext as plt

KAFKA_EGRESS_TOPIC = os.getenv("KAFKA_EGRESS_TOPIC", "spark-egress")

DATA_TYPES = [
    "sourceIp-count",
    "userAgent-count",
    "httpVersion-count",
    "contentType-count",
    "contentLength-average",
]
TIME_WINDOWS = ["window-running", "window-30m", "window-8h", "window-24h"]

PLOT_ROWS = 3
PLOT_COLS = 3
PLOT_LABEL_CHAR_LIMIT = 13
PLOT_CORDS_MAP = {
    # sourceIp
    "sourceIp-count-window-running": (1, 1),
    "sourceIp-count-window-30m": (1, 2),
    "sourceIp-count-window-8h": (1, 2),
    "sourceIp-count-window-24h": (1, 2),
    # userAgent
    "userAgent-count-window-running": (2, 1),
    "userAgent-count-window-30m": (2, 2),
    "userAgent-count-window-8h": (2, 2),
    "userAgent-count-window-24h": (2, 2),
    # httpVersion
    "httpVersion-count-window-running": (3, 1),
    "httpVersion-count-window-30m": (3, 2),
    "httpVersion-count-window-8h": (3, 2),
    "httpVersion-count-window-24h": (3, 2),
    # contentType
    "contentType-count-window-running": (1, 3),
    "contentType-count-window-30m": (2, 3),
    "contentType-count-window-8h": (2, 3),
    "contentType-count-window-24h": (2, 3),
    # contentLength
    "contentLength-average-window-running": (3, 3),
    "contentLength-average-window-30m": (3, 3),
    "contentLength-average-window-8h": (3, 3),
    "contentLength-average-window-24h": (3, 3),
}
PLOT_COLOR_MAP = {"sourceIp": 2, "userAgent": 3, "httpVersion": 4, "contentType": 5}


def parse_args() -> Namespace:
    parser = ArgumentParser(description="CLI PDI")

    parser.add_argument(
        "-t",
        "--time-window",
        dest="time_window",
        default="30m",
        choices=["30m", "8h", "24h"],
        help="Specify the time window to be displayed.",
    )

    return parser.parse_args()


def init_data(windowed_data_types: List[str]) -> Dict:
    data = {}
    for data_type in windowed_data_types:
        _, value_name, _ = data_type.split("-", maxsplit=2)

        if value_name == "average":
            data.update({data_type: {}})
        else:
            data.update({data_type: defaultdict(lambda: {})})
    return data


def get_labels(full_names: List[str]) -> Iterator[str]:
    for name in full_names:
        if len(name) > PLOT_LABEL_CHAR_LIMIT:
            yield f"{name[:PLOT_LABEL_CHAR_LIMIT]}..."
        else:
            yield name


def get_title(windowed_data_type: str) -> str:
    data_type, value_name, _, window = windowed_data_type.split("-")
    return f"{data_type} {value_name} {window}"


def get_average_title(windowed_data_type: str) -> str:
    data_type, _, _, window = windowed_data_type.split("-")
    return f"{data_type} {window}"


def get_color(windowed_data_type: str) -> int:
    data_type, _ = windowed_data_type.split("-", maxsplit=1)
    return PLOT_COLOR_MAP[data_type]


def display_data(data: Dict, windowed_data_types: List[str], latest_window: Dict):

    # init plot grid
    plt.subplots(PLOT_ROWS, PLOT_COLS)

    # aux struct for collecting all the averages values
    averages = {
        data_type: 0.0 for data_type in windowed_data_types if "average" in data_type
    }

    for data_type in windowed_data_types:
        resources = data[data_type]
        if not resources:
            continue

        # handle averages plot
        # TODO: finish the plot logic
        if "average" in data_type:

            averages[data_type] = resources["average"]
            plt.subplot(*PLOT_CORDS_MAP[data_type])
            plt.clt()
            plt.cld()

            plt.bar(
                [
                    get_average_title(average_name)
                    for average_name in averages
                    if averages[average_name] > 0
                ],
                [average for average in averages.values() if average > 0],
            )
            plt.title("Averages")

        # handle rest of the data plot
        else:
            if any("start" in value and "end" in value for value in resources.values()):
                resources = {
                    key: value
                    for key, value in resources.items()
                    if value["start"] == latest_window["start"]
                }

            plt.subplot(*PLOT_CORDS_MAP[data_type])
            plt.clt()
            plt.cld()
            plt.bar(
                [label for label in get_labels(resources)],
                [value["count"] for value in resources.values()],
                color=get_color(data_type),
            )
            plt.frame(True)
            plt.grid(True)
            plt.yticks(ticks=[value["count"] for value in resources.values()])

            if any("start" in value and "end" in value for value in resources.values()):
                plt.title(
                    f"{get_title(data_type)} ({latest_window['start']} - {latest_window['end']})"
                )
            else:
                plt.title(get_title(data_type))

    plt.show()


def main():

    try:
        args = parse_args()
        time_windows = ["window-running", f"window-{args.time_window}"]
        windowed_data_types = [
            f"{data_type}-{time_window}"
            for data_type in DATA_TYPES
            for time_window in time_windows
        ]

        # prepare the sctructure for data storage
        data = init_data(windowed_data_types)

        # init kafka consumer
        consumer = KafkaConsumer(
            KAFKA_EGRESS_TOPIC,
            bootstrap_servers="localhost:29092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=1000,
            value_deserializer=json.loads,
            group_id=None,
        )

        displayed = False
        latest_window = {"start": None, "end": None}

        # poll for the kafka messages infinitelly
        while True:

            for msg in consumer:

                value = msg.value
                data_type = value["data_type"]
                if not data_type in windowed_data_types:
                    continue

                # rerender new data
                displayed = False

                resource, value_name, _ = data_type.split("-", maxsplit=2)

                if value_name == "average":
                    data_storage = data[data_type]
                else:
                    resource_name = value[resource]
                    data_storage = data[data_type][resource_name]

                data_storage[value_name] = value[value_name]
                if "window" in value:
                    data_storage["start"] = datetime.strptime(
                        value["window"]["start"], "%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                    # calculate latest window start
                    latest_window["start"] = (
                        max(
                            data_storage["start"],
                            latest_window["start"],
                        )
                        if latest_window["start"] is not None
                        else data_storage["start"]
                    )

                    data_storage["end"] = datetime.strptime(
                        value["window"]["end"], "%Y-%m-%dT%H:%M:%S.%fZ"
                    )
                    # calculate latest window end
                    latest_window["end"] = (
                        max(
                            data_storage["end"],
                            latest_window["end"],
                        )
                        if latest_window["end"] is not None
                        else data_storage["end"]
                    )
            if not displayed:
                displayed = True
                display_data(data, windowed_data_types, latest_window)

    except KeyboardInterrupt:
        sys.exit(0)


if __name__ == "__main__":
    main()
