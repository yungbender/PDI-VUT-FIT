# Requirements
- Python3

# Python requirements
Create a python virtual environment and install the python requirements.
```
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

# Usage
App connects to the Apache Kafka via the Python3 Kafka package, processes the received data and displays it in the terminal. For graph ploting the [plotext](https://github.com/piccolomo/plotext) package has been used.

```
usage: visualizer-cli.py [-h] [-t {30m,8h,24h}]

CLI data visualizer. It displayes running window and one additional window selected by the argument.

optional arguments:
  -h, --help            show this help message and exit
  -t {30m,8h,24h}, --time-window {30m,8h,24h}
                        Specify the time window to be displayed.
```
