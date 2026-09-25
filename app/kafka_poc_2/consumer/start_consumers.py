import subprocess
import sys

consumer_script = "consumer.py"

consumer_1 = subprocess.Popen(
    [
        sys.executable,
        consumer_script,
        "consumer-1",
        "consumer_order_group_1"
    ]
)

consumer_2 = subprocess.Popen(
    [
        sys.executable,
        consumer_script,
        "consumer-2",
        "consumer_order_group_1"
    ]
)

consumer_4 = subprocess.Popen(
    [
        sys.executable,
        consumer_script,
        "consumer-4",
        "consumer_order_group_1"
    ]
)

consumer_5 = subprocess.Popen(
    [
        sys.executable,
        consumer_script,
        "consumer-5",
        "consumer_order_group_1"
    ]
)

consumer_3 = subprocess.Popen(
    [
        sys.executable,
        consumer_script,
        "consumer-3",
        "consumer_order_group_2"
    ]
)

consumer_1.wait()
consumer_2.wait()
consumer_4.wait()
consumer_5.wait()
consumer_3.wait()

