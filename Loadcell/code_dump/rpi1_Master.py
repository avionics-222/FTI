'''
import socket
import time
import csv
from datetime import datetime

# UDP receive setup
UDP_IP = "0.0.0.0"  # IP of RPi1 (master)
UDP_PORT = 5005
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # Optional, helps with re-running
sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256 * 1024)
sock.bind((UDP_IP, UDP_PORT))  # Make sure UDP_IP is "0.0.0.0" or your local IP
sock.settimeout(0.001)

print("master ready, waiting for data") 

# Logging setup
log_interval = 1.0 / 400.0  # 400 Hz
csv_filename = "rpi1_udp_log_09_07.csv"

# Open combined CSV log file
with open(csv_filename, mode="w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow([
        "Type", "Timestamp", "Location", "X", "Y", "Z",
        "RPi1_Counter", "RPi1_Timestamp", "RPi2_Counter", "RPi2_Timestamp", "Packet_Age (s)"
    ])

rpi1_counter = 0
log_count = 0
last_rpi2_counter = "-"
last_rpi2_timestamp = "-"
last_packet_received_time = None

def format_timestamp(ts_float):
    """Convert float timestamp to 'HH:MM:SS:msms' format"""
    dt = datetime.fromtimestamp(ts_float)
    return dt.strftime("%H:%M:%S.%f")[:-4]  # gives up to milliseconds

print("Logging started...")

try:
    while True:
        loop_start_time = time.time()

        # Read UDP messages
        while True:
            try:
                data, addr = sock.recvfrom(1024)
                message = data.decode()

                if message.startswith("ACCEL"):
                    parts = message.strip().split(",")
                    print("Received ACCEL:", message)  # DEBUG LINE
                    if len(parts) == 6:
                        _, loc, ts, x, y, z = parts
                        with open(csv_filename, mode="a", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow([
                                "ACCEL", ts, loc, x, y, z,
                                "-", "-", "-", "-", "-"
                            ])

                else:  # Assume counter,timestamp format
                    rpi2_counter_str, rpi2_timestamp_str = message.strip().split(",")
                    last_rpi2_counter = int(rpi2_counter_str)
                    last_rpi2_timestamp = float(rpi2_timestamp_str)

            except socket.timeout:
                break

        # RPi1 counter updated every alternate cycle
        if log_count % 2 == 0:
            rpi1_counter += 1
            rpi1_counter_str = str(rpi1_counter)
        else:
            rpi1_counter_str = "-"

        rpi1_timestamp = time.time()

        if isinstance(last_rpi2_timestamp, float):
            packet_age = rpi1_timestamp - last_rpi2_timestamp
            rpi2_ts_formatted = format_timestamp(last_rpi2_timestamp)
        else:
            packet_age = "-"
            rpi2_ts_formatted = "-"

        rpi1_ts_formatted = format_timestamp(rpi1_timestamp)

        # Log to CSV
        with open(csv_filename, mode="a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                rpi1_counter_str,
                rpi1_ts_formatted,
                last_rpi2_counter if last_rpi2_counter != "-" else "-",
                rpi2_ts_formatted,
                f"{packet_age:.9f}" if packet_age != "-" else "-"
            ])

        log_count += 1
        sleep_time = log_interval - (time.time() - loop_start_time)
        if sleep_time > 0:
            time.sleep(sleep_time)

except KeyboardInterrupt:
    print("Logging stopped by user.")
'''
import multiprocessing
import socket
import time
import can_v1
import can
import argparse
import sys
import os
import csv
from datetime import datetime
from math import sqrt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
import signal

running = True

def handle_termination(signum, frame):
    global running
    print("Termination signal received. Cleaning up...")
    running = False

signal.signal(signal.SIGTERM, handle_termination)

TIMEOUT_SECONDS = 30

# Global variables
CAN_CYCLIC_RATE = 0x3C
can_bus_timeout = 0.05
kt_value = 0.88
nominal_rpm = 6000
multi_rate = 0.5
num_esc_param = 11

# Cloud / InfluxDB config
cloud_test = 0  # 1 = cloud active, else inactive
token = 'ZdwOtVTj7kWLmHqBk_p7d3Snz1Gbt2Xm1EPNUlY96xUxhV93wtU1KWb5mBZ6ubRl8I_m-ty0Jnxbb38Nl5nrFg=='
influxdb_url = "https://us-east-1-1.aws.cloud2.influxdata.com"
org = "IdeaForge"
bucket = "Thrust_Rig_v1"

client = InfluxDBClient(url=influxdb_url, token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

def udp_listener(queue, location):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 256 * 1024)
    sock.bind(("0.0.0.0", 5005))
    sock.settimeout(0.001)
    while running:
        try:
            data, addr = sock.recvfrom(1024)
            message = data.decode()
            if message.startswith("ACCEL"):
                parts = message.strip().split(",")
                print("Received ACCEL:", message)  # Debug print
                if len(parts) == 6:
                    _, loc, ts, x, y, z = parts
                    with open(csv_filename, mode="a", newline="") as f:
                        writer = csv.writer(f)
                        writer.writerow(["ACCEL", ts, loc, x, y, z])
                    # Put accel data on queue
                    queue.put(("Accelerometer", (float(x), float(y), float(z))))
        except socket.timeout:
            continue

def loadcell_worker(dout, sck, load_cell_label, offset, queue, index):
    from hx711_module import HX711
    hx = HX711(dout, sck)
    hx.offset = offset
    while running:
        try:
            weight_out, raw = hx.get_weight()
            queue.put((index, weight_out))
            time.sleep(multi_rate)
        except Exception as e:
            print(f"Load cell {load_cell_label} error: {e}")
            time.sleep(multi_rate)

def send_rpm_and_torque_and_kt(CAN_CYCLIC_RATE=CAN_CYCLIC_RATE):
    global kt_value
    try:
        cyclic_rpm_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.RPM_REGISTER, CAN_CYCLIC_RATE], is_extended_id=False)
        cyclic_torque_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.TORQUE_REGISTER, CAN_CYCLIC_RATE], is_extended_id=False)
        cyclic_temp_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.TEMP_REGISTER, CAN_CYCLIC_RATE], is_extended_id=False)
        cyclic_current_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.CURRENT_REGISTER, CAN_CYCLIC_RATE], is_extended_id=False)
        cyclic_vdc_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.VDC_REGISTER, CAN_CYCLIC_RATE], is_extended_id=False)
        cyclic_vout_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.VOUT_VXXX, CAN_CYCLIC_RATE], is_extended_id=False)
        cyclic_igbt_temp_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.IGBT_TEMP, CAN_CYCLIC_RATE], is_extended_id=False)
        kt_req = can.Message(arbitration_id=can_v1.BAMOCAR_ID, data=[0x3D, can_v1.KT_REGISTER, 0x00], is_extended_id=False)

        kt_received = False
        with can_v1.can.interface.Bus(channel=can_v1.CAN_INTERFACE, interface="socketcan") as bus:
            while not kt_received and running:
                bus.send(kt_req)
                time.sleep(0.1)
                message = bus.recv(timeout=can_bus_timeout)
                if message and message.arbitration_id == can_v1.BAMOCAR_RESPONSE_ID:
                    data = message.data
                    if data[0] == can_v1.KT_REGISTER:
                        kt_raw = (data[2] << 8) | data[1]
                        kt_value = kt_raw * 0.001
                        print(f"Kt Value: {kt_value}")
                        kt_received = True
            for msg in [cyclic_rpm_req, cyclic_torque_req, cyclic_temp_req, cyclic_current_req, cyclic_vdc_req, cyclic_vout_req, cyclic_igbt_temp_req]:
                bus.send(msg)
                time.sleep(0.1)
            print("Sent RPM, Torque, Temp, Current requests")
    except Exception as e:
        print(f"Error sending RPM/Torque/KT: {e}")

def temp_out(temperature_raw):
    temperature_actual = 0.0000003 * temperature_raw ** 2 + 0.0103 * temperature_raw - 127.43
    return round(temperature_actual, 1)

def igbt_temp_out(igbt_temp_raw):
    igbt_temp_value = 6e-08 * igbt_temp_raw ** 2 + 0.0032 * igbt_temp_raw - 23.236
    return round(igbt_temp_value)

def read_rpm_and_torque():
    global kt_value
    try:
        with can_v1.can.interface.Bus(channel=can_v1.CAN_INTERFACE, interface="socketcan") as bus:
            while running:
                message = bus.recv(timeout=can_bus_timeout)
                if message and message.arbitration_id == can_v1.BAMOCAR_RESPONSE_ID:
                    data = message.data
                    if data[0] == can_v1.RPM_REGISTER:
                        rpm_raw = (data[2] << 8) | data[1]
                        if rpm_raw > 32767:
                            rpm_raw -= 65536
                        rpm = round(rpm_raw * nominal_rpm / 32767, 2)
                        queue.put(("RPM", rpm))
                    elif data[0] == can_v1.TORQUE_REGISTER:
                        torque_raw = (data[2] << 8) | data[1]
                        torque = round(torque_raw * 169.7 * kt_value / (32767 * sqrt(2)), 2)
                        if torque < 200:
                            queue.put(("Torque", torque))
                    elif data[0] == can_v1.TEMP_REGISTER:
                        temperature_raw = (data[2] << 8) | data[1]
                        temperature_actual = temp_out(temperature_raw)
                        queue.put(("Temperature", temperature_actual))
                    elif data[0] == can_v1.CURRENT_REGISTER:
                        current_raw = (data[2] << 8) | data[1]
                        if current_raw > 32767:
                            current_raw -= 65536
                        current = round(current_raw * 0.138768, 2)
                        queue.put(("Current", current))
                    elif data[0] == can_v1.VDC_REGISTER:
                        vdc_raw = (data[2] << 8) | data[1]
                        if vdc_raw > 32767:
                            vdc_raw -= 65536
                        vdc = round(vdc_raw * 0.0316635, 2)
                        queue.put(("Vdc", vdc))
                    elif data[0] == can_v1.VOUT_VXXX:
                        vout_vxxx = (data[2] << 8) | data[1]
                        if vout_vxxx > 32767:
                            vout_vxxx -= 65536
                        vout = round(vout_vxxx, 2)
                        vout_ph = vout / 4096 * vdc / sqrt(2) * 0.92
                        queue.put(("Vout", vout_ph))
                    elif data[0] == can_v1.IGBT_TEMP:
                        igbt_temp_raw = (data[2] << 8) | data[1]
                        igbt_temp_value = igbt_temp_out(igbt_temp_raw)
                        queue.put(("IGBT_Temp", igbt_temp_value))
                time.sleep(0.02)
    except Exception as e:
        print(f"Error reading CAN data: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Thrust Rig Data Logger")
    parser.add_argument("-o", "--output", help="Output CSV file name")
    parser.add_argument("-l", "--location", default="RPI2", help="Location tag for UDP accel data")
    # Add mode argument here:
    parser.add_argument("--mode", choices=["esc_on", "esc_off"], default="esc_off", help="ESC operation mode")
    args = parser.parse_args()

    if args.output:
        csv_filename = args.output
    else:
        csv_filename = "data" + datetime.now().strftime("_%Y-%m-%d_%H-%M-%S") + ".csv"

    queue = multiprocessing.Queue()

    # Setup signal handler
    signal.signal(signal.SIGINT, handle_termination)
    signal.signal(signal.SIGTERM, handle_termination)

    # Load cell GPIO pins - update with your pins
    loadcell_pins = [
        {"dout": 6, "sck": 5, "label": "LoadCell1", "offset": 2738, "index": 1},
        {"dout": 23, "sck": 22, "label": "LoadCell2", "offset": 2130, "index": 2},
        {"dout": 18, "sck": 17, "label": "LoadCell3", "offset": 3718, "index": 3},
        {"dout": 19, "sck": 26, "label": "LoadCell4", "offset": 3118, "index": 4},
    ]

    # Start UDP listener process
    udp_process = multiprocessing.Process(target=udp_listener, args=(queue, args.location))
    udp_process.start()

    # Start Load cell processes
    loadcell_processes = []
    for pinset in loadcell_pins:
        p = multiprocessing.Process(target=loadcell_worker, args=(
            pinset["dout"], pinset["sck"], pinset["label"], pinset["offset"], queue, pinset["index"]))
        p.start()
        loadcell_processes.append(p)

    # Start CAN RPM & Torque sender thread/process
    can_sender_process = multiprocessing.Process(target=send_rpm_and_torque_and_kt)
    can_sender_process.start()

    # Start CAN reader process
    can_reader_process = multiprocessing.Process(target=read_rpm_and_torque)
    can_reader_process.start()

    # Open CSV file and write header
    with open(csv_filename, mode="w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Timestamp", "RPM", "Torque", "Temperature", "Current", "Vdc", "Vout", "IGBT_Temp",
                         "LoadCell1", "LoadCell2", "LoadCell3", "LoadCell4", "Accel_X", "Accel_Y", "Accel_Z"])

    # State variables
    rpm = torque = temperature = current = vdc = vout = igbt_temp = None
    loadcells = {1: None, 2: None, 3: None, 4: None}
    accel_x = accel_y = accel_z = None

    print(f"Starting data logging to {csv_filename}...")

    try:
        with open(csv_filename, mode="a", newline="") as csvfile:
            writer = csv.writer(csvfile)
            last_write_time = time.time()
            while running:
                try:
                    while not queue.empty():
                        item = queue.get()
                        if item[0] == "RPM":
                            rpm = item[1]
                        elif item[0] == "Torque":
                            torque = item[1]
                        elif item[0] == "Temperature":
                            temperature = item[1]
                        elif item[0] == "Current":
                            current = item[1]
                        elif item[0] == "Vdc":
                            vdc = item[1]
                        elif item[0] == "Vout":
                            vout = item[1]
                        elif item[0] == "IGBT_Temp":
                            igbt_temp = item[1]
                        elif isinstance(item[0], int):  # Load cell data by index
                            loadcells[item[0]] = item[1]
                        elif item[0] == "Accelerometer":
                            accel_x, accel_y, accel_z = item[1]

                    current_time = time.time()
                    if current_time - last_write_time >= multi_rate:
                        timestamp = datetime.now().isoformat()
                        row = [
                            timestamp,
                            rpm if rpm is not None else "",
                            torque if torque is not None else "",
                            temperature if temperature is not None else "",
                            current if current is not None else "",
                            vdc if vdc is not None else "",
                            vout if vout is not None else "",
                            igbt_temp if igbt_temp is not None else "",
                            loadcells[1] if loadcells[1] is not None else "",
                            loadcells[2] if loadcells[2] is not None else "",
                            loadcells[3] if loadcells[3] is not None else "",
                            loadcells[4] if loadcells[4] is not None else "",
                            accel_x if accel_x is not None else "",
                            accel_y if accel_y is not None else "",
                            accel_z if accel_z is not None else ""
                        ]
                        writer.writerow(row)
                        csvfile.flush()
                        last_write_time = current_time

                        if cloud_test == 1:
                            # Write to InfluxDB
                            point = Point("thrust_rig").tag("location", args.location).field("rpm", rpm or 0)\
                                .field("torque", torque or 0).field("temperature", temperature or 0)\
                                .field("current", current or 0).field("vdc", vdc or 0).field("vout", vout or 0)\
                                .field("igbt_temp", igbt_temp or 0)\
                                .field("loadcell1", loadcells[1] or 0).field("loadcell2", loadcells[2] or 0)\
                                .field("loadcell3", loadcells[3] or 0).field("loadcell4", loadcells[4] or 0)\
                                .field("accel_x", accel_x or 0).field("accel_y", accel_y or 0).field("accel_z", accel_z or 0)\
                                .time(datetime.utcnow())
                            write_api.write(bucket=bucket, org=org, record=point)
                    time.sleep(0.01)
                except KeyboardInterrupt:
                    break
    finally:
        print("Terminating all processes...")
        running = False
        udp_process.terminate()
        can_sender_process.terminate()
        can_reader_process.terminate()
        for p in loadcell_processes:
            p.terminate()
        print("Shutdown complete.")
