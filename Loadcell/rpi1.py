import multiprocessing
import time
import can_v1
import can
import argparse
import os
import csv
import struct
from datetime import datetime
from math import sqrt
import signal

running = True
def handle_termination(signum, frame):
    global running
    print("Termination signal received. Cleaning up...")
    running = False
signal.signal(signal.SIGTERM, handle_termination)

TIMEOUT_SECONDS = 30
CAN_CYCLIC_RATE = 0x3C
can_bus_timeout = 0.05
kt_value = 0.88
nominal_rpm = 6000
multi_rate = 0.5
num_esc_param = 13  # total parameters being tracked

def loadcell_worker(dout, sck, label, offset, queue, index):
    from hx711_module import HX711
    hx = HX711(dout, sck)
    hx.offset = offset
    try:
        while True:
            weight_out, raw = hx.get_weight()
            queue.put((index, weight_out))
            time.sleep(multi_rate)
    except KeyboardInterrupt:
        print(f"Stopped: {label}")

def send_rpm_and_torque_and_kt():
    # ... Use your full existing code to send RPM/Torque/KT requests ...
    pass  # Assume existing code here, unchanged

def temp_out(temperature_raw):
    temperature_actual = 0.0000003 * temperature_raw ** 2 + 0.0103 * temperature_raw - 127.43
    return round(temperature_actual, 1)

def igbt_temp_out(igbt_temp_raw):
    igbt_temp_value = 6e-08 * igbt_temp_raw * igbt_temp_raw + 0.0032 * igbt_temp_raw - 23.236
    return round(igbt_temp_value)

def read_rpm_and_torque(queue):
    # ... Your full existing ESC reading code, putting results into queue ...
    pass  # Assume existing code here, unchanged

def can_broadcast_worker(shared_values):
    bus = can.interface.Bus(channel="can1", bustype="socketcan")
    while running:
        # Pack and send all parameters as floats:
        # Order: Thrust_0, Thrust_90, Thrust_180, Thrust_270, Total Weight, Total Force,
        # RPM, ESC_Torque, Motor Temp, Current, Vdc, Vout, IGBT Temp, RPM Command,
        # Battery Volt, Battery Bus Volt
        payload = struct.pack('<6f10f',
                              shared_values[0], shared_values[1], shared_values[2], shared_values[3],  # Thrust 4
                              shared_values[4],  # Total Weight (Kg)
                              shared_values[5],  # Total Force (N)
                              shared_values[6],  # RPM
                              shared_values[7],  # ESC_Torque
                              shared_values[8],  # Motor Temp
                              shared_values[9],  # Current
                              shared_values[10], # Vdc
                              shared_values[11], # Vout
                              shared_values[12], # IGBT Temp
                              shared_values[13], # RPM Command
                              shared_values[14], # Battery Volt
                              shared_values[15]) # Battery Bus Volt
        # Arbitration id 0x100 used for all data packet
        msg = can.Message(arbitration_id=0x100, data=payload, is_extended_id=False)
        try:
            bus.send(msg)
        except can.CanError as e:
            print(f"Error sending CAN message: {e}")
        time.sleep(1)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ESC Data Reader and Broadcaster")
    parser.add_argument('--mode', type=str, choices=["esc_on", "esc_off", "esc_only"], default="esc_on",
                        help='Mode to read: ESC ON, ESC OFF or ESC ONLY')
    args = parser.parse_args()

    esc_data_state = 1 if args.mode in ["esc_on", "esc_only"] else 0
    load_cell_state = 1 if args.mode != "esc_only" else 0

    load_cells_config = [
        {"dout": 12, "sck": 13},
        {"dout": 20, "sck": 21},
        {"dout": 26, "sck": 19},
        {"dout": 23, "sck": 24}
    ]

    hardcoded_offsets = [311303.0, 1043999.0, 1099991.0, -821255.5]
    load_cell_labels = ["Thrust_0Deg", "Thrust_90Deg", "Thrust_180Deg", "Thrust_270Deg"]

    queue = multiprocessing.Queue()
    process_list = []

    number_lc = 4

    # Shared values array for CAN broadcast (16 floats)
    from multiprocessing import Array
    shared_values = Array('f', [0.0]*16)

    # Set up CSV logging
    os.makedirs("logs", exist_ok=True)
    csv_filename = f"logs/{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    csv_file = open(csv_filename, mode='w', newline='')
    csv_writer = csv.writer(csv_file)

    headers = load_cell_labels + ["Total Weight (Kg)", "Thrust (N)", "RPM", "ESC_Torque (Nm)", "Motor Temp (°C)", 
                                 "Current (A)", "Vdc (V)", "Vout (V)", "IGBT Temp (°C)", "RPM Command", 
                                 "Battery Volt (V)", "Battery Bus Volt (V)"]
    csv_writer.writerow(headers)

    if load_cell_state:
        for i in range(number_lc):
            p = multiprocessing.Process(target=loadcell_worker,
                                        args=(load_cells_config[i]["dout"], load_cells_config[i]["sck"],
                                              load_cell_labels[i], hardcoded_offsets[i], queue, i))
            process_list.append(p)
            p.start()
    if esc_data_state:
        esc_proc = multiprocessing.Process(target=read_rpm_and_torque, args=(queue,))
        process_list.append(esc_proc)
        esc_proc.start()

    broadcast_proc = multiprocessing.Process(target=can_broadcast_worker, args=(shared_values,))
    broadcast_proc.start()

    latest_weights = [0.0] * number_lc
    ESC_data = [0.0] * num_esc_param

    try:
        while running:
            time.sleep(can_bus_timeout)
            while not queue.empty():
                key, val = queue.get()
                if isinstance(key, int):  # Load cell indices 0-3
                    latest_weights[key] = round(val, 2)
                else:
                    # Map keys to ESC params here (example order):
                    # Assuming queue keys are strings like "RPM", "Torque", "Temperature", "Current", etc.
                    if key == "RPM":
                        ESC_data[0] = val
                    elif key == "Torque":
                        ESC_data[1] = val
                    elif key == "Temperature":
                        ESC_data[2] = val
                    elif key == "Current":
                        ESC_data[3] = val
                    elif key == "Vdc":
                        ESC_data[4] = val
                    elif key == "Vout":
                        ESC_data[5] = val
                    elif key == "Igbt":
                        ESC_data[6] = val
                    elif key == "RPM_Command":
                        ESC_data[7] = val
                    elif key == "Battery_Volt":
                        ESC_data[8] = val
                    elif key == "Battery_Bus_Volt":
                        ESC_data[9] = val
                    # Extend as needed for rest parameters

            # Calculate totals
            total_weight = sum(latest_weights)
            total_force = total_weight * 9.8

            # Update shared_values for CAN broadcast
            for i in range(number_lc):
                shared_values[i] = latest_weights[i]
            shared_values[4] = total_weight
            shared_values[5] = total_force
            # ESC data indexes start at shared_values[6]
            for i in range(10):
                shared_values[6 + i] = ESC_data[i] if i < len(ESC_data) else 0.0

            # Log to CSV
            row = latest_weights + [total_weight, total_force] + ESC_data[:10]
            csv_writer.writerow(row)
            csv_file.flush()

            print("Logged data:", row)

    except KeyboardInterrupt:
        print("Keyboard interrupt received, stopping...")
    finally:
        for p in process_list:
            p.terminate()
            p.join()
        broadcast_proc.terminate()
        broadcast_proc.join()
        csv_file.close()
