import time
import csv
import threading
import os
from datetime import datetime
import logging

import ADS1263
from gpiozero import Button

import can
import struct

# Local sensor configuration
REF = 5.00
FLOW_SENSOR_PINS = [23, 24]
FLOW_FACTORS = [9.9, 9.89]

pulse_counts = [0, 0]
flow_rates = [0.0, 0.0]
pressure_bars = [0.0, 0.0]
temperatures = [0.0, 0.0]  # Add temperature recording logic if available

data_lock = threading.Lock()
logging.basicConfig(level=logging.ERROR)
logging.getLogger("pymodbus").setLevel(logging.ERROR)

def pulse_inc_0():
    pulse_counts[0] += 1

def pulse_inc_1():
    pulse_counts[1] += 1

flow_sensors = [
    Button(FLOW_SENSOR_PINS[0], pull_up=True),
    Button(FLOW_SENSOR_PINS[1], pull_up=True)
]
flow_sensors[0].when_pressed = pulse_inc_0
flow_sensors[1].when_pressed = pulse_inc_1

def calc_flow(pulse_count, factor):
    return round(pulse_count / factor, 2)

def pressure_thread():
    try:
        adc = ADS1263.ADS1263()
        if adc.ADS1263_init_ADC1('ADS1263_50SPS') == -1:
            print("Failed ADC init")
            return
        adc.ADS1263_SetMode(0)
        channels = [0, 1]
        while True:
            values = adc.ADS1263_GetAll(channels)
            for i in range(2):
                raw = values[i]
                if raw & 0x80000000:
                    raw -= (1 << 32)
                voltage = (raw / 2147483648.0) * REF
                if voltage < 0.5:
                    psi = 0.0
                elif voltage > 4.5:
                    psi = 100.0
                else:
                    psi = ((voltage - 0.5) / 4.0) * 100.0
                bar = psi * 0.0689
                adjusted_bar = 0 if ((0.82 * bar) - 0.017) < 0 else (0.82 * bar) - 0.017
                with data_lock:
                    pressure_bars[i] = adjusted_bar
            time.sleep(0.1)
    except Exception as e:
        print(f"Pressure thread error: {e}")
    finally:
        try:
            adc.ADS1263_Exit()
        except:
            pass

def flow_thread():
    while True:
        time.sleep(1)
        with data_lock:
            flow_rates[0] = calc_flow(pulse_counts[0], FLOW_FACTORS[0])
            flow_rates[1] = calc_flow(pulse_counts[1], FLOW_FACTORS[1])
            pulse_counts[0] = 0
            pulse_counts[1] = 0

# CAN data storage with thread safety
latest_can_data = {
    'rpi1_loadcells': [0.0, 0.0, 0.0, 0.0],
    'rpi1_esc': [0.0]*10,
    'rpi3_adc': [0.0, 0.0]  # ADCValue and Voltage from rpi3
}
can_lock = threading.Lock()

def can_listener():
    bus = can.interface.Bus(channel='can0', bustype='socketcan')
    while True:
        msg = bus.recv()
        if msg is None:
            continue
        with can_lock:
            try:
                if msg.arbitration_id == 0x100:
                    data = struct.unpack('<16f', msg.data)
                    latest_can_data['rpi1_loadcells'] = list(data[:4])
                    latest_can_data['rpi1_esc'] = list(data[6:16])
                elif msg.arbitration_id == 0x110:
                    # Now unpack 2 floats: ADC raw value and voltage
                    latest_can_data['rpi3_adc'] = struct.unpack('<2f', msg.data)
            except Exception as e:
                print(f"CAN decode error: {e}")

def main():
    loc = input("Enter sensor location (default: Clog): ").strip()
    if not loc:
        loc = "Clog"

    os.makedirs("logs", exist_ok=True)
    filename = f"logs/{loc}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    headers = [
        "Timestamp",
        "Pressure_1_Bar", "Pressure_2_Bar",
        "Temperature_1_C", "Temperature_2_C",
        "Flow_1_Lpm", "Flow_2_Lpm",
        "RPI1_Thrust_0", "RPI1_Thrust_90", "RPI1_Thrust_180", "RPI1_Thrust_270",
        "RPI1_Total_Weight", "RPI1_Total_Force",
        "RPI1_ESC_RPM", "RPI1_ESC_Torque", "RPI1_ESC_Motor_Temp", "RPI1_ESC_Current",
        "RPI1_ESC_Vdc", "RPI1_ESC_Vout", "RPI1_ESC_IGBT_Temp", "RPI1_ESC_RPM_Command",
        "RPI1_Battery_Volt", "RPI1_Battery_Bus_Volt",
        "RPI3_ADCValue", "RPI3_Voltage"
    ]

    with open(filename, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

    threading.Thread(target=pressure_thread, daemon=True).start()
    threading.Thread(target=flow_thread, daemon=True).start()
    threading.Thread(target=can_listener, daemon=True).start()

    with open(filename, 'a', newline='') as f:
        writer = csv.writer(f)
        try:
            while True:
                with data_lock:
                    p1, p2 = pressure_bars
                    t1, t2 = temperatures  # Add your temp sensor updates here if available
                    f1, f2 = flow_rates

                with can_lock:
                    thrusts = latest_can_data['rpi1_loadcells']
                    esc = latest_can_data['rpi1_esc']
                    adc = latest_can_data['rpi3_adc']

                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                row = [
                    ts,
                    p1, p2,
                    t1, t2,
                    f1, f2,
                    thrusts[0], thrusts[1], thrusts[2], thrusts[3],
                    esc[4], esc[5],
                    esc[0], esc[1], esc[2], esc[3], esc[4], esc[5], esc[6], esc[7], esc[8], esc[9],
                    adc[0], adc[1]
                ]
                print(f"Logged data: {row}")
                writer.writerow(row)
                f.flush()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nTerminated by user")

if __name__ == "__main__":
    main()
