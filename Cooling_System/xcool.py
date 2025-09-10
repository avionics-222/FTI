
import time
import csv
import threading
import os
from datetime import datetime
import logging
from pymodbus.client import ModbusSerialClient
import board
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
from gpiozero import Button
import traceback

# ---- Config ----
RS485_PORT = '/dev/ttyAMA0'
BAUD_RATE = 9600
REF = 3.3
FLOW_SENSOR_PINS = [23, 24]
FLOW_FACTORS = [9.9, 9.89]

pulse_counts = [0, 0]
flow_rates = [0.0, 0.0]
pressure_bars = [0.0, 0.0]
temperatures = [0.0, 0.0]
off_t2 = 9.2
off_t1 = 6.7

data_lock = threading.Lock()
modbus_lock = threading.Lock()
i2c_lock = threading.Lock()

# ---- Logging ----
logging.basicConfig(level=logging.ERROR)
logging.getLogger("pymodbus").setLevel(logging.ERROR)
logging.getLogger("pymodbus.logging").setLevel(logging.ERROR)
logging.getLogger("serial").setLevel(logging.ERROR)

# ---- Flow sensor ----
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

def flow_thread():
    while True:
        time.sleep(1)
        with data_lock:
            flow_rates[0] = calc_flow(pulse_counts[0], FLOW_FACTORS[0])
            flow_rates[1] = calc_flow(pulse_counts[1], FLOW_FACTORS[1])
            pulse_counts[0] = 0
            pulse_counts[1] = 0

# ---- Pressure thread ----
def pressure_thread(ads, channels):
    try:
        while True:
            with i2c_lock:
                for i in range(2):
                    try:
                        voltage = channels[i].voltage
                        #print(f"Channel {i} Voltage: {voltage:.4f} V")  # Debug
                        if voltage < 0.5:
                            psi = 0.0
                        elif voltage > (REF - 0.8):
                            psi = 100.0
                        else:
                            psi = ((voltage - 0.5) / (REF - 0.5)) * 100.0
                        bar = psi * 0.0689
                        bar = 0 if (0.82 * bar - 0.017) < 0 else (0.82 * bar - 0.017)
                        with data_lock:
                            pressure_bars[i] = bar
                        time.sleep(0.1)  # Delay between reads
                    except Exception as e:
                        print(f"Error reading channel {i}: {e}")
                        # Continue instead of crashing
                        with data_lock:
                            pressure_bars[i] = -1.0  # Indicate error
            time.sleep(0.5)  # Delay between loops
    except Exception as e:
        print(f"Pressure thread error: {e}")
        traceback.print_exc()

# ---- RS485 Temp thread ----
def rs485_temp_thread():
    client = None
    try:
        client = ModbusSerialClient(
            port=RS485_PORT, baudrate=BAUD_RATE,
            parity='N', stopbits=1, bytesize=8, timeout=3
        )
        if not client.connect():
            print("Could not connect to RS485")
            return
        while True:
            for idx, dev_id in enumerate([1, 3]):
                with modbus_lock:
                    if hasattr(client, "socket") and hasattr(client.socket, "reset_input_buffer"):
                        try:
                            client.socket.reset_input_buffer()
                        except:
                            pass
                    rr = client.read_holding_registers(address=0, count=1, device_id=dev_id)
                    temp = 0.0
                    if not rr.isError():
                        raw = rr.registers[0]
                        temp = (raw - 65536)/10 if (raw & 0x8000) else raw/10
                    with data_lock:
                        temperatures[idx] = temp
                    time.sleep(0.3)
            time.sleep(0.5)
    except Exception as e:
        print(f"RS485 temp thread error: {e}")
    finally:
        if client:
            client.close()

# ---- Main ----
def main():
    i2c = None
    try:
        i2c = busio.I2C(board.SCL, board.SDA)
        ads = ADS.ADS1115(i2c)
        ads.gain = 1
        channels = [AnalogIn(ads, ADS.P0), AnalogIn(ads, ADS.P1)]

        loc = input("Enter sensor location (default: Clog): ").strip() or "Clog"
        timestamp_suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_folder = "logs"
        os.makedirs(log_folder, exist_ok=True)
        filename = os.path.join(log_folder, f"{loc}_{timestamp_suffix}.csv")

        with open(filename, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["Timestamp", "Pressure1_Bar", "Flow1_Lpm", "Temp1_C",
                             "Pressure2_Bar", "Flow2_Lpm", "Temp2_C"])

        threading.Thread(target=pressure_thread, args=(ads, channels), daemon=True).start()
        threading.Thread(target=flow_thread, daemon=True).start()
        threading.Thread(target=rs485_temp_thread, daemon=True).start()

        with open(filename, 'a', newline='') as f:
            writer = csv.writer(f)
            try:
                while True:
                    with data_lock:
                        p1, p2 = pressure_bars
                        f1, f2 = flow_rates
                        t2 = temperatures[0] - off_t2
                        t1 = temperatures[1] - off_t1
                    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    print(f"| P1: {p1:.3f} Bar | F1: {f1:.2f} Lpm | T1: {t1:.2f} °C | "
                          f"P2: {p2:.3f} Bar | F2: {f2:.2f} Lpm | T2: {t2:.2f} °C")
                    writer.writerow([ts, p1, f1, t1, p2, f2, t2])
                    f.flush()
                    time.sleep(1)
            except KeyboardInterrupt:
                print("\nTerminated by User")
    except Exception as e:
        print(f"Main error: {e}")
        traceback.print_exc()
    finally:
        if i2c:
            try:
                i2c.deinit()
            except:
                pass

if __name__ == "__main__":
    main()


