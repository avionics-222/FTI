import multiprocessing
import time
import os
import inspect
from datetime import datetime
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
from gpiozero import Button

SCL_PIN = 3  # GPIO 3 (Pin 5)
SDA_PIN = 2  # GPIO 2 (Pin 3)

# ADC Setup for GY-61 (ADS1115)
i2c = busio.I2C(SCL_PIN, SDA_PIN)
ads = ADS.ADS1115(i2c)
accel_channel = AnalogIn(ads, ADS.P0)  # X-axis on A0

# Global Variables (Note: These are per-process in multiprocessing)
rpm_count = 0

def assign_core(core_id, func_name):
    """Assigns the process to a specific CPU core and prints function name."""
    os.sched_setaffinity(0, {core_id})
    print(f"Process {os.getpid()} assigned to core {core_id} for function '{func_name}'")

def read_accelerometer():
    """Reads accelerometer data and processes it."""
    assign_core(1, inspect.currentframe().f_code.co_name)  # Get function name dynamically
    while True:
        voltage = accel_channel.voltage
        acceleration = (voltage - 1.590) / 0.55  # Calibration adjustment
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"[{timestamp}] Accelerometer: {acceleration:.4f}")
        time.sleep(1)

def count_rpm():
    """Counts RPM using the IR sensor."""
    assign_core(2, inspect.currentframe().f_code.co_name)  # Get function name dynamically
    global rpm_count

    # Initialize the Button inside the child process
    rpm_sensor = Button(17, pull_up=False)  # Adjust pull_up based on sensor

    def rpm_pulse():
        global rpm_count
        rpm_count += 1

    rpm_sensor.when_pressed = rpm_pulse  # Attach callback

    while True:
        start_time = time.time()
        time.sleep(1)  # Measure over 1 second
        elapsed_time = time.time() - start_time
        rpm = (rpm_count / elapsed_time) * 60  # Calculate RPM
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"[{timestamp}] RPM: {rpm:.2f}")
        rpm_count = 0  # Reset after calculation

if __name__ == "__main__":
    accel_process = multiprocessing.Process(target=read_accelerometer)
    rpm_process = multiprocessing.Process(target=count_rpm)

    accel_process.start()
    rpm_process.start()

    accel_process.join()
    rpm_process.join()

