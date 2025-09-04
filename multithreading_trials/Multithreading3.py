
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

def read_accelerometer(accel_timestamp):
    """Reads accelerometer data and processes it."""
    assign_core(1, inspect.currentframe().f_code.co_name)  # Get function name dynamically
    prev_timestamp = time.time()  # Store the previous timestamp
    
    while True:
        voltage = accel_channel.voltage
        acceleration = (voltage - 1.590) / 0.55  # Calibration adjustment
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        
        # Get current time in milliseconds and calculate the difference from the previous timestamp
        current_timestamp = time.time()
        time_diff_ms = (current_timestamp - prev_timestamp) * 1000  # Convert to milliseconds
        prev_timestamp = current_timestamp  # Update previous timestamp
        
        # Update the shared timestamp for accelerometer readings
        accel_timestamp.value = current_timestamp
        
        print(f"[{timestamp}] Accelerometer: {acceleration:.4f} | Time Diff (Accel-Accel): {time_diff_ms:.2f} ms")
        time.sleep(0.01)  # Sleep for 10ms to maintain a consistent rate

def count_rpm(rpm_timestamp, accel_timestamp):
    """Counts RPM using the IR sensor."""
    assign_core(2, inspect.currentframe().f_code.co_name)  # Get function name dynamically
    global rpm_count
    prev_timestamp = time.time()  # Store the previous timestamp
    
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
        
        # Get current time in milliseconds and calculate the difference from the previous timestamp
        current_timestamp = time.time()
        time_diff_ms = (current_timestamp - prev_timestamp) * 1000  # Convert to milliseconds
        prev_timestamp = current_timestamp  # Update previous timestamp
        
        # Update the shared timestamp for RPM readings
        rpm_timestamp.value = current_timestamp
        
        # Calculate the time difference between the last accelerometer reading and the current RPM reading
        time_diff_between_readings = (current_timestamp - accel_timestamp.value) * 1000  # in milliseconds
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"[{timestamp}] RPM: {rpm:.2f}    | Time Diff (RPM-RPM): {time_diff_ms:.2f} ms | Time Diff (Accel-RPM): {time_diff_between_readings:.2f} ms")
        
        rpm_count = 0  # Reset after calculation

if __name__ == "__main__":
    # Shared timestamp variables for accel and rpm
    accel_timestamp = multiprocessing.Value('d', time.time())  # Timestamp for accelerometer
    rpm_timestamp = multiprocessing.Value('d', time.time())    # Timestamp for RPM

    # Create processes for accelerometer and RPM readings
    accel_process = multiprocessing.Process(target=read_accelerometer, args=(accel_timestamp,))
    rpm_process = multiprocessing.Process(target=count_rpm, args=(rpm_timestamp, accel_timestamp))

    accel_process.start()
    rpm_process.start()

    accel_process.join()
    rpm_process.join()

