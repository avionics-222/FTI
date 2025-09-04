import threading
import time
from datetime import datetime
import board
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
from gpiozero import Button


SCL_PIN = 3  # GPIO 3 (Pin 5)
SDA_PIN = 2  # GPIO 2 (Pin 3)

# ADC Setup for GY-61 (ADS1115)
#i2c = busio.I2C(board.SCL, board.SDA)
i2c = busio.I2C(SCL_PIN, SDA_PIN)

ads = ADS.ADS1115(i2c)
accel_channel = AnalogIn(ads, ADS.P0)  # Assuming X-axis on A0

# IR RPM Sensor Setup
rpm_sensor = Button(17)  # Change GPIO pin as per wiring

# Global Variables
rpm_count = 0

def read_accelerometer():
    """Reads GY-61 accelerometer value and prints timestamped output."""
    while True:
        voltage = accel_channel.voltage
        acceleration =(voltage-1.590)/0.55
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"[{timestamp}] Accelerometer: {acceleration:.4f}")
        time.sleep(1)  # Adjust sampling rate

def count_rpm():
    """Counts RPM pulses and prints timestamped output."""
    global rpm_count

    def rpm_pulse():
        global rpm_count
        rpm_count += 1

    rpm_sensor.when_pressed = rpm_pulse  # Increment count on pulse

    while True:
        start_time = time.time()
        time.sleep(1)  # 1-second interval for RPM measurement
        elapsed_time = time.time() - start_time
        rpm = (rpm_count / elapsed_time) * 60  # Convert to RPM
        rpm_count = 0  # Reset count after each second
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        print(f"[{timestamp}] RPM: {rpm:.2f}")
        
# Create Threads
accel_thread = threading.Thread(target=read_accelerometer, daemon=True)
rpm_thread = threading.Thread(target=count_rpm, daemon=True)

# Start Threads
accel_thread.start()
rpm_thread.start()

# Keep Main Thread Alive
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopping...")
    
    
