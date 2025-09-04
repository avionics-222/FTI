import time
import datetime
import multiprocessing
from smbus2 import SMBus
from gpiozero import DigitalInputDevice

# Constants for ADS1115 (I2C Address)
ADS1115_ADDR = 0x48
CONFIG_REG = 0x01
CONVERSION_REG = 0x00

# IR Sensor (Digital)
ir_sensor = DigitalInputDevice(17)  # Change GPIO pin as needed

# Initialize I2C Bus
bus = SMBus(1)

# Function to configure ADS1115 for single-ended mode (fastest mode)
def configure_ads1115():
    config = 0x8483  # 4.096V range, single-shot mode, fastest conversion
    bus.write_i2c_block_data(ADS1115_ADDR, CONFIG_REG, [(config >> 8) & 0xFF, config & 0xFF])

# Function to read ADC value
def read_adc(channel):
    assert 0 <= channel <= 3, "ADS1115 has 4 channels (0-3)"
    config = 0xC183 | (channel << 12)  # Adjust MUX bits for channel selection
    bus.write_i2c_block_data(ADS1115_ADDR, CONFIG_REG, [(config >> 8) & 0xFF, config & 0xFF])
    time.sleep(0.001)  # Allow conversion (~860 SPS max)
    data = bus.read_i2c_block_data(ADS1115_ADDR, CONVERSION_REG, 2)
    raw_value = (data[0] << 8) | data[1]
    voltage = (raw_value * 4.096) / 32768  # Convert to voltage
    return round(voltage, 2)  # Format to 2 decimal places

# Process 1: Print timestamp continuously
def timestamp_process(shared_adc1, shared_digital):
    while True:
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
        adc1 = shared_adc1.value if shared_adc1.value is not None else '-'
        digital = shared_digital.value
        print(f"[{timestamp}] : {adc1:>6} : {digital}")
        time.sleep(0.00001)  # 10 microseconds

# Process 2: Read ADC sensor (DS18B20 via ADS1115)
def adc_process(shared_adc1):
    configure_ads1115()
    while True:
        shared_adc1.value = read_adc(0)  # Read from ADS1115 Channel 0

# Process 3: Read Digital IR sensor
def digital_process(shared_digital):
    while True:
        shared_digital.value = ir_sensor.value  # Read IR sensor

if __name__ == "__main__":
    # Shared variables for inter-process communication
    manager = multiprocessing.Manager()
    shared_adc1 = manager.Value('d', None)  # ADC1 shared value
    shared_digital = manager.Value('i', 0)  # Digital sensor shared value

    # Create three processes
    p1 = multiprocessing.Process(target=timestamp_process, args=(shared_adc1, shared_digital))
    p2 = multiprocessing.Process(target=adc_process, args=(shared_adc1,))
    p3 = multiprocessing.Process(target=digital_process, args=(shared_digital,))

    # Start all processes
    p1.start()
    p2.start()
    p3.start()

    # Keep running until manually stopped
    p1.join()
    p2.join()
    p3.join()
