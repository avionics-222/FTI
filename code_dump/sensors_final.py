import gpiozero
from gpiozero import InputDevice
from pymodbus.client import ModbusSerialClient
import time
from time import sleep
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import client

#ADC for strain gauge libs
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn


token = 'nEl4RchZAyQAOaPD_oIKFOyYdjOmAmxv74YGh91otKnjmELUOSPZy7m_EjjrSxdZVuIknYpeDeKUE3H_r8uT4w=='
influxdb_url = "https://us-east-1-1.aws.cloud2.influxdata.com"  # Replace with your InfluxDB URL
org = "IdeaForge"  # Replace with your organization name

bucket = "Sensor_Data"  # Replace with your bucket name
#temp_bucket = "_monitoring"
#vibration_bucket = "_tasks"

# Create an InfluxDB client
client = InfluxDBClient(url=influxdb_url, token=token)

# Create a Write API instance
write_api = client.write_api()

# Create an InputDevice object for the IR sensor
ir_pin = 17  # Adjust to your GPIO pin

modbus_client = ModbusSerialClient(
    port="/dev/ttyACM0",
    baudrate=4800,
    timeout=1,
    stopbits=1,
    bytesize=8,
    parity="N",
)

if modbus_client.connect():
    print("Connected to Modbus sensor")

# IR RPM FUCNTION

def monitor_rpm(ir_pin, markers_per_revolution=2, sample_time=1.0):

    ir_sensor = gpiozero.InputDevice(ir_pin)
    
    start_time = time.time()
    marker_count = 0
    last_state = False
    last_change_time = time.time()
    debounce_delay = 0.01  # 10ms debounce
    
    # Main sampling loop
    while time.time() - start_time < sample_time:
        current_time = time.time()
        current_state = ir_sensor.is_active
        
        # Only count transitions from non-detected to detected (rising edge)
        # and ensure enough time has passed since last detection (debounce)
        if (not last_state and current_state and 
            current_time - last_change_time > debounce_delay):
            marker_count += 1
            last_change_time = current_time
        
        last_state = current_state
        time.sleep(0.001)  # Small delay to prevent CPU overload
    
    # Calculate RPM
    elapsed_time = time.time() - start_time
    rpm = (marker_count / markers_per_revolution) * (60 / elapsed_time)
    
    return rpm
    
    
def read_rpm():
    
    
    current_rpm = monitor_rpm(ir_pin)
    # Only show RPM if it's above a meaningful threshold
    if current_rpm > 1:  # Adjust threshold as needed
        print(f"Current RPM: {current_rpm:.1f}")
    else:
        print("No rotation detected")
    
    #Time stamp for sensors
    time1 = datetime.now()
    sensor_time = time1.strftime("%H:%M:%S.%f")[:-3]
    print("Sensor_TimeStamp",sensor_time)
    
    point = (
            Point("RPM_SensorData")
            .tag("Sensor", "RPM_Sensor")
            .field("RPM", current_rpm)
        )   
    point1 = (
            Point("Sensor_TimeStamp")
            .tag("Sensor", "Timestamp")
            .field("Timestamp", sensor_time)
        )   
    
    write_api.write(bucket=bucket, org=org, record=point)
    write_api.write(bucket=bucket, org=org, record=point1)
         
         
x_vib_f=0.0
y_vib_f=0.0
z_vib_f=0.0
           
           
def read_vib():
    
    #Vibration sensor
    response = modbus_client.read_holding_registers(0, count=4, slave=1)
    response_disp = modbus_client.read_holding_registers(4, count=3, slave=1)
    
    if response.isError():
        print("No Respone from vibration") 
    else:
        temperature = response.registers[0] / 10.0
        x_vib_velocity = response.registers[1] / 10.0
        y_vib_velocity = response.registers[2] / 10.0
        z_vib_velocity = response.registers[3] / 10.0
        
        x_disp = response_disp.registers[0] / 10.0
        y_disp = response_disp.registers[1] / 10.0
        z_disp = response_disp.registers[2] / 10.0
        
        x_vib_f = x_vib_velocity / (2*3.1416*(x_disp+0.0001)) 
        y_vib_f = y_vib_velocity / (2*3.1416*(y_disp+0.0001)) 
        z_vib_f = z_vib_velocity / (2*3.1416*(z_disp+0.0001))

            
        print(f"Temperature: {temperature} *C")
        print(f"X axis Vibration Velocity: {x_vib_velocity} mm/s")
        print(f"Y axis Vibration Velocity: {y_vib_velocity} mm/s")
        print(f"Z axis Vibration Velocity: {z_vib_velocity} mm/s")
        
        print(f"X axis Vibration Disp: {x_disp} mm")
        print(f"Y axis Vibration Disp: {y_disp} mm")
        print(f"Z axis Vibration Disp: {z_disp} mm")
        
        print(f"X axis Vibration Freq: {x_vib_f} Hz")
        print(f"Y axis Vibration Freq: {y_vib_f} Hz")
        print(f"Z axis Vibration Freq: {z_vib_f} Hz")
            
        # Create a data point to write to InfluxDB
       
        temp_point = (
            Point("Temp_SensorData")
            .tag("Sensor", "temperature_sensor")
            .field("temperature", temperature)
        )
            
        vibration_point = (
            Point("Vib_SensorData")
            .tag("Sensor", "vibration_sensor")
            .field("x_vib_velocity", x_vib_f)
            .field("y_vib_velocity", y_vib_f)
            .field("z_vib_velocity", z_vib_f)
        )
  
        write_api.write(bucket=bucket, org=org, record=temp_point)
        write_api.write(bucket=bucket, org=org, record=vibration_point)


def read_strain():
    
    SCL_PIN = 3  # GPIO 3 (Pin 5)
    SDA_PIN = 2  # GPIO 2 (Pin 3)

    # Initialize I2C
    i2c = busio.I2C(SCL_PIN, SDA_PIN)

    # Initialize ADS1115
    ads = ADS.ADS1115(i2c)

    # Create an analog input channel on A0
    channel = AnalogIn(ads, ADS.P0)

    GF = 2.0           # Gauge Factor provided by the manufacturer


    
    voltage = channel.voltage  # Get voltage reading
    strain = ((voltage / 5.0) / GF )* 10**5
            

    #print(f"Voltage: {voltage:.4f} V")
    print(f"Strain Value: {strain:.3f} µε")
    time.sleep(0.1)
    
    strain_point = (
            Point("Strain_SensorData")
            .tag("Sensor", "strain_sensor")
            .field("Strain", strain)
        )
    
    write_api.write(bucket=bucket, org=org, record=strain_point)
    
 
 
#//////////////////////////////////////////////////////////////////////// 
 
# Call All sensor functions    
    
try:
    while True:
        
        read_vib()
        read_strain()
        read_rpm()
        sleep(0.1)
    
except KeyboardInterrupt:
    print("\nProgram stopped by user.")

finally:
    # Close the InfluxDB client
    modbus_client.close()
    client.close()
