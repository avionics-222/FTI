from gpiozero import InputDevice
from pymodbus.client import ModbusSerialClient
from time import sleep
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import client

IR_PIN = 17  # GPIO17 (pin 11 on the Raspberry Pi)
token = 'nEl4RchZAyQAOaPD_oIKFOyYdjOmAmxv74YGh91otKnjmELUOSPZy7m_EjjrSxdZVuIknYpeDeKUE3H_r8uT4w=='
influxdb_url = "https://us-east-1-1.aws.cloud2.influxdata.com"  # Replace with your InfluxDB URL
org = "IdeaForge"  # Replace with your organization name

bucket = "AllSensor_Data"  # Replace with your bucket name
#temp_bucket = "_monitoring"
#vibration_bucket = "_tasks"

# Create an InfluxDB client
client = InfluxDBClient(url=influxdb_url, token=token)

# Create a Write API instance
write_api = client.write_api()

# Create an InputDevice object for the IR sensor
ir_sensor = InputDevice(IR_PIN)

modbus_client = ModbusSerialClient(
    port="/dev/ttyACM0",
    baudrate=4800,
    timeout=1,
    stopbits=1,
    bytesize=8,
    parity="N",
)

print("Starting IR sensor test...")

if modbus_client.connect():
    print("Connected to Modbus sensor")

def read_ir():
    
    # Check the state of the IR sensor
    if ir_sensor.is_active: 
        print("No obstacle detected")
        status = 1
        
    else:
        print("Obstacle detected")
        status = 0
        
    #Time stamp for sensors
    time1 = datetime.now()
    sensor_time = time1.strftime("%H:%M:%S.%f")[:-3]
    print("Sensor_TimeStamp",sensor_time)
    
    point = (
            Point("IR_SensorData")
            .tag("Sensor", "IR_Sensor")
            .field("State", status*5)
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


try:
    while True:
        read_ir()
        read_vib()
        sleep(0.01)
    
except KeyboardInterrupt:
    print("\nProgram stopped by user.")

finally:
    # Close the InfluxDB client
    modbus_client.close()
    client.close()
