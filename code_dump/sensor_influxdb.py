from gpiozero import InputDevice
from pymodbus.client import ModbusSerialClient
from time import sleep
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

try:
    while True:
        #Vibration sensor
        response = modbus_client.read_holding_registers(0, count=4, slave=1)
        # Check the state of the IR sensor
        if ir_sensor.is_active: 
            print("No obstacle detected")
            status = 1
        
        else:
            print("Obstacle detected")
            status = 0
        
        point = (
                Point("IR_SensorData")
                .tag("Sensor", "IR_Sensor")
                .field("State", status*5)
            )
           # Write the data point to InfluxDB
        #write_api.write(bucket=IR_bucket, org=org, record=point)
            
        if response.isError():
            print("No Respone from vibration") 
        else:
            temperature = response.registers[0] / 10.0
            x_vib_velocity = response.registers[1] / 10.0
            y_vib_velocity = response.registers[2] / 10.0
            z_vib_velocity = response.registers[3] / 10.0

            
            print(f"Temperature: {temperature} *C")
            print(f"X axis Vibration Velocity: {x_vib_velocity} mm/s")
            print(f"Y axis Vibration Velocity: {y_vib_velocity} mm/s")
            print(f"Z axis Vibration Velocity: {z_vib_velocity} mm/s")
            
              # Create a data point to write to InfluxDB
       
            temp_point = (
                Point("Temp_SensorData")
                .tag("Sensor", "temperature_sensor")
                .field("temperature", temperature)
            )
            
            vibration_point = (
                Point("Vib_SensorData")
                .tag("Sensor", "vibration_sensor")
                .field("x_vib_velocity", x_vib_velocity)
                .field("y_vib_velocity", y_vib_velocity)
                .field("z_vib_velocity", z_vib_velocity)
            )
  
        # Write the data point to InfluxDB
            write_api.write(bucket=bucket, org=org, record=point)
            
            write_api.write(bucket=bucket, org=org, record=temp_point)
            write_api.write(bucket=bucket, org=org, record=vibration_point)
        
        # Write the data point to InfluxDB
            #write_api.write(bucket=vibration_bucket, org=org, record=vibration_point)
            
        sleep(0.3)  # Delay for half a second
            
except KeyboardInterrupt:
    print("\nProgram stopped by user.")

finally:
    # Close the InfluxDB client
    modbus_client.close()
    client.close()
