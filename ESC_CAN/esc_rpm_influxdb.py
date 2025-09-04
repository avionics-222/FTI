
import os
import can
import time
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

rpm_value=0

# Configuration
CAN_INTERFACE = "can0"
BITRATE = 500000
BAMOCAR_ID = 0x201  # BAMOCAR Receive ID
BAMOCAR_RESPONSE_ID = 0x181  # BAMOCAR Response ID
RPM_REGISTER = 0x30  # Correct register for RPM

# InfluxDB Credentials
token = 'nEl4RchZAyQAOaPD_oIKFOyYdjOmAmxv74YGh91otKnjmELUOSPZy7m_EjjrSxdZVuIknYpeDeKUE3H_r8uT4w=='
influxdb_url = "https://us-east-1-1.aws.cloud2.influxdata.com"  # Replace with your InfluxDB URL
org = "IdeaForge"  # Replace with your organization name
bucket = "ESC_CAN_Data"  # Replace with your bucket name

# Create an InfluxDB client
client = InfluxDBClient(url=influxdb_url, token=token)
write_api = client.write_api()

def is_can_interface_up():
    """Check if the CAN interface is already up."""
    result = os.popen(f"ip link show {CAN_INTERFACE}").read()
    return "<UP," in result  

def setup_can_interface():
    """Set up the CAN interface if not already up."""
    if is_can_interface_up():
        print(f"CAN interface {CAN_INTERFACE} is already up.")
    else:
        print(f"Bringing up CAN interface {CAN_INTERFACE} at {BITRATE} bps...")
        os.system(f"sudo ip link set {CAN_INTERFACE} up type can bitrate {BITRATE}")

def request_rpm():
    """Send a CAN request to read the RPM register (0x30)."""
    with can.Bus(interface='socketcan', channel=CAN_INTERFACE) as bus:
        msg = can.Message(arbitration_id=BAMOCAR_ID, data=[0x3D, RPM_REGISTER,0x64], is_extended_id=False)
        bus.send(msg)
        print(f"Sent RPM request to BAMOCAR (ID: 0x{BAMOCAR_ID:X})")

def read_rpm():
    global rpm_value
    """Read RPM response from BAMOCAR ESC and return RPM value."""
    with can.Bus(interface='socketcan', channel=CAN_INTERFACE) as bus:
        message = bus.recv(timeout=1) 
        #print(message)
        if message and message.arbitration_id == BAMOCAR_RESPONSE_ID:
            data = message.data
            #print(data)
            if data[0] == RPM_REGISTER: 
                temp = (data[2] << 8) | data[1]  # Convert bytes to integer temperature value
                temp1 = 0.091547146780592 * temp  # Conversion of raw rpm to rpm
                print(f"RPM : {temp1} rpm")
                rpm_value = temp1  # Return RPM value
        time.s
    leep(1)
    #return None  # Return None if no valid data

if __name__ == "__main__":
    setup_can_interface()
    #bus = can.interface.Bus(channel=CAN_INTERFACE, interface="socketcan")

    try:
        request_rpm()  # Send request first
        while True:
            
            read_rpm() 
            print(rpm_value) # Get the RPM value
            
            if rpm_value is not None:  # Ensure we only send valid data
                rpm_point = (
                    Point("RPM_Data")
                    .tag("Sensor", "RPM_Data")
                    .field("RPM", int(rpm_value))
                )

                write_api.write(bucket=bucket, org=org, record=rpm_point)
            rpm_value= None

            #time.sleep(0.1)  # Add a small delay to prevent flooding

    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        print("CAN interface closed.")
