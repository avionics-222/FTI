import os
import can
import time

# Configuration
CAN_INTERFACE = "can0"
BITRATE = 500000
BAMOCAR_ID = 0x201  # BAMOCAR Receive ID
BAMOCAR_RESPONSE_ID = 0x181  # BAMOCAR Response ID
RPM_REGISTER = 0x30  # Correct register for RPM

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


def request_rpm(bus):
    """Send a CAN request to read the RPM register (0x30)."""

    msg = can.Message(arbitration_id=BAMOCAR_ID, data=[0x3D, RPM_REGISTER, 0x64], is_extended_id=False)  # 0x64 for 100ms delay (esc specific)
    bus.send(msg)
    print(f"Sent RPM request to BAMOCAR (ID: 0x{BAMOCAR_ID:X})")


def read_rpm(bus):
    """Read RPM response from BAMOCAR ESC."""
    
    while True:
        message = bus.recv(timeout=1) 
        if message and message.arbitration_id == BAMOCAR_RESPONSE_ID:
            data = message.data
            if data[0] == RPM_REGISTER: 
                temp = (data[2] << 8) | data[1]  # Convert bytes to integer temperature value
                temp1= 0.091547146780592* temp   # Conversion of raw rpm to rpm
                print(f"RPM : {temp1} rpm")
                
                return temp
        

if __name__ == "__main__":
    setup_can_interface()
    bus = can.interface.Bus(channel=CAN_INTERFACE, interface="socketcan")

    try:
        request_rpm(bus)
        while True:
            
            read_rpm(bus)
           
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        bus.shutdown()
        print("CAN interface closed.")
