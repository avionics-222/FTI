import os
import can
import time

# Configuration
CAN_INTERFACE = "can0"
BITRATE = 500000
BAMOCAR_ID = 0x201  # BAMOCAR Receive ID
BAMOCAR_RESPONSE_ID = 0x181  # BAMOCAR Response ID
IGBT_TEMP_REGISTER = 0x20  # Correct register for IGBT temperature

def is_can_interface_up():
    """Check if the CAN interface is already up."""
    result = os.popen(f"ip link show {CAN_INTERFACE}").read()
    return "<UP," in result  # If 'UP' is in output, interface is already active

def setup_can_interface():
    """Set up the CAN interface if not already up."""
    if is_can_interface_up():
        print(f"CAN interface {CAN_INTERFACE} is already up.")
    else:
        print(f"Bringing up CAN interface {CAN_INTERFACE} at {BITRATE} bps...")
        os.system(f"sudo ip link set {CAN_INTERFACE} up type can bitrate {BITRATE}")

def request_igbt_temp(bus):
    """Send a CAN request to read the IGBT temperature register (0x4A)."""
    msg = can.Message(arbitration_id=BAMOCAR_ID, data=[0x3D, IGBT_TEMP_REGISTER, 0x64], is_extended_id=False)
    bus.send(msg)
    print(f"Sent RPM request to BAMOCAR (ID: 0x{BAMOCAR_ID:X})")

def read_igbt_temp(bus):
    """Read IGBT temperature response from BAMOCAR ESC."""
    while True:
        message = bus.recv(timeout=1)  # Wait for response
        if message and message.arbitration_id == BAMOCAR_RESPONSE_ID:
            data = message.data
            if data[0] == IGBT_TEMP_REGISTER:  # Ensure it's the correct response
                temp = (data[2] << 8) | data[1]  # Convert bytes to integer temperature value
                temp1= (-3.632149e+03) + (6.056921e-01 * temp) - (3.801182e-05 * temp**2) + (1.069198e-09 * temp**3) - (1.120435e-14 * temp**4)
                print(f"igbt temp from ESC: {temp1} and raw igbt temp from esc {temp}")
                
                return temp
        

if __name__ == "__main__":
    setup_can_interface()
    bus = can.interface.Bus(channel=CAN_INTERFACE, interface="socketcan")

    try:
        request_igbt_temp(bus)
        while True:
            
            read_igbt_temp(bus)
           
    except KeyboardInterrupt:
        print("\nExiting...")
    finally:
        bus.shutdown()
        print("CAN interface closed.")
