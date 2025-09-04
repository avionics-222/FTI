import os
import can
import time

# Configuration
CAN_INTERFACE = "can0"
BITRATE = 500000
BAMOCAR_ID = 0x201  # BAMOCAR Receive ID
BAMOCAR_RESPONSE_ID = 0x181  # BAMOCAR Response ID
RPM_REGISTER = 0x30  # Register for RPM
POLL_INTERVAL = 0.005  # 5 milliseconds (200Hz)

def is_can_interface_up():
    result = os.popen(f"ip link show {CAN_INTERFACE}").read()
    return "<UP," in result  

def setup_can_interface():
    if is_can_interface_up():
        print(f"CAN interface {CAN_INTERFACE} is already up.")
    else:
        print(f"Bringing up CAN interface {CAN_INTERFACE} at {BITRATE} bps...")
        os.system(f"sudo ip link set {CAN_INTERFACE} up type can bitrate {BITRATE}")

def request_rpm(bus):
    msg = can.Message(
        arbitration_id=BAMOCAR_ID,
        data=[0x3D, RPM_REGISTER, 0x00],  # Last byte could be timing or padding; set to 0x00 for immediate
        is_extended_id=False
    )
    bus.send(msg)

def read_rpm(bus):
    message = bus.recv(timeout=0.001)  # Short timeout
    if message and message.arbitration_id == BAMOCAR_RESPONSE_ID:
        data = message.data
        if data[0] == RPM_REGISTER and len(data) >= 3:
            raw_rpm = (data[2] << 8) | data[1]
            rpm = 0.091547146780592 * raw_rpm
            print(f"RPM: {rpm:.2f}")
            return rpm
    return None

if __name__ == "__main__":
    setup_can_interface()
    bus = can.interface.Bus(channel=CAN_INTERFACE, interface="socketcan")

    try:
        while True:
            start_time = time.time()
            
            request_rpm(bus)
            read_rpm(bus)

            # Wait remaining time to hit 200Hz
            elapsed = time.time() - start_time
            sleep_time = POLL_INTERVAL - elapsed
            if sleep_time > 0:
                time.sleep(sleep_time)
            else:
                print("⚠️ Loop overrun: consider optimizing timing or check bus latency.")

    except KeyboardInterrupt:
        print("\nExiting...")

    finally:
        bus.shutdown()
        print("CAN interface closed.")
