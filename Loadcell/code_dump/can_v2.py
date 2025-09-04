import can
import time
import os

# CAN Configuration
CAN_INTERFACE = "can0"
BITRATE = 250000
BAMOCAR_ID = 0x201
BAMOCAR_RESPONSE_ID = 0x181

# ESC Parameter Registers
RPM_REGISTER = 0x30
VOLTAGE_REGISTER = 0xEB
TORQUE_REGISTER = 0xA0
KT_REGISTER = 0x87
CURRENT_REGISTER = 0x20
TEMP_REGISTER = 0x49


# Conversion factors
RPM_CONVERSION = 0.091547146780592
VOLTAGE_CONVERSION = 1
CURRENT_CONVERSION = 1
TEMP_CONVERSION = 1

def is_can_interface_up():
    result = os.popen(f"ip link show {CAN_INTERFACE}").read()
    return "<UP," in result

def setup_can_interface():
    if is_can_interface_up():
        print(f"CAN interface {CAN_INTERFACE} is already up.")
    else:
        print(f"Bringing up CAN interface {CAN_INTERFACE} at {BITRATE} bps...")
        os.system(f"sudo ip link set {CAN_INTERFACE} up type can bitrate {BITRATE}")

def query_can_parameter(bus, register, conversion):
    msg = can.Message(
        arbitration_id=BAMOCAR_ID,
        data=[0x3D, register, 0x64],
        is_extended_id=False
    )
    bus.send(msg)

    start_time = time.time()
    while time.time() - start_time < 1.0:
        message = bus.recv(timeout=0.1)
        if message and message.arbitration_id == BAMOCAR_RESPONSE_ID:
            data = message.data
            if data[0] == register:
                raw_value = (data[2] << 8) | data[1]
                return conversion * raw_value
    return None

def clear_can_bus_buffer(interface="socketcan", channel="can0", timeout=0.05):
    try:
        print("Clearing CAN bus buffer...")
        with can.interface.Bus(channel=channel, interface=interface) as bus:
            while True:
                msg = bus.recv(timeout=timeout)
                if msg is None:
                    break  # Buffer is empty
        print("CAN bus buffer cleared.")
    except Exception as e:
        print(f"Error while clearing CAN buffer: {e}")


def main():
    setup_can_interface()
    bus = can.interface.Bus(channel=CAN_INTERFACE, interface="socketcan")

    try:
        while True:
            rpm = query_can_parameter(bus, RPM_REGISTER, RPM_CONVERSION)
            voltage = query_can_parameter(bus, VOLTAGE_REGISTER, VOLTAGE_CONVERSION)
            current = query_can_parameter(bus, CURRENT_REGISTER, CURRENT_CONVERSION)
            temperature = query_can_parameter(bus, TEMP_REGISTER, TEMP_CONVERSION)

            print("\n===== ESC CONTROLLER DATA (CAN) =====")
            if rpm is not None:
                print(f"{'RPM':<15} | {rpm:>10.2f} rpm")
            if voltage is not None:
                print(f"{'Voltage':<15} | {voltage:>10.2f} V")
            if current is not None:
                print(f"{'Current':<15} | {current:>10.2f} A")
            if temperature is not None:
                print(f"{'Temperature':<15} | {temperature:>10.2f} °C")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nExiting...")
        bus.shutdown()

if __name__ == "__main__":
    main()
