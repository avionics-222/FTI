
import can

# Set up the CAN interface (adjust based on your setup)
can_interface = "can0"  # Change if your interface is different

bus = can.interface.Bus(channel=can_interface, interface='socketcan')

print("Listening for CAN messages with ID 0x6A5...")

try:
    while True:
        message = bus.recv()  # Receive CAN message
        if message :
            print(f"Received data: {message.data.hex()}")  # Print data in hex format
except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    bus.shutdown()
