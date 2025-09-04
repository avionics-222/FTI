import time
import statistics
import serial
from gpiozero import DigitalInputDevice, DigitalOutputDevice
from pymavlink.dialects.v20 import common as mavlink2
from pymavlink import mavutil
import sys

class HX711:
    def __init__(self, dout_pin=5, sck_pin=6):
        try:
            self.dout = DigitalInputDevice(dout_pin)
            self.sck = DigitalOutputDevice(sck_pin, initial_value=False)
        except Exception as e:
            print(f"Error initializing GPIO: {e}")
            sys.exit(1)

        self.calibration_factor = 40.04
        self.offset = 0
        self.last_valid_reading = 0
        time.sleep(0.2)

    def read(self):
        timeout_start = time.time()
        while self.dout.value == 1:
            if time.time() - timeout_start > 0.1:
                return None

        data = 0
        for _ in range(24):
            self.sck.on()
            self.sck.off()
            data = (data << 1) | self.dout.value

        self.sck.on()
        self.sck.off()

        if data & 0x800000:
            data -= 0x1000000

        return None if data == 0 or data == -1 else data

    def read_average(self, num_readings=3, max_retries=5):
        readings = []
        retries = 0

        while len(readings) < num_readings and retries < max_retries:
            reading = self.read()
            if reading is not None:
                readings.append(reading)
            else:
                retries += 1
            time.sleep(0.005)

        if not readings:
            return self.last_valid_reading if self.last_valid_reading else 0

        return statistics.median(readings)

    def zero(self, num_readings=14):
        print("Zeroing scale...")
        self.offset = self.read_average(num_readings)
        print(f"Zero offset: {self.offset}")
        return self.offset

    def get_weight(self, num_readings=3):
        reading = self.read_average(num_readings)
        raw_value = reading - self.offset
        weight = raw_value / self.calibration_factor
        #
        w1=122.01
        return weight, raw_value

    def reset_hx711(self):
        self.sck.on()
        time.sleep(0.06)
        self.sck.off()
        time.sleep(0.01)

    def cleanup(self):
        pass

def main():
    hx = None
    try:
        # Setup MAVLink serial connection (to SiK radio)
        mav_serial = mavutil.mavlink_connection('/dev/ttyUSB0', baud=57600)
        
        start_time = time.time()        ###
        
        hx = HX711(dout_pin=5, sck_pin=6)
        print(f"Using fixed calibration factor: {hx.calibration_factor}")

        hx.zero()
        print("Scale ready! Starting weight measurements...")

        failure_count = 0

        while True:
            try:
                weight, raw_value = hx.get_weight(num_readings=3)

                # Send MAVLink heartbeat (needed to maintain link in Mission Planner)
                mav_serial.mav.heartbeat_send(
                    mavlink2.MAV_TYPE_GENERIC,
                    mavlink2.MAV_AUTOPILOT_GENERIC,
                    0, 0, 0
                )

                # Send the loadcell weight using a named value
                mav_serial.mav.named_value_float_send(
                    int(time.time() * 1000),   # timestamp in ms
                    b"LoadCell",               # label (<= 10 bytes)
                    float(weight)              # actual weight value
                )

                print(f"Weight: {weight:.2f} g")
                failure_count = 0

            except Exception as e:
                print(f"Error: {e}")
                failure_count += 1
                if failure_count >= 3:
                    print("Resetting HX711...")
                    hx.reset_hx711()
                    failure_count = 0

            time.sleep(0.25)

    except KeyboardInterrupt:
        print("\nExiting program")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if hx:
            hx.cleanup()

if __name__ == "__main__":
    main()

