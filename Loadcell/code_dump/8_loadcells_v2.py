import time
import statistics
from gpiozero import DigitalInputDevice, DigitalOutputDevice
import sys

class HX711:
    def __init__(self, dout_pin=5, sck_pin=6):
        try:
            self.dout = DigitalInputDevice(dout_pin)
            self.sck = DigitalOutputDevice(sck_pin, initial_value=False)
        except Exception as e:
            print(f"Error initializing GPIO: {e}")
        
        self.offset_3v3_100 = 0.6625/6.6
        self.calibration_factor = 25.4
        self.offset = 0
        self.last_valid_reading = 0
        time.sleep(0.2)

    def read(self):
        timeout_start = time.time()
        while self.dout.value == 1:
            if time.time() - timeout_start > 0.01:
                return None

        data = 0
        for _ in range(24):
            self.sck.on()
            self.sck.off()
            data = (data << 1) | self.dout.value

        self.sck.on()
        self.sck.off()

        if data & 0x800000:
            data -= 1 << 24
        return None if data == 0 or data == -1 else data

    def read_average(self, num_readings=10):
        readings = []
        while len(readings) < num_readings:
            reading = self.read()
            if reading is not None:
                readings.append(reading)
            time.sleep(0.01)
        return statistics.median(readings)

    def get_weight(self, num_readings=10):
        reading = self.read_average(num_readings)
        raw_value = reading - self.offset
        out_volt = raw_value * 20 / 8388607
        weight = out_volt * 120 * 1000 * self.offset_3v3_100
        return weight, raw_value

    def reset_hx711(self):
        self.sck.on()
        time.sleep(0.06)
        self.sck.off()
        time.sleep(0.01)

    def cleanup(self):
        pass

# HX711 configuration for 8 load cells
load_cells_config = [
    {"dout": 5,  "sck": 6},
    {"dout": 14, "sck": 15},
    {"dout": 17, "sck": 27},
    {"dout": 22, "sck": 23},
    {"dout": 24, "sck": 25},
    {"dout": 12, "sck": 13},
    {"dout": 20, "sck": 21},
    {"dout": 26, "sck": 19} 
]

load_cell_labels = [
    "Thrust_0deg",
    "Thrust_90deg",
    "Thrust_180deg",
    "Thrust_270deg",
    "Torque_0deg",
    "Torque_90deg",
    "Torque_180deg",
    "Torque_270deg"
]

hardcoded_offsets = [
    82243,
    30683,
    213752,
    81425,
    109366,
    38272,
    -216303,
    84388
]

calibration_factors = [25.4] * 8

R = 0.1  # Torque arm radius in meters

def main():
    hx_objects = []

    # Initialize each HX711
    for i, config in enumerate(load_cells_config):
        hx = HX711(config['dout'], config['sck'])
        hx.offset = hardcoded_offsets[i]
        hx.calibration_factor = calibration_factors[i]
        hx_objects.append(hx)

    try:
        while True:
            readings = []

            # Read all load cells
            for hx in hx_objects:
                weight, raw = hx.get_weight(num_readings=5)
                readings.append((weight, raw))

            print("\n===== LOADCELL READINGS =====")
            for i in range(len(readings)):
                weight, raw = readings[i]
                label = load_cell_labels[i]
                weight_kg = weight / 1000.0

                if i < 4:
                    thrust = weight_kg * 9.8
                    print(f"[{label}] Weight: {weight:.2f} g, Thrust: {thrust:.2f} N")
                else:
                    torque = weight_kg * 9.8 * R
                    print(f"[{label}] Weight: {weight:.2f} g, Torque: {torque:.2f} Nm")

            total_thrust = sum([(readings[i][0] / 1000.0) * 9.8 for i in range(4)])
            total_torque = sum([(readings[i][0] / 1000.0) * 9.8 * R for i in range(4, 8)])
            print(f"\nTotal Thrust: {total_thrust:.2f} N")
            print(f"Total Torque: {total_torque:.2f} Nm")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("\nExiting program")
    finally:
        for hx in hx_objects:
            hx.cleanup()

if __name__ == "__main__":
    main()
