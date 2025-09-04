from multiprocessing import Process, Queue
import time
import os
import statistics
from gpiozero import DigitalInputDevice, DigitalOutputDevice
import sys

class HX711:
    def __init__(self, dout_pin, sck_pin, calibration_factor=25.6, offset=0, name="unnamed"):
        """Initialize HX711 using gpiozero"""
        self.name = name
        try:
            self.dout = DigitalInputDevice(dout_pin)
            self.sck = DigitalOutputDevice(sck_pin, initial_value=False)
        except Exception as e:
            print(f"Error initializing GPIO for cell {name}: {e}")
            sys.exit(1)

        self.calibration_factor = calibration_factor  
        self.offset = offset
        self.last_valid_reading = 0
        time.sleep(0.2)  # Reduced startup delay

    def read(self):
        """Optimized HX711 raw data reading with reduced delay"""
        timeout_start = time.time()
        while self.dout.value == 1:  # Wait for HX711 to be ready
            if time.time() - timeout_start > 0.1:  # 100ms timeout
                return None

        data = 0
        for _ in range(24):
            self.sck.on()
            self.sck.off()
            data = (data << 1) | self.dout.value  # Faster bit shifting

        self.sck.on()
        self.sck.off()  # 25th clock pulse

        if data & 0x800000:
            data -= 0x1000000  # Convert 2's complement

        return None if data == 0 or data == -1 else data

    def read_average(self, num_readings=3, max_retries=5):
        """Optimized function to take multiple readings and return median-filtered average"""
        readings = []
        retries = 0

        while len(readings) < num_readings and retries < max_retries:
            reading = self.read()
            if reading is not None:
                readings.append(reading)
            else:
                retries += 1
            time.sleep(0.005)  # Reduced sleep time

        if not readings:
            return self.last_valid_reading if self.last_valid_reading else 0

        return statistics.median(readings)  # Faster noise filtering

    def zero(self, num_readings=14):
        """Optimized tare function using median filtering"""
        print(f"Zeroing {self.name} scale...")
        self.offset = self.read_average(num_readings)
        print(f"{self.name} Zero offset: {self.offset}")
        return self.offset

    def get_weight(self, num_readings=3):
        """Get weight with optimized averaging"""
        reading = self.read_average(num_readings)
        raw_value = reading - self.offset
        weight = raw_value / self.calibration_factor
        return weight, raw_value

    def reset_hx711(self):
        """Reset the HX711 chip by toggling the clock line"""
        self.sck.on()
        time.sleep(0.06)
        self.sck.off()
        time.sleep(0.01)

    def cleanup(self):
        """Cleanup resources (gpiozero handles this automatically)"""
        pass


def set_process_affinity(process_id, core_id):
    """Set which CPU core a process runs on"""
    try:
        os.sched_setaffinity(process_id, {core_id})
        print(f"Process {process_id} assigned to core {core_id}")
    except Exception as e:
        print(f"Error setting affinity: {e}")


def loadcell_worker(dout, sck, q, index, offset, calibration_factor, name):
    """Worker function for each load cell process"""
    hx = HX711(dout_pin=dout, sck_pin=sck, calibration_factor=calibration_factor, offset=offset, name=name)
    
    # Only zero if no offset was provided
    if offset == 0:
        hx.zero()
    
    failure_count = 0
    
    while True:
        try:
            weight, raw = hx.get_weight(num_readings=3)
            q.put((index, weight, raw))
            failure_count = 0  # Reset on success
        except Exception as e:
            print(f"Error on {name}: {e}")
            failure_count += 1
            if failure_count >= 3:  # Reduced reset threshold
                print(f"Resetting {name} HX711...")
                hx.reset_hx711()
                failure_count = 0
        
        time.sleep(0.25)  # Faster updates


def main():
    """Main function to read from multiple load cells in parallel"""
    # Define configuration for 4 load cells using the pins from your other project
    load_cells_config = [
        {"name": "Cell-1", "dout": 5, "sck": 6, "offset": 0, "calibration_factor": 40.04},
        {"name": "Cell-2", "dout": 13, "sck": 19, "offset": 0, "calibration_factor": 40.04},
        {"name": "Cell-3", "dout": 17, "sck": 27, "offset": 0, "calibration_factor": 40.04},
        {"name": "Cell-4", "dout": 22, "sck": 23, "offset": 0, "calibration_factor": 40.04},
    ]
    
    # Number of cores on Raspberry Pi 5
    num_cores = 4
    
    processes = []
    queues = []
    
    # Create and start worker processes for each load cell
    for i, config in enumerate(load_cells_config):
        q = Queue()
        queues.append(q)
        
        p = Process(
            target=loadcell_worker, 
            args=(
                config['dout'], 
                config['sck'], 
                q, 
                i, 
                config['offset'], 
                config['calibration_factor'],
                config['name']
            )
        )
        p.start()
        
        # Assign each process to a specific core (one process per core in this case)
        core_id = i % num_cores
        set_process_affinity(p.pid, core_id)
        
        processes.append(p)
    
    print("All load cell processes started. Starting weight measurements...")
    print("Press Ctrl+C to exit")
    
    
    
if __name__ == "__main__":
    main()
