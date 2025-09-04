from multiprocessing import Process, Queue
from hx711_module import HX711
import time
import os

# Define pins for each HX711 (8 load cells)
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

# Load cell labels
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

# Manually measured offsets (from taring)
hardcoded_offsets = [
    78000, # Loadcell 0
    0,    # Loadcell 1
    20000,    # Loadcell 2
    20000,    # Loadcell 3
    0,    # Loadcell 4
    0,    # Loadcell 5
    0,    # Loadcell 6
    0     # Loadcell 7
]

# 📐 Manually calculated calibration factors (based on known weights)
calibration_factors = [
    40.04,   # Loadcell 0
    142.8,   # Loadcell 1
    144.1,   # Loadcell 2
    143.7,   # Loadcell 3
    145.2,   # Loadcell 4
    144.5,   # Loadcell 5
    146.0,   # Loadcell 6
    143.9    # Loadcell 7
]

# Radius for torque calculation in meters
R = 0.1  # Replace with your actual radius value

def loadcell_worker(dout, sck, q, index, offset, calibration_factor):
    hx = HX711(dout, sck)
    hx.offset = offset
    hx.calibration_factor = calibration_factor
    while True:
        weight, raw = hx.get_weight()
        q.put((index, weight, raw))
        time.sleep(0.2)

def set_process_affinity(process_id, core_id):
    """Set which CPU core a process runs on"""
    try:
        os.sched_setaffinity(process_id, {core_id})
        print(f"Process {process_id} assigned to core {core_id}")
    except Exception as e:
        print(f"Error setting affinity: {e}")

def main():
    processes = []
    queues = []
    
    # Number of cores on Raspberry Pi 5
    num_cores = 4
    
    for i, config in enumerate(load_cells_config):
        q = Queue()
        queues.append(q)
        offset = hardcoded_offsets[i]
        cal_factor = calibration_factors[i]
        p = Process(target=loadcell_worker, args=(config['dout'], config['sck'], q, i, offset, cal_factor))
        p.start()
        
        # Assign each process to a specific core (round-robin)
        # This will distribute the 8 processes across the 4 cores
        core_id = i % num_cores
        set_process_affinity(p.pid, core_id)
        
        processes.append(p)

    try:
        while True:
            readings = {}
            for i, q in enumerate(queues):
                while not q.empty():
                    idx, weight, raw = q.get()
                    readings[idx] = (weight, raw)
            
            if readings:
                print("\n===== LOADCELL READINGS =====")
                for i in range(len(load_cells_config)):
                    if i in readings:
                        weight, raw = readings[i]
                        label = load_cell_labels[i]
                        weight_kg = weight / 1000.0  # Convert from g to kg
                        
                        if i < 4:  # Thrust loadcells (0-3)
                            thrust = weight_kg * 9.8  # Thrust in Newtons
                            print(f"[{label}] Weight: {weight:.2f} g, Thrust: {thrust:.2f} N")
                        else:      # Torque loadcells (4-7)
                            torque = weight_kg * 9.8 * R  # Torque in Nm
                            print(f"[{label}] Weight: {weight:.2f} g, Torque: {torque:.2f} Nm")
                
                # Calculate and display the total thrust and torque
                total_thrust = sum([(readings[i][0] / 1000.0) * 9.8 for i in range(4) if i in readings])
                total_torque = sum([(readings[i][0] / 1000.0) * 9.8 * R for i in range(4, 8) if i in readings])
                print(f"\nTotal Thrust: {total_thrust:.2f} N")
                print(f"Total Torque: {total_torque:.2f} Nm")
                
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("Exiting...")
        for p in processes:
            p.terminate()
            p.join()

if __name__ == "__main__":
    main()
