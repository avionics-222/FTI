# main_multiprocess.py
from multiprocessing import Process, Queue
from hx711_module import HX711
import time

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

# Manually measured offsets (from taring)

hardcoded_offsets = [
    82243,  # Loadcell 0
    30683,    # Loadcell 1
    213752,    # Loadcell 2
    81425,    # Loadcell 3
    109366,    # Loadcell 4
    38272,    # Loadcell 5
    -216303,    # Loadcell 6
    84388     # Loadcell 7           N0T VERIFIED
]

# Manually calculated calibration factors (based on known weights)
calibration_factors = [
    25.4,   # Loadcell 0
    25.4,   # Loadcell 1
    25.4,   # Loadcell 2
    25.4,   # Loadcell 3
    25.4,   # Loadcell 4
    25.4,   # Loadcell 5
    25.4,   # Loadcell 6
    25.4    # Loadcell 7
]

# Custom labels for each load cell
load_cell_labels = [
    "Thrust_0Deg",
    "Thrust_90Deg",
    "Thrust_180Deg",
    "Thrust_270Deg",
    "Torque_0Deg",
    "Torque_90Deg",
    "Torque_180Deg",
    "Torque_270Deg"
]

def loadcell_worker(dout, sck, q, index, offset, calibration_factor):
    hx = HX711(dout, sck)
    hx.offset = offset
    hx.calibration_factor = calibration_factor
    while True:
        weight, raw = hx.get_weight()
        q.put((index, weight, raw))
        time.sleep(0.2)

def main():
    processes = []
    queues = []
    R=0.093  #in meters

    for i, config in enumerate(load_cells_config):
        q = Queue()
        queues.append(q)
        offset = hardcoded_offsets[i]
        cal_factor = calibration_factors[i]
        p = Process(target=loadcell_worker, args=(config['dout'], config['sck'], q, i, offset, cal_factor))
        p.start()
        processes.append(p)

    try:
        while True:
            for i, q in enumerate(queues):
                if not q.empty():
                    idx, weight, raw = q.get()
                    label = load_cell_labels[idx]
                    weight_kg = weight / 1000.0

                    if "Thrust" in label:
                        thrust = weight_kg * 9.8
                        print(f"[{label}] Weight: {weight:.2f} g | Thrust: {thrust:.2f} N")
                    elif "Torque" in label:
                        torque = weight_kg * 9.8 * R
                        print(f"[{label}] Weight: {weight:.2f} g | Torque: {torque:.2f} N·m")
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("Exiting...")
        for p in processes:
            p.terminate()
            p.join()

if __name__ == "__main__":
    main()


