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

# Radius for torque calculation in meters
R = 0.1  # Replace with your actual radius value

def main():
    # Initialize HX711 objects for all load cells
    hx_objects = []
    for i in range(len(load_cells_config)):
        print(i, load_cells_config[i]['dout'], load_cells_config[i]['sck'])
        try:
            hx = HX711(load_cells_config[i]['dout'], load_cells_config[i]['sck'])
            hx.offset = hardcoded_offsets[i]
            #hx.calibration_factor = calibration_factors[i]
            hx_objects.append(hx)  
        except Exception as e:
            print(e)
            time.sleep(5)

    try:
        while True:
            readings = []
            print(hx_objects)
            # Read all sensors sequentially
            for y in range(len(hx_objects)):
                
                if y == 4 or y == 7:
                    readings.append((0,0))
                    continue
                print(hx_objects[y],y)    
                    
                weight, raw = hx_objects[y].get_weight()
                readings.append((weight, raw))

            print("\n===== LOADCELL READINGS =====")
            for i in range(len(readings)):
                weight, raw = readings[i]
                label = load_cell_labels[i]
                weight_kg = weight / 1000.0  # Convert from g to kg

                if i < 4:  # Thrust loadcells (0-3)
                    thrust = weight_kg * 9.8  # Thrust in Newtons
                    print(f"[{label}] Weight: {weight_kg:.2f} Kg, Thrust: {thrust:.2f} N")
                else:      # Torque loadcells (4-7)
                    torque = weight_kg * 9.8 * R  # Torque in Nm
                    print(f"[{label}] Weight: {weight_kg:.2f} Kg, Torque: {torque:.2f} Nm")

            total_thrust = sum([(readings[i][0] / 1000.0) * 9.8 for i in range(4)])
            total_torque = sum([(readings[i][0] / 1000.0) * 9.8 * R for i in range(4, 8)])
            print(f"\nTotal Thrust: {total_thrust:.2f} N")
            print(f"Total Torque: {total_torque:.2f} Nm")

            time.sleep(0.5)

    except KeyboardInterrupt:
        print("Exiting...")

if __name__ == "__main__":
    main()
