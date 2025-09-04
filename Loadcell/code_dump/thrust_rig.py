from multiprocessing import Process, Queue, Event
from hx711_module import HX711
import can
import time
import os

# LoadCell Configuration
load_cells_config = [
    {"dout": 5,  "sck": 6},
    {"dout": 13, "sck": 19},
    {"dout": 17, "sck": 27},
    {"dout": 22, "sck": 23},
    {"dout": 24, "sck": 25},
    {"dout": 12, "sck": 16},
    {"dout": 20, "sck": 21},
    {"dout": 26, "sck": 18}
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
    262158.0, 841231, 844120, 842998, 843102, 840001, 841555, 843888
]

calibration_factors = [
    40.04, 142.8, 144.1, 143.7, 145.2, 144.5, 146.0, 143.9
]

# Radius for torque calculation in meters
R = 0.1

# CAN Configuration
CAN_INTERFACE = "can0"
BITRATE = 500000
BAMOCAR_ID = 0x201
BAMOCAR_RESPONSE_ID = 0x181

# ESC Parameter Registers
RPM_REGISTER = 0x30
VOLTAGE_REGISTER = 0xEB
CURRENT_REGISTER = 0x20
TEMP_REGISTER = 0x49

# Conversion factors
RPM_CONVERSION = 0.091547146780592
VOLTAGE_CONVERSION = 0.1
CURRENT_CONVERSION = 0.1
TEMP_CONVERSION = 0.1

def is_can_interface_up():
    result = os.popen(f"ip link show {CAN_INTERFACE}").read()
    return "<UP," in result

def setup_can_interface():
    if is_can_interface_up():
        print(f"CAN interface {CAN_INTERFACE} is already up.")
    else:
        print(f"Bringing up CAN interface {CAN_INTERFACE} at {BITRATE} bps...")
        os.system(f"sudo ip link set {CAN_INTERFACE} up type can bitrate {BITRATE}")

def set_process_affinity(process_id, core_id):
    try:
        os.sched_setaffinity(process_id, {core_id})
        print(f"Process {process_id} assigned to core {core_id}")
    except Exception as e:
        print(f"Error setting affinity: {e}")

def loadcell_worker(dout, sck, q, index, offset, calibration_factor, stop_event):
    hx = HX711(dout, sck)
    hx.offset = offset
    hx.calibration_factor = calibration_factor
    
    while not stop_event.is_set():
        weight, raw = hx.get_weight()
        q.put((index, weight, raw))
        time.sleep(0.2)

def can_worker(param_register, param_name, param_conversion, result_queue, stop_event):
    setup_can_interface()
    bus = can.interface.Bus(channel=CAN_INTERFACE, interface="socketcan")
    
    try:
        while not stop_event.is_set():
            msg = can.Message(
                arbitration_id=BAMOCAR_ID, 
                data=[0x3D, param_register, 0x64], 
                is_extended_id=False
            )
            bus.send(msg)
            
            start_time = time.time()
            while time.time() - start_time < 1.0 and not stop_event.is_set():
                message = bus.recv(timeout=0.1)
                if message and message.arbitration_id == BAMOCAR_RESPONSE_ID:
                    data = message.data
                    if data[0] == param_register:
                        raw_value = (data[2] << 8) | data[1]
                        value = param_conversion * raw_value
                        result_queue.put((param_name, value))
                        break
            
            time.sleep(0.2)
    
    finally:
        bus.shutdown()

def formatted_loadcell_output(label, weight, thrust_or_torque, unit):
    return f"{label:<15} | Weight: {weight:>10.2f} g | {unit}: {thrust_or_torque:>10.2f} {'N' if unit == 'Thrust' else 'Nm'}"

def main():
    stop_event = Event()
    loadcell_queues = []
    can_queue = Queue()
    loadcell_processes = []
    
    for i, config in enumerate(load_cells_config):
        q = Queue()
        loadcell_queues.append(q)
        p = Process(
            target=loadcell_worker, 
            args=(config['dout'], config['sck'], q, i, hardcoded_offsets[i], calibration_factors[i], stop_event)
        )
        p.start()
        core_id = 0 if i < 4 else 1
        set_process_affinity(p.pid, core_id)
        loadcell_processes.append(p)
    
    can_processes = []
    can_parameters = [
        (RPM_REGISTER, "RPM", RPM_CONVERSION),
        (VOLTAGE_REGISTER, "Voltage", VOLTAGE_CONVERSION),
        (CURRENT_REGISTER, "Current", CURRENT_CONVERSION),
        (TEMP_REGISTER, "Temperature", TEMP_CONVERSION)
    ]
    
    for i, (register, name, conversion) in enumerate(can_parameters):
        p = Process(
            target=can_worker,
            args=(register, name, conversion, can_queue, stop_event)
        )
        p.start()
        core_id = 2 if i < 2 else 3
        set_process_affinity(p.pid, core_id)
        can_processes.append(p)
    
    can_values = {
        "RPM": 0,
        "Voltage": 0,
        "Current": 0,
        "Temperature": 0
    }
    
    try:
        while True:
            loadcell_readings = {}
            for i, q in enumerate(loadcell_queues):
                data_received = False
                while not data_received:
                    if not q.empty():
                        idx, weight, raw = q.get()
                        loadcell_readings[idx] = (weight, raw)
                        data_received = True
                    elif stop_event.is_set():
                        return
                    else:
                        time.sleep(0.01)

            if len(loadcell_readings) == len(load_cells_config):
                print("\n===== LOADCELL READINGS =====")
                print("--- THRUST MEASUREMENTS ---")
                thrust_values = []
                for i in range(4):
                    weight, raw = loadcell_readings[i]
                    label = load_cell_labels[i]
                    weight_kg = weight / 1000.0
                    thrust = weight_kg * 9.8
                    thrust_values.append(thrust)
                    print(formatted_loadcell_output(label, weight, thrust, "Thrust"))

                print("\n--- TORQUE MEASUREMENTS ---")
                torque_values = []
                for i in range(4, 8):
                    weight, raw = loadcell_readings[i]
                    label = load_cell_labels[i]
                    weight_kg = weight / 1000.0
                    torque = weight_kg * 9.8 * R
                    torque_values.append(torque)
                    print(formatted_loadcell_output(label, weight, torque, "Torque"))

                total_thrust = sum(thrust_values)
                total_torque = sum(torque_values)
                print(f"\n{'Total Thrust':<15} | {total_thrust:>10.2f} N")
                print(f"{'Total Torque':<15} | {total_torque:>10.2f} Nm")

            while not can_queue.empty():
                param_name, value = can_queue.get()
                can_values[param_name] = value

            print("\n===== ESC CONTROLLER DATA (CAN) =====")
            print(f"{'RPM':<15} | {can_values['RPM']:>10.2f} rpm")
            print(f"{'Voltage':<15} | {can_values['Voltage']:>10.2f} V")
            print(f"{'Current':<15} | {can_values['Current']:>10.2f} A")
            print(f"{'Temperature':<15} | {can_values['Temperature']:>10.2f} °C")
            time.sleep(0.1)
    
    except KeyboardInterrupt:
        print("\nExiting...")
        stop_event.set()
        for p in loadcell_processes + can_processes:
            p.join(timeout=1.0)
            if p.is_alive():
                p.terminate()
        print("All processes terminated.")

if __name__ == "__main__":
    main()
