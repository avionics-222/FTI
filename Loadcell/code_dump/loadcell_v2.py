import multiprocessing
import time
TIMEOUT_SECONDS = 20
def loadcell_worker(dout, sck, load_cell_label, offset, queue, index):
    from hx711_module import HX711
    hx = HX711(dout, sck)
    hx.offset = offset
    try:

        while True:
            # Simulate weight reading
            weight_out, raw = hx.get_weight() # Dummy value for demonstration
            queue.put((index, weight_out))  # Send (index, weight) to main process
            time.sleep(0.1)
    except KeyboardInterrupt:
        print(f"Stopped: {load_cell_label}")

def loadcell_worker_test(dout, sck, load_cell_label, offset, queue, index):
    try:
        while True:
            # Simulate weight reading
            if index == 2:
                time.sleep(15)
            weight = index * 10 + 0.1  # Dummy value for demonstration
            queue.put((index, weight))  # Send (index, weight) to main process
            time.sleep(0.1)
    except KeyboardInterrupt:
        print(f"Stopped: {load_cell_label}")

if __name__ == "__main__":
    load_cells_config = [
        {"dout": 12,  "sck": 13},
        {"dout": 12, "sck": 13},
        {"dout": 20, "sck": 21},
        {"dout": 14, "sck": 15},
        {"dout": 12, "sck": 13},
        {"dout": 20, "sck": 21},
        {"dout": 14, "sck": 15},
        {"dout": 26, "sck": 19}
    ]
    hardcoded_offsets = [613980,691715,1342414,-1123296,0,0,0,0]#with Motor
    hardcoded_offsets_1=[503894,606426,1188189,-1238130,0,0,0,0]#Only Stand
    hardcoded_offsets_2 = [-1139, 30683, 213752, 81425,
        109366, 38272, -216303, 84388]
    load_cell_labels = [
        "Thrust_0Deg", "Thrust_90Deg", "Thrust_180Deg", "Thrust_270Deg",
        "Torque_0Deg", "Torque_90Deg", "Torque_180Deg", "Torque_270Deg"
    ]

    process_list = []
    queue = multiprocessing.Queue()
    number_lc = 4 #Change this number if you want to increase number of load cell5

    latest_weights = [0.0] * number_lc
    last_update_times = [time.time()] * number_lc

    try:
        for i in range(number_lc):
            p = multiprocessing.Process(
                target=loadcell_worker,
                args=(load_cells_config[i]["dout"], load_cells_config[i]["sck"],
                      load_cell_labels[i], hardcoded_offsets[i], queue, i)
            )
            process_list.append(p)
            p.start()

        while True:
            # Read all messages
            while not queue.empty():
                index, weight = queue.get()
                latest_weights[index] = round(weight,2)
                last_update_times[index] = time.time()

            # Check for timeout
            now = time.time()
            for i in range(number_lc):
                if now - last_update_times[i] > TIMEOUT_SECONDS:
                    print(f"No data from sensor {i+1} for more than {TIMEOUT_SECONDS} seconds.")
                    raise TimeoutError

            total_weight = sum(latest_weights)
            print(f"Latest Weights: {latest_weights} | Total: {total_weight:.2f} Kg | Thrust: {total_weight*9.8: .3f} N")
            time.sleep(0.1)

    except (KeyboardInterrupt, TimeoutError):
        print("Exiting due to KeyboardInterrupt or timeout.")
        for p in process_list:
            p.terminate()
            p.join()
