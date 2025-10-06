from xmlrpc.client import ServerProxy
from datetime import datetime, timedelta, timezone
import random
import time
import json

def load_config():
    try:
        with open('../config.json', 'r') as f:
            config = json.load(f)
        print("Configuration loaded successfully from config.json")
        return config
    except FileNotFoundError:
        print("Error: config.json file not found!")
        print("Please create a config.json file with server and teacher IP configurations.")
        exit(1)
    except json.JSONDecodeError:
        print("Error: Invalid JSON format in config.json!")
        exit(1)

def create_server_url(config, server_type):
    server_config = config[server_type]
    protocol = server_config.get('protocol', 'http')
    ip = server_config['ip']
    port = server_config['port']
    url = f"{protocol}://{ip}:{port}"
    print(f"Created {server_type} URL: {url}")
    return url

# Load configuration from JSON file
config = load_config()

# Create server and teacher URLs from config
server_url = create_server_url(config, 'server')
teacher_url = create_server_url(config, 'teacher')
client_url = create_server_url(config, 'client')

print(f"Server URL: {server_url}")
print(f"Teacher URL: {teacher_url}")
print(f"Client URL: {client_url}")

server = ServerProxy(server_url + "/RPC2")
teacher = ServerProxy(teacher_url + "/RPC2")

# Register with the server
try:
    result = server.register(client_url, teacher_url)
    print(f"Registration result: {result}")
except Exception as e:
    print(f"Registration failed: {e}")
    exit(1)

# Get student list and test duration from config
student_list = config.get('student_list', [29, 40, 42, 50, 52])
student_status = {i: 0 for i in student_list}
test_duration_sec = config.get('test_duration_sec', 60)

def input_initial_time():
    h, m, s = map(int, input("Enter client time (HH MM SS): ").split())
    manual_time = datetime.now(timezone.utc).replace(hour=h, minute=m, second=s, microsecond=0)
    system_utc = datetime.now(timezone.utc).replace(microsecond=0)
    return manual_time - system_utc

clock_offset = input_initial_time()

def get_local_time():
    return datetime.now(timezone.utc) + clock_offset

def adjust_time(offset_sec):
    global clock_offset
    clock_offset += timedelta(seconds=offset_sec)

def berkeley_sync():
    print("\n=== BERKELEY CLOCK SYNCHRONIZATION ===")
    print("Step 1: Time Master (Client) collecting times from all nodes")
    
    times = {}
    self_time = get_local_time().replace(tzinfo=None)
    print(f"Step 2: [Client] Local time: {self_time.time()}")
    times["client"] = self_time

    try:
        # Step 3: Get times from server and teacher
        times["server"] = datetime.fromisoformat(server.get_time()).replace(tzinfo=None)
        times["teacher"] = datetime.fromisoformat(teacher.get_time()).replace(tzinfo=None)
        
        print(f"Step 3: [Server] Time: {times['server'].time()}")
        print(f"Step 3: [Teacher] Time: {times['teacher'].time()}")

        # Step 4: Calculate average time
        all_times = list(times.values())
        avg_time = sum([t.timestamp() for t in all_times]) / len(all_times)
        avg_dt = datetime.fromtimestamp(avg_time)
        
        print(f"Step 4: [Berkeley] Calculated average time: {avg_dt.time()}")
        print("Step 4: Calculation = (Client + Server + Teacher) / 3")
        print(f"Step 4: ({self_time.timestamp():.2f} + {times['server'].timestamp():.2f} + {times['teacher'].timestamp():.2f}) / 3 = {avg_time:.2f}")

        # Step 5: Calculate offsets for each node
        print("Step 5: Calculating offsets for synchronization:")
        for name, dt in times.items():
            diff = (avg_dt - dt).total_seconds()
            print(f"  [{name.capitalize()}] Offset: {diff:.3f} seconds")

        # Step 6: Apply offsets to synchronize all clocks
        print("Step 6: Applying time adjustments to all nodes:")
        adjust_time((avg_dt - self_time).total_seconds())
        server.adjust_time((avg_dt - times['server']).total_seconds())
        teacher.adjust_time((avg_dt - times['teacher']).total_seconds())

        # Step 7: Verify synchronization
        print("Step 7: Verification - All nodes synchronized to:")
        print(f"  [Client] Time adjusted to: {get_local_time().time()}")
        print("  [Server] Time adjusted (confirmed by server)")
        print("  [Teacher] Time adjusted (confirmed by teacher)")
        print("=== SYNCHRONIZATION COMPLETE ===\n")

    except Exception as e:
        print(f"Berkeley sync failed: {e}")

def pick_valid_student():
    valid = [num for num, status in student_status.items() if status < 2]
    selected = random.choice(valid) if valid else None
    if selected:
        print(f"Selected student: {selected} (available: {valid})")
    return selected

def send_student():
    student = pick_valid_student()
    
    if student is None:
        print("No more students left to send.")
        print("Requesting final results from teacher...")
        try:
            teacher.print_results()
        except Exception as e:
            print(f"Failed to print results: {e}")
        return False

    student_status[student] += 1
    status = student_status[student]
    
    try:
        msg = server.update_status(student, status)
        action = "warned" if status == 1 else "caught"
        print(f"Sent {student} — Status: {status} ({action}) — Server says: {msg}")
        return True
    except Exception as e:
        print(f"Failed to send student {student}: {e}")
        return False

def run():
    print("Starting client...")
    
    # Step 1: Perform Berkeley Clock Synchronization first
    berkeley_sync()
    
    # Step 2: Start the test after synchronization
    print(f"Starting test simulation (duration: {test_duration_sec} seconds)...")
    try:
        teacher.run_timer(test_duration_sec)
        start_time = datetime.fromisoformat(teacher.get_test_start_time())
        delay = (start_time - get_local_time()).total_seconds()
        
        if delay > 0:
            print(f"Waiting for test to start in {delay:.2f} seconds...")
            time.sleep(delay)

        while True:
            try:
                if not teacher.is_test_active():
                    print("Test time expired!")
                    break
                if not send_student():
                    print("No more students available to send!")
                    break
                time.sleep(4)
            except Exception as e:
                print(f"Error in main loop: {e}")
                break

        print("Test simulation finished.")

    except Exception as e:
        print(f"Failed to run test: {e}")

if __name__ == '__main__':
    run()
