from xmlrpc.client import ServerProxy
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
    protocol = server_config.get('protocol', 'https')
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

# Initialize connections
print("Connecting to server...")
server = ServerProxy(server_url)
print("Connecting to teacher...")
teacher = ServerProxy(teacher_url)

# Register teacher with server
print("Registering teacher with server...")
server.register_teacher(teacher_url)

# Get student list from config
student_list = config.get('student_list', [29, 40, 42, 50, 52])
student_status = {i: 0 for i in student_list}
print(f"Initialized student list: {student_list}")

# Get test duration from config
test_duration_sec = config.get('test_duration_sec', 60)
print(f"Test duration set to: {test_duration_sec} seconds")

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
        teacher.print_results()
        return False
    
    student_status[student] += 1
    print(f"Sending student {student} to server (attempt #{student_status[student]})")
    msg = server.cheating(student)
    print(f"Sent {student} — Status: {student_status[student]} — Server says: {msg}")
    return True

def run():
    print("Starting test simulation...")
    print(f"Starting test timer ({test_duration_sec} seconds)...")
    teacher.run_timer(test_duration_sec)
    
    while True:
        if not teacher.is_test_active():
            print("Test time expired!")
            break
        
        if not send_student():
            print("No more students available to send!")
            break
        time.sleep(3)
    
    print("Test simulation finished.")

if __name__ == '__main__':
    run()
