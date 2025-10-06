from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from xmlrpc.client import ServerProxy
import threading
import time
from datetime import datetime, timedelta, timezone

students = {
    29: {"name": "Mayuresh", "marks": 100, "reason": ""},
    40: {"name": "Ayush", "marks": 100, "reason": ""},
    42: {"name": "Aashna", "marks": 100, "reason": ""},
    50: {"name": "Rohit", "marks": 100, "reason": ""},
    52: {"name": "Rushikesh", "marks": 100, "reason": ""},
}

test_active = False
start_utc = None
client_url = ""
server_url = ""
self_url = ""

def input_initial_time():
    h, m, s = map(int, input("Enter teacher time (HH MM SS): ").split())
    manual_time = datetime.now(timezone.utc).replace(hour=h, minute=m, second=s, microsecond=0)
    system_utc = datetime.now(timezone.utc).replace(microsecond=0)
    return manual_time - system_utc

clock_offset = input_initial_time()

def get_time():
    return (datetime.now(timezone.utc) + clock_offset).isoformat()

def get_test_start_time():
    return start_utc

def run_timer(duration_sec):
    def timer_thread():
        global test_active
        print(f"Test started for {duration_sec} seconds...")
        time.sleep(duration_sec)
        print("Time's up!")
        test_active = False
        print_results()
    
    global start_utc, test_active
    start_utc = get_time()
    test_active = True
    threading.Thread(target=timer_thread, daemon=True).start()
    return True

def warn_student(roll_no):
    if roll_no in students:
        print(f"[Teacher] WARNING: Student {students[roll_no]['name']} (Roll: {roll_no})")
        students[roll_no]["marks"] = 50
        students[roll_no]["reason"] = "Warning"
        return True
    else:
        print(f"Invalid roll number: {roll_no}")
        return False

def catch_student(roll_no):
    if roll_no in students:
        print(f"[Teacher] CAUGHT: Student {students[roll_no]['name']} (Roll: {roll_no}) cheating!")
        students[roll_no]["marks"] = 0
        students[roll_no]["reason"] = "Cheating"
        return True
    else:
        print(f"Invalid roll number: {roll_no}")
        return False

def is_test_active():
    return test_active

def print_results():
    print("\n" + "="*60)
    print("FINAL STUDENT REPORT")
    print("="*60)
    header = f"{'Roll No':<8} | {'Name':<12} | {'Marks':<5} | {'Status':<6} | {'Reason':<10}"
    print(header)
    print("-" * len(header))
    
    for roll_no, info in students.items():
        reason = info["reason"] if info["reason"] else "-"
        status = "PASS" if info["marks"] >= 50 else "FAIL"
        print(f"{roll_no:<8} | {info['name']:<12} | {info['marks']:<5} | {status:<6} | {reason:<10}")
    
    print("-" * len(header))
    print("="*60)
    return True

def register_urls(client, server, self_):
    global client_url, server_url, self_url
    client_url = client
    server_url = server
    self_url = self_
    print(f"[Teacher] URLs registered - Client: {client}, Server: {server}")
    return True

def adjust_time(offset_sec):
    global clock_offset
    clock_offset += timedelta(seconds=offset_sec)
    current_time = datetime.now(timezone.utc) + clock_offset
    print(f"[Teacher] Time adjusted by {offset_sec} seconds to: {current_time.time()}")
    return True

def run_teacher():
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"Teacher hostname: {hostname}")
    print(f"Teacher IP address: {local_ip}")
    print("Teacher server running at port 9001...")
    print(f"Teacher accessible at: http://{local_ip}:9001")
    
    with SimpleXMLRPCServer(("0.0.0.0", 9001),
                           requestHandler=SimpleXMLRPCRequestHandler,
                           allow_none=True) as server:
        server.register_function(get_time, "get_time")
        server.register_function(get_test_start_time, "get_test_start_time")
        server.register_function(warn_student, "warn_student")
        server.register_function(catch_student, "catch_student")
        server.register_function(print_results, "print_results")
        server.register_function(run_timer, "run_timer")
        server.register_function(is_test_active, "is_test_active")
        server.register_function(register_urls, "register_urls")
        server.register_function(adjust_time, "adjust_time")
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nTeacher server shutting down...")

if __name__ == "__main__":
    run_teacher()
