from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
import threading
import time

students = {
    29: {"name": "Mayuresh", "marks": 100, "reason": ""},
    40: {"name": "Ayush", "marks": 100, "reason": ""},
    42: {"name": "Aashna", "marks": 100, "reason": ""},
    50: {"name": "Rohit", "marks": 100, "reason": ""},
    52: {"name": "Rushikesh", "marks": 100, "reason": ""},
}

test_active = True

def warn_student(roll_no):
    if roll_no in students:
        students[roll_no]["marks"] = 50
        print(f"Warning issued to student {roll_no} ({students[roll_no]['name']}) - Marks reduced to 50")
        return True
    else:
        print(f"Invalid roll number: {roll_no}")
        return False

def catch_student(roll_no):
    if roll_no in students:
        students[roll_no]["marks"] = 0
        students[roll_no]["reason"] = "Cheating"
        print(f"Student {roll_no} ({students[roll_no]['name']}) caught cheating - Marks set to 0")
        return True
    else:
        print(f"Invalid roll number: {roll_no}")
        return False

def print_results():
    print("\nFinal Student Report:\n")
    header = f"{'Roll No':<8} | {'Name':<12} | {'Marks':<5} | {'Status':<6} | {'Reason':<10}"
    print(header)
    print("-" * len(header))
    
    for roll_no, info in students.items():
        reason = info["reason"] if info["reason"] else "-"
        status = "PASS" if info["marks"] >= 50 else "FAIL"
        print(f"{roll_no:<8} | {info['name']:<12} | {info['marks']:<5} | {status:<6} | {reason:<10}")
    
    print("-" * len(header))
    return True


def run_timer(duration_sec):
    def timer_thread():
        global test_active
        print(f"Test started for {duration_sec} seconds...")
        time.sleep(duration_sec)
        print("Time's up!")
        test_active = False
        print_results()
    
    threading.Thread(target=timer_thread, daemon=True).start()
    return True

def is_test_active():
    return test_active

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

def run_teacher():
    print("Starting Teacher Server...")
    
    with SimpleXMLRPCServer(("0.0.0.0", 9001), requestHandler=RequestHandler, allow_none=True) as server:
        server.register_function(warn_student, "warn_student")
        server.register_function(catch_student, "catch_student") 
        server.register_function(print_results, "print_results")
        server.register_function(run_timer, "run_timer")
        server.register_function(is_test_active, "is_test_active")
        
        print("Teacher server running at port 9001...")
        server.serve_forever()

if __name__ == "__main__":
    run_teacher()
