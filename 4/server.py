from xmlrpc.server import SimpleXMLRPCServer
import xmlrpc.client
from datetime import datetime, timedelta

def input_time():
    h, m, s = map(int, input("Enter server time (HH MM SS): ").split())
    now = datetime.now().replace(hour=h, minute=m, second=s, microsecond=0)
    return now

time_now = input_time()
clients = {}
client_url = ""
teacher_url = ""

# Original baseline functionality
student_status = {i: 0 for i in [29, 40, 42, 50, 52]}
teacher_proxy = None

def register(client, teacher):
    global client_url, teacher_url, teacher_proxy
    client_url = client
    teacher_url = teacher
    
    print(f"Registering client: {client}")
    print(f"Registering teacher: {teacher}")
    
    try:
        clients["client"] = xmlrpc.client.ServerProxy(client + "/RPC2")
        clients["teacher"] = xmlrpc.client.ServerProxy(teacher + "/RPC2")
        teacher_proxy = clients["teacher"]
        print("Client and teacher registered successfully.")
        return "Registered client and teacher."
    except Exception as e:
        print(f"Registration failed: {e}")
        return f"Registration failed: {e}"

def get_time():
    return time_now.isoformat()

def adjust_time(offset):
    global time_now
    time_now += timedelta(seconds=offset)
    print(f"Server time adjusted by {offset} seconds to: {time_now.time()}")
    return True

def berkeley_sync():
    global time_now
    print("[Server] Starting Berkeley time sync...")
    times = {"server": time_now}
    
    for name, proxy in clients.items():
        try:
            remote_time = datetime.fromisoformat(proxy.get_time())
            times[name] = remote_time
            print(f"[Server] Got time from {name}: {remote_time.time()}")
        except Exception as e:
            print(f"[Server] {name} unreachable: {e}")

    all_times = list(times.values())
    if len(all_times) > 1:
        avg_seconds = sum([(t - time_now).total_seconds() for t in all_times]) / len(all_times)
        adjusted = time_now + timedelta(seconds=avg_seconds)
        
        for name, proxy in clients.items():
            if name in times:
                offset = (adjusted - times[name]).total_seconds()
                try:
                    proxy.adjust_time(offset)
                    print(f"[Server] Adjusted {name}'s time by {offset:.3f} seconds")
                except Exception as e:
                    print(f"[Server] Failed to adjust {name}'s time: {e}")
        
        time_now = adjusted
        print(f"[Server] Time synced to: {adjusted.time()}")
        return adjusted.isoformat()
    else:
        print("[Server] No clients to sync with")
        return time_now.isoformat()

# Enhanced cheating detection with original logic
def update_status(roll_no, status):
    print(f"[Server] Updating status for student {roll_no}: {status}")
    
    if roll_no not in student_status:
        student_status[roll_no] = 0
    
    student_status[roll_no] = status
    
    try:
        if status == 1:
            result = teacher_proxy.warn_student(roll_no)
            print(f"[Server] Student {roll_no} warned (first offense)")
            return "Warned"
        elif status == 2:
            result = teacher_proxy.catch_student(roll_no)
            print(f"[Server] Student {roll_no} caught cheating (second offense)")
            return "Caught"
        else:
            return "Status updated"
    except Exception as e:
        print(f"[Server] Error communicating with teacher: {e}")
        return f"Error: {e}"

def run_server():
    import socket
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    
    print(f"Server hostname: {hostname}")
    print(f"Server IP address: {local_ip}")
    print("Server running on port 8000...")
    print(f"Server accessible at: http://{local_ip}:8000")
    
    server = SimpleXMLRPCServer(("0.0.0.0", 8000), allow_none=True)
    server.register_function(register, "register")
    server.register_function(get_time, "get_time")
    server.register_function(adjust_time, "adjust_time")
    server.register_function(berkeley_sync, "berkeley_sync")
    server.register_function(update_status, "update_status")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer shutting down...")

if __name__ == "__main__":
    run_server()
