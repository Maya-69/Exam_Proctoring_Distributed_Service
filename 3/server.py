from xmlrpc.server import SimpleXMLRPCServer, SimpleXMLRPCRequestHandler
from xmlrpc.client import ServerProxy

# Student status tracking
student_status = {i: 0 for i in [29, 40, 42, 50, 52]}
teacher_proxy = None

def cheating(roll_no):
    global teacher_proxy
    
    if teacher_proxy is None:
        print(f"Teacher not connected when processing student {roll_no}")
        return "Teacher not connected."
    
    status = student_status.get(roll_no)
    if status is None:
        print(f"Invalid roll number received: {roll_no}")
        return "Invalid roll number."
    
    if status == 0:
        student_status[roll_no] = 1
        teacher_proxy.warn_student(roll_no)
        print(f"Student {roll_no} warned (first offense)")
        return f"Student {roll_no} warned."
        
    elif status == 1:
        student_status[roll_no] = 2
        teacher_proxy.catch_student(roll_no)
        print(f"Student {roll_no} caught cheating (second offense)")
        return f"Student {roll_no} caught cheating."
        
    else:
        print(f"Student {roll_no} already caught - no action taken")
        return f"Student {roll_no} already caught."

def register_teacher(teacher_url):
    global teacher_proxy
    full_url = teacher_url + "/RPC2" if "/RPC2" not in teacher_url else teacher_url
    teacher_proxy = ServerProxy(full_url)
    print(f"Teacher registered successfully at: {full_url}")
    return "Teacher registered."

class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

def run_server():
    print("Starting Main Server...")
    print("Server binding to 0.0.0.0:8000")
    
    with SimpleXMLRPCServer(("0.0.0.0", 8000), requestHandler=RequestHandler, allow_none=True) as server:
        server.register_function(cheating, "cheating")
        server.register_function(register_teacher, "register_teacher")
        
        print("Server running at port 8000...")
        print("Registered functions: cheating, register_teacher")
        print("Ready to handle requests...")
        
        server.serve_forever()

if __name__ == "__main__":
    run_server()
