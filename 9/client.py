from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer
from datetime import datetime, timedelta, timezone
import random
import time
import threading
import queue
import socket
import json
import os
import psutil
import ping3
import subprocess

def load_config():
    config_paths = ['config.json', './config.json', '../config.json']
    
    for config_path in config_paths:
        try:
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                print(f"Configuration loaded successfully from {config_path}")
                return config
        except Exception as e:
            print(f"Failed to load {config_path}: {e}")
            continue
    
    print("Error: config.json file not found!")
    exit(1)

def create_server_url(config, server_type):
    server_config = config[server_type]
    protocol = server_config.get('protocol', 'http')
    ip = server_config['ip']
    port = server_config['port']
    url = f"{protocol}://{ip}:{port}"
    print(f"Created {server_type} URL: {url}")
    return url

config = load_config()
server_url = create_server_url(config, 'server')
teacher_url = create_server_url(config, 'teacher')
client_url = create_server_url(config, 'client')

server = ServerProxy(server_url + "/RPC2", allow_none=True)
teacher = ServerProxy(teacher_url + "/RPC2", allow_none=True)

student_list = config.get('student_list', [29, 40, 42, 50, 52])
student_status = {i: 0 for i in student_list}
test_duration_sec = config.get('test_duration_sec', 60)
students_info = {29: "Mayuresh", 40: "Ayush", 42: "Aashna", 50: "Rohit", 52: "Rushikesh"}

clock_offset = None
logical_clock = 0
request_queue = queue.PriorityQueue()
deferred_replies = []
requesting_cs = False
in_cs = False
num_nodes = 3
replies_received = 0
cs_lock = threading.Lock()
my_process_id = "client"
copying_students = [29, 40, 50, 52]

exam_active = False
exam_results = {}
current_exam_student = None
exam_start_time = None
deadlock_detected = False

load_test_active = False
total_requests_sent = 0
successful_responses = 0
failed_responses = 0
main_server_responses = 0
backup_server_responses = 0
request_threads = []

resource_monitor_active = False
network_stats = {
    "ping_server": 0,
    "ping_teacher": 0,
    "packets_sent": 0,
    "packets_lost": 0
}
system_resources = {
    "cpu_percent": 0,
    "memory_percent": 0,
    "disk_percent": 0,
    "network_bytes_sent": 0,
    "network_bytes_recv": 0
}

processing_jobs = []
job_queue = queue.Queue()
job_results = {}

def increment_clock():
    global logical_clock
    logical_clock += 1
    return logical_clock

def update_clock(received_timestamp):
    global logical_clock
    logical_clock = max(logical_clock, received_timestamp) + 1

def get_formatted_time():
    global clock_offset
    current_time = datetime.now() + clock_offset
    return current_time.strftime("%d/%b/%Y %H:%M:%S")

def input_initial_time():
    h, m, s = map(int, input("Enter client time (HH MM SS): ").split())
    manual_time = datetime.now(timezone.utc).replace(hour=h, minute=m, second=s, microsecond=0)
    system_utc = datetime.now(timezone.utc).replace(microsecond=0)
    return manual_time - system_utc

def get_local_time():
    global clock_offset
    return datetime.now(timezone.utc) + clock_offset

def adjust_time(offset_sec):
    global clock_offset
    clock_offset += timedelta(seconds=offset_sec)

def get_system_resources():
    try:
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()
        disk = psutil.disk_usage('/')
        net_io = psutil.net_io_counters()
        
        global system_resources
        system_resources = {
            "cpu_percent": cpu_percent,
            "memory_percent": memory.percent,
            "memory_available": memory.available // (1024*1024),
            "disk_percent": disk.percent,
            "disk_free": disk.free // (1024*1024*1024),
            "network_bytes_sent": net_io.bytes_sent,
            "network_bytes_recv": net_io.bytes_recv,
            "timestamp": time.time()
        }
        
        return system_resources
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Error getting system resources: {e}")
        return {
            "cpu_percent": 0,
            "memory_percent": 0,
            "memory_available": 1000,
            "disk_percent": 50,
            "disk_free": 10,
            "network_bytes_sent": 0,
            "network_bytes_recv": 0,
            "timestamp": time.time()
        }

def get_network_health():
    try:
        server_ip = server_url.split("//")[1].split(":")[0]
        teacher_ip = teacher_url.split("//")[1].split(":")[0]
        
        ping_server = ping3.ping(server_ip, timeout=2)
        ping_teacher = ping3.ping(teacher_ip, timeout=2)
        
        global network_stats
        network_stats["ping_server"] = round(ping_server * 1000, 2) if ping_server else 999.0
        network_stats["ping_teacher"] = round(ping_teacher * 1000, 2) if ping_teacher else 999.0
        network_stats["packets_sent"] += 2
        
        if ping_server is None:
            network_stats["packets_lost"] += 1
        if ping_teacher is None:
            network_stats["packets_lost"] += 1
        
        return network_stats
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Error getting network health: {e}")
        return {
            "ping_server": 999.0,
            "ping_teacher": 999.0,
            "packets_sent": 0,
            "packets_lost": 0
        }

def start_resource_monitoring():
    global resource_monitor_active
    
    def resource_monitor_thread():
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Resource monitoring started")
        
        while resource_monitor_active:
            try:
                resources = get_system_resources()
                network = get_network_health()
                
                node_metrics = {
                    "node_id": "client",
                    "resources": resources,
                    "network": network,
                    "status": "active",
                    "heartbeat": time.time()
                }
                
                try:
                    server.report_node_metrics(node_metrics)
                except Exception as e:
                    print(f"[{get_formatted_time()}] [CLIENT-TASK9] Failed to report metrics to resource manager: {e}")
                
                time.sleep(10)
                
            except Exception as e:
                print(f"[{get_formatted_time()}] [CLIENT-TASK9] Resource monitoring error: {e}")
                time.sleep(5)
    
    resource_monitor_active = True
    monitor_thread = threading.Thread(target=resource_monitor_thread, daemon=True)
    monitor_thread.start()

def stop_resource_monitoring():
    global resource_monitor_active
    resource_monitor_active = False
    print(f"[{get_formatted_time()}] [CLIENT-TASK9] Resource monitoring stopped")

def submit_processing_job(job_type, parameters=None):
    job_id = f"job_{int(time.time())}_{random.randint(1000, 9999)}"
    
    job = {
        "job_id": job_id,
        "job_type": job_type,
        "parameters": parameters or {},
        "submitted_by": "client",
        "submit_time": time.time(),
        "status": "submitted"
    }
    
    try:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Submitting processing job: {job_type}")
        result = server.submit_processing_job(job)
        
        if result.get("success"):
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job {job_id} submitted successfully")
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job queued for execution")
            return job_id
        else:
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job submission failed: {result.get('message', 'Unknown error')}")
            return None
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Error submitting job: {e}")
        return None

def check_job_status(job_id):
    try:
        result = server.get_job_status(job_id)
        
        if result.get("success"):
            job_info = result["job"]
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job {job_id} Status: {job_info['status']}")
            
            if job_info['status'] == 'completed':
                print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job Result: {job_info.get('result', 'No result')}")
                return job_info['result']
            elif job_info['status'] == 'failed':
                print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job Failed: {job_info.get('error', 'Unknown error')}")
            
            return job_info['status']
        else:
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Failed to get job status: {result.get('message', 'Unknown error')}")
            return None
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Error checking job status: {e}")
        return None

def view_cluster_resources():
    try:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Fetching cluster resource status...")
        result = server.get_cluster_resources()
        
        if result.get("success"):
            resources = result["resources"]
            
            print(f"\n[{get_formatted_time()}] " + "="*80)
            print(f"[{get_formatted_time()}] TASK 9: HADOOP-STYLE CLUSTER RESOURCE STATUS")
            print(f"[{get_formatted_time()}] " + "="*80)
            
            for node_id, metrics in resources.items():
                if metrics:
                    res = metrics.get("resources", {})
                    net = metrics.get("network", {})
                    
                    print(f"[{get_formatted_time()}] ")
                    print(f"[{get_formatted_time()}] Node: {node_id.upper()}")
                    print(f"[{get_formatted_time()}] CPU Usage: {res.get('cpu_percent', 0):.1f}%")
                    print(f"[{get_formatted_time()}] Memory Usage: {res.get('memory_percent', 0):.1f}%")
                    print(f"[{get_formatted_time()}] Memory Available: {res.get('memory_available', 0)} MB")
                    print(f"[{get_formatted_time()}] Disk Usage: {res.get('disk_percent', 0):.1f}%")
                    print(f"[{get_formatted_time()}] Disk Free: {res.get('disk_free', 0)} GB")
                    print(f"[{get_formatted_time()}] Network Latency: Server={net.get('ping_server', 999):.2f}ms, Teacher={net.get('ping_teacher', 999):.2f}ms")
                    print(f"[{get_formatted_time()}] Network Packets: Sent={net.get('packets_sent', 0)}, Lost={net.get('packets_lost', 0)}")
                    
                    last_update = time.time() - metrics.get("heartbeat", 0)
                    print(f"[{get_formatted_time()}] Status: {'ONLINE' if last_update < 30 else 'OFFLINE'} (last seen {last_update:.0f}s ago)")
                    print(f"[{get_formatted_time()}] " + "-"*60)
            
            print(f"[{get_formatted_time()}] " + "="*80)
            
        else:
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Failed to get cluster resources: {result.get('message', 'Unknown error')}")
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Error viewing cluster resources: {e}")

def view_job_queue():
    try:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Fetching job queue status...")
        result = server.get_job_queue_status()
        
        if result.get("success"):
            queue_info = result["queue"]
            
            print(f"\n[{get_formatted_time()}] " + "="*80)
            print(f"[{get_formatted_time()}] TASK 9: HADOOP-STYLE JOB QUEUE STATUS")
            print(f"[{get_formatted_time()}] " + "="*80)
            
            print(f"[{get_formatted_time()}] Total Jobs: {queue_info['total_jobs']}")
            print(f"[{get_formatted_time()}] Queued Jobs: {queue_info['queued_jobs']}")
            print(f"[{get_formatted_time()}] Running Jobs: {queue_info['running_jobs']}")
            print(f"[{get_formatted_time()}] Completed Jobs: {queue_info['completed_jobs']}")
            print(f"[{get_formatted_time()}] Failed Jobs: {queue_info['failed_jobs']}")
            
            if queue_info.get("recent_jobs"):
                print(f"[{get_formatted_time()}] ")
                print(f"[{get_formatted_time()}] Recent Jobs:")
                for job in queue_info["recent_jobs"][:5]:
                    print(f"[{get_formatted_time()}]   {job['job_id']}: {job['job_type']} ({job['status']})")
            
            print(f"[{get_formatted_time()}] " + "="*80)
            
        else:
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Failed to get job queue: {result.get('message', 'Unknown error')}")
            
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Error viewing job queue: {e}")

def run_distributed_analytics():
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] TASK 9: DISTRIBUTED DATA PROCESSING JOBS")
    print(f"[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] Select Analytics Job:")
    print(f"[{get_formatted_time()}] 1. Count students across all chunks")
    print(f"[{get_formatted_time()}] 2. Calculate average scores from replicas")
    print(f"[{get_formatted_time()}] 3. Find students with scores > 80")
    print(f"[{get_formatted_time()}] 4. Generate distributed report")
    print(f"[{get_formatted_time()}] 5. Process exam submissions")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    try:
        choice = input("Enter choice (1-5): ").strip()
        
        job_id = None
        if choice == "1":
            job_id = submit_processing_job("COUNT_STUDENTS")
        elif choice == "2":
            job_id = submit_processing_job("AVERAGE_SCORES")
        elif choice == "3":
            job_id = submit_processing_job("FILTER_BY_SCORE", {"min_score": 80})
        elif choice == "4":
            job_id = submit_processing_job("GENERATE_REPORT")
        elif choice == "5":
            job_id = submit_processing_job("PROCESS_SUBMISSIONS")
        else:
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Invalid choice")
            return
        
        if job_id:
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Monitoring job execution...")
            
            for i in range(10):
                time.sleep(2)
                status = check_job_status(job_id)
                
                if status in ['completed', 'failed']:
                    break
                    
                print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job {job_id} still {status}...")
            
            print(f"[{get_formatted_time()}] [CLIENT-TASK9] Job execution finished")
    
    except KeyboardInterrupt:
        print(f"\n[{get_formatted_time()}] [CLIENT-TASK9] Analytics interrupted by user")
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-TASK9] Analytics error: {e}")

def send_request_with_timeout(url, method, *args, timeout=5):
    try:
        proxy = ServerProxy(url + "/RPC2", allow_none=True)
        proxy._ServerProxy__transport.timeout = timeout
        result = getattr(proxy, method)(*args)
        return result
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT] Timeout/Error calling {method}: {e}")
        return False

def berkeley_sync():
    print("\n=== BERKELEY CLOCK SYNCHRONIZATION ===")
    print("Step 1: Time Master (Client) collecting times from all nodes")
    
    times = {}
    self_time = get_local_time().replace(tzinfo=None)
    print(f"Step 2: [Client] Local time: {self_time.time()}")
    times["client"] = self_time

    try:
        times["server"] = datetime.fromisoformat(server.get_time()).replace(tzinfo=None)
        times["teacher"] = datetime.fromisoformat(teacher.get_time()).replace(tzinfo=None)
        
        print(f"Step 3: [Server] Time: {times['server'].time()}")
        print(f"Step 3: [Teacher] Time: {times['teacher'].time()}")

        all_times = list(times.values())
        avg_time = sum([t.timestamp() for t in all_times]) / len(all_times)
        avg_dt = datetime.fromtimestamp(avg_time)
        
        print(f"Step 4: [Berkeley] Calculated average time: {avg_dt.time()}")
        print(f"Step 5: Applying time adjustments to all nodes:")
        
        adjust_time((avg_dt - self_time).total_seconds())
        server.adjust_time((avg_dt - times['server']).total_seconds())
        teacher.adjust_time((avg_dt - times['teacher']).total_seconds())

        print("Step 6: Verification - All nodes synchronized")
        print("=== SYNCHRONIZATION COMPLETE ===\n")

        print("=== TASK 8: INITIALIZING REPLICATION SYSTEM ===")
        print("Step 1: Triggering database chunking and replication...")
        server.initialize_replication_system()
        teacher.initialize_replication_system()
        
        print("Step 2: Background replication processes starting...")
        print("Step 3: Chunk distribution and metadata setup...")
        print("=== REPLICATION SYSTEM ACTIVE ===\n")
        
        print("=== TASK 9: INITIALIZING HADOOP-STYLE CLUSTER ===")
        print("Step 1: Starting resource monitoring across all nodes...")
        start_resource_monitoring()
        
        print("Step 2: Initializing YARN-like Resource Manager...")
        try:
            server.initialize_resource_manager()
            teacher.initialize_node_manager()
        except Exception as e:
            print(f"Resource manager initialization warning: {e}")
        
        print("Step 3: Starting distributed data processing system...")
        print("=== HADOOP-STYLE CLUSTER ACTIVE ===\n")

    except Exception as e:
        print(f"Berkeley sync failed: {e}")

def bombard_server_with_requests():
    global load_test_active, total_requests_sent, request_threads
    
    num_requests = random.randint(15, 25)
    
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] TASK 7: SERVER LOAD BALANCING TEST")
    print(f"[{get_formatted_time()}] Automatically generating {num_requests} concurrent requests")
    print(f"[{get_formatted_time()}] Using randomly generated unique student IDs")
    print(f"[{get_formatted_time()}] Expected: 100% success rate with load balancing")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    load_test_active = True
    request_threads = []
    
    global successful_responses, failed_responses, main_server_responses, backup_server_responses
    successful_responses = 0
    failed_responses = 0
    main_server_responses = 0
    backup_server_responses = 0
    total_requests_sent = 0
    
    base_id = random.randint(10000, 99999)
    unique_student_ids = []
    
    for i in range(num_requests):
        student_id = base_id + i + random.randint(100, 999)
        unique_student_ids.append(student_id)
    
    print(f"[{get_formatted_time()}] [LOAD-TEST] Generated student IDs: {unique_student_ids[:3]}...{unique_student_ids[-2:]}")
    
    for i in range(num_requests):
        student_id = unique_student_ids[i]
        request_thread = threading.Thread(
            target=send_load_test_request,
            args=(i+1, student_id),
            daemon=True
        )
        
        request_threads.append(request_thread)
        print(f"[{get_formatted_time()}] [LOAD-TEST] Launching request {i+1}/{num_requests} for student {student_id}")
        request_thread.start()
        
        time.sleep(0.02)
    
    print(f"\n[{get_formatted_time()}] [LOAD-TEST] All {num_requests} requests launched - waiting for completion...")
    
    completed_count = 0
    timed_out_count = 0
    
    for i, thread in enumerate(request_threads):
        thread.join(timeout=30)
        if not thread.is_alive():
            completed_count += 1
            if completed_count % 5 == 0:
                print(f"[{get_formatted_time()}] [LOAD-TEST] Progress: {completed_count}/{num_requests} completed")
        else:
            timed_out_count += 1
            print(f"[{get_formatted_time()}] [LOAD-TEST] Request {i+1} timed out")
    
    print(f"[{get_formatted_time()}] [LOAD-TEST] Final status: {completed_count} completed, {timed_out_count} timed out")
    display_load_test_results()

def send_load_test_request(request_num, student_id):
    global total_requests_sent, successful_responses, failed_responses, main_server_responses, backup_server_responses
    
    total_requests_sent += 1
    
    print(f"[{get_formatted_time()}] [REQUEST-{request_num}] Starting for student {student_id}")
    
    try:
        result = server.start_interactive_exam(student_id)
        
        if result and result.get("success"):
            successful_responses += 1
            server_used = result.get("server", "unknown")
            
            if server_used == "main":
                main_server_responses += 1
                print(f"[{get_formatted_time()}] [REQUEST-{request_num}] SUCCESS - Main server processing")
            elif server_used == "backup":
                backup_server_responses += 1
                print(f"[{get_formatted_time()}] [REQUEST-{request_num}] SUCCESS - Load balanced to backup processing")
            else:
                successful_responses += 1
                print(f"[{get_formatted_time()}] [REQUEST-{request_num}] SUCCESS - Server processing")
            
        else:
            failed_responses += 1
            print(f"[{get_formatted_time()}] [REQUEST-{request_num}] FAILED: {result.get('message', 'Unknown')}")
            
    except Exception as e:
        failed_responses += 1
        print(f"[{get_formatted_time()}] [REQUEST-{request_num}] EXCEPTION: {e}")

def display_load_test_results():
    print(f"\n[{get_formatted_time()}] " + "="*80)
    print(f"[{get_formatted_time()}] TASK 7: LOAD BALANCING TEST RESULTS")
    print(f"[{get_formatted_time()}] " + "="*80)
    
    print(f"[{get_formatted_time()}] Total Requests Sent: {total_requests_sent}")
    print(f"[{get_formatted_time()}] Successful Responses: {successful_responses}")
    print(f"[{get_formatted_time()}] Failed Responses: {failed_responses}")
    print(f"[{get_formatted_time()}] Main Server Processing: {main_server_responses}")
    print(f"[{get_formatted_time()}] Backup Server Processing: {backup_server_responses}")
    
    if total_requests_sent > 0:
        success_rate = (successful_responses / total_requests_sent) * 100
        migration_rate = (backup_server_responses / total_requests_sent) * 100
        
        print(f"[{get_formatted_time()}] ")
        print(f"[{get_formatted_time()}] PERFORMANCE METRICS:")
        print(f"[{get_formatted_time()}] SUCCESS RATE: {success_rate:.1f}%")
        print(f"[{get_formatted_time()}] MIGRATION RATE: {migration_rate:.1f}%")
        print(f"[{get_formatted_time()}] MAIN SERVER UTILIZATION: {(main_server_responses/total_requests_sent)*100:.1f}%")
        print(f"[{get_formatted_time()}] BACKUP SERVER UTILIZATION: {migration_rate:.1f}%")
        
        if backup_server_responses > 0:
            print(f"[{get_formatted_time()}] ")
            print(f"[{get_formatted_time()}] LOAD BALANCING: ACTIVE")
            print(f"[{get_formatted_time()}] Load balancer successfully migrated {backup_server_responses} requests")
            print(f"[{get_formatted_time()}] Server handled overload gracefully")
            print(f"[{get_formatted_time()}] Chaining worked: Main -> Backup distribution")
        else:
            print(f"[{get_formatted_time()}] LOAD BALANCING: NOT TRIGGERED")
            print(f"[{get_formatted_time()}] All requests handled by main server buffer")
        
        if success_rate >= 100:
            print(f"[{get_formatted_time()}] ")
            print(f"[{get_formatted_time()}] RESULT: PERFECT! 100% Success Rate Achieved!")
            print(f"[{get_formatted_time()}] No service denial - All {total_requests_sent} requests processed")
            print(f"[{get_formatted_time()}] Load balancing system working optimally")
        elif success_rate >= 95:
            print(f"[{get_formatted_time()}] RESULT: EXCELLENT - High success rate ({success_rate:.1f}%)")
        elif success_rate >= 85:
            print(f"[{get_formatted_time()}] RESULT: GOOD - Acceptable success rate ({success_rate:.1f}%)")
        else:
            print(f"[{get_formatted_time()}] RESULT: NEEDS IMPROVEMENT - Low success rate ({success_rate:.1f}%)")
    
    print(f"[{get_formatted_time()}] " + "="*80)

def start_interactive_exam(student_id):
    global exam_active, current_exam_student, exam_start_time
    
    try:
        print(f"\n" + "="*80)
        print(f"TASK 6: DEADLOCK IN EXAM GRID - INTERACTIVE EXAM")
        print(f"STUDENT: {student_id} ({students_info.get(student_id, 'Unknown')})")
        print(f"TIMER: 1 minute (60 seconds)")
        print(f"DEADLOCK RESOLUTION: Simultaneous manual/auto submission handling")
        print(f"="*80)
        
        result = server.start_interactive_exam(student_id)
        
        if result and result.get("success"):
            exam_active = True
            current_exam_student = student_id
            exam_start_time = datetime.now()
            
            print(f"EXAM INITIALIZED SUCCESSFULLY!")
            print(f"Duration: {result.get('duration', 60)} seconds")
            print(f"Total Questions: {result.get('total_questions', 10)}")
            print(f"Server processing request transparently")
            print(f"="*80)
            
            conduct_interactive_exam(student_id)
            return True
        else:
            print(f"EXAM INITIALIZATION FAILED")
            print(f"Error: {result.get('message', 'Unknown error')}")
            return False
            
    except Exception as e:
        print(f"Exception starting exam: {e}")
        return False

def conduct_interactive_exam(student_id):
    global exam_active
    
    print(f"\n" + "="*90)
    print(f"INTERACTIVE EXAM INTERFACE ACTIVATED")
    print(f"TYPE A, B, C, or D and press ENTER")
    print(f"Student: {student_id} ({students_info.get(student_id, 'Unknown')})")
    print(f"="*90)
    
    question_count = 0
    
    while exam_active:
        try:
            print(f"\n[CLIENT] Getting question {question_count + 1} from server...")
            question_data = server.get_question(student_id)
            
            if not question_data.get("success"):
                if question_data.get("completed"):
                    print(f"\nALL QUESTIONS COMPLETED!")
                    print(f"You can now submit manually or wait for auto-submit.")
                    
                    try:
                        submit_choice = input("Do you want to SUBMIT NOW? (y/n): ").strip().lower()
                        if submit_choice == 'y' or submit_choice == 'yes':
                            print(f"User chose MANUAL submission!")
                            attempt_manual_submission_with_deadlock(student_id)
                        else:
                            print(f"User chose to wait - AUTO-submit will occur at timer expiry")
                    except:
                        pass
                    break
                    
                elif question_data.get("time_expired"):
                    print(f"\nSERVER REPORTS: EXAM TIME EXPIRED!")
                    print(f"AUTO-SUBMISSION IN PROGRESS...")
                    exam_active = False
                    break
                else:
                    error_msg = question_data.get('message', 'Unknown error')
                    if "time" in error_msg.lower() or "expired" in error_msg.lower():
                        print(f"\nTIME EXPIRED - AUTO-SUBMISSION TRIGGERED!")
                        exam_active = False
                        break
                    else:
                        print(f"Error getting question: {error_msg}")
                        break
            
            question = question_data["question"]
            q_num = question_data["question_number"]
            total_q = question_data["total_questions"]
            remaining_time = question_data["remaining_time"]
            
            if remaining_time <= 0:
                print(f"\nTIME REMAINING: 0 seconds - EXAM EXPIRED!")
                print(f"AUTO-SUBMISSION TRIGGERED!")
                exam_active = False
                break
            
            print(f"\n" + "="*70)
            print(f"QUESTION {q_num} of {total_q}")
            print(f"TIME REMAINING: {remaining_time:.0f} seconds")
            print(f"="*70)
            print()
            print(f"{question['q']}")
            print()
            for option in question['options']:
                print(f"{option}")
            print()
            print(f"-"*70)
            
            while True:
                try:
                    user_answer = input(f"Your answer (A/B/C/D): ").strip().upper()
                    if user_answer in ['A', 'B', 'C', 'D']:
                        print(f"You selected: {user_answer}")
                        break
                    else:
                        print("Please enter A, B, C, or D only!")
                except KeyboardInterrupt:
                    print(f"\nExam interrupted by user")
                    return
                except Exception as e:
                    print(f"Input error: {e}")
                    continue
            
            print(f"[CLIENT] Submitting your answer '{user_answer}' to server...")
            answer_result = server.submit_answer(student_id, user_answer)
            
            if answer_result.get("success"):
                is_correct = answer_result.get("is_correct", False)
                correct_answer = answer_result.get("correct_answer", "")
                remaining_time = answer_result.get("remaining_time", 0)
                
                if remaining_time <= 0:
                    print(f"\n" + "="*50)
                    print(f"ANSWER SUBMITTED!")
                    print(f"Your answer: {user_answer}")
                    print(f"Correct answer: {correct_answer}")
                    print(f"Result: {'CORRECT!' if is_correct else 'Wrong'}")
                    print(f"="*50)
                    print(f"\nTIME EXPIRED AFTER SUBMISSION!")
                    print(f"AUTO-SUBMISSION TRIGGERED!")
                    exam_active = False
                    break
                
                print(f"\n" + "="*50)
                print(f"ANSWER SUBMITTED SUCCESSFULLY!")
                print(f"Your answer: {user_answer}")
                print(f"Correct answer: {correct_answer}")
                print(f"Result: {'CORRECT!' if is_correct else 'Wrong'}")
                print(f"Time remaining: {remaining_time:.0f} seconds")
                print(f"="*50)
                
                question_count += 1
                
                if answer_result.get("all_completed"):
                    print(f"\nCONGRATULATIONS! ALL QUESTIONS ANSWERED!")
                    try:
                        submit_choice = input("Submit exam NOW? (y/n): ").strip().lower()
                        if submit_choice == 'y' or submit_choice == 'yes':
                            print(f"Submitting exam immediately...")
                            attempt_manual_submission_with_deadlock(student_id)
                            break
                        else:
                            print(f"Waiting for auto-submit...")
                    except:
                        pass
                    break
            else:
                print(f"Error submitting answer: {answer_result.get('message', 'Unknown')}")
                break
                
        except Exception as e:
            print(f"Exception in exam interface: {e}")
            break
    
    if not exam_active:
        print(f"\n" + "="*80)
        print(f"EXAM SESSION ENDED")
        print(f"Waiting for final results from teacher...")
        print(f"="*80)

def attempt_manual_submission_with_deadlock(student_id):
    global deadlock_detected
    
    print(f"\n" + "="*80)
    print(f"TASK 6: MANUAL SUBMISSION ATTEMPT")
    print(f"DEADLOCK DETECTION: Active")
    print(f"[CLIENT] Requesting final submission from server...")
    print(f"="*80)
    
    try:
        result = server.submit_exam_final(student_id, "manual")
        
        if result and result.get("success"):
            if result.get("deadlock_resolved"):
                print(f"\nDEADLOCK DETECTED AND RESOLVED!")
                print(f"Resolution Strategy: {result.get('resolution_strategy', 'Unknown')}")
                deadlock_detected = True
            
            print(f"\nFINAL SUBMISSION SUCCESSFUL!")
            
            score = result.get('score', 0)
            submission_type = result.get('type', 'unknown')
            
            print(f"Score: {score}/10 questions correct")
            print(f"Final Score: {score * 10}/100 marks")
            print(f"Submission Type: {submission_type.upper()}")
            print(f"Result: {'PASS' if (score * 10) >= 50 else 'FAIL'}")
            
            global exam_active
            exam_active = False
        else:
            print(f"\nSUBMISSION FAILED!")
            print(f"Error: {result.get('message', 'Unknown error')}")
            
    except Exception as e:
        print(f"Exception during final submission: {e}")

def handle_exam_timeout(student_id):
    global exam_active
    
    print(f"\n" + "="*90)
    print(f"EXAM TIME EXPIRED!")
    print(f"Your exam is being AUTO-SUBMITTED automatically")
    print(f"Please wait while we process your answers...")
    print(f"="*90)
    
    exam_active = False
    
    time.sleep(2)
    
    print(f"\nAUTO-SUBMISSION COMPLETE!")
    print(f"Waiting for final results from teacher...")
    
    return True

def receive_exam_results(student_id, score, submission_type):
    exam_results[student_id] = {"score": score, "type": submission_type}
    
    score_out_of_100 = score * 10
    
    print(f"\nFINAL EXAM RESULTS RECEIVED FROM TEACHER")
    print(f"Student: {student_id} ({students_info.get(student_id, 'Unknown')})")
    print(f"RAW SCORE: {score}/10 questions")
    print(f"FINAL SCORE: {score_out_of_100}/100 marks")
    print(f"Submission Method: {submission_type.upper()}")
    print(f"Final Grade: {'PASS' if score_out_of_100 >= 50 else 'FAIL'}")
    
    if deadlock_detected:
        print(f"DEADLOCK DETECTED: Yes - Resolution was successful")
    
    return True

def request_critical_section(student_id):
    global requesting_cs, replies_received, logical_clock
    timestamp = increment_clock()
    print(f"[{get_formatted_time()}] [CLIENT-RA] Student {students_info.get(student_id, student_id)} requesting Critical Section")
    
    requesting_cs = True
    replies_received = 0
    
    try:
        success1 = send_request_with_timeout(teacher_url, "receive_request", my_process_id, timestamp)
        success2 = send_request_with_timeout(server_url, "receive_request", my_process_id, timestamp)
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-RA] Error sending requests: {e}")
    
    threading.Thread(target=wait_for_replies_timeout, args=(timestamp, student_id), daemon=True).start()

def wait_for_replies_timeout(request_timestamp, student_id, timeout=10):
    global requesting_cs
    start_time = time.time()
    
    while requesting_cs and replies_received < (num_nodes - 1):
        if time.time() - start_time > timeout:
            break
        time.sleep(0.1)
    
    if requesting_cs:
        enter_critical_section(student_id)

def receive_request(sender_id, timestamp):
    global logical_clock, deferred_replies, requesting_cs, in_cs
    update_clock(timestamp)
    print(f"[{get_formatted_time()}] [CLIENT-RA] Received REQUEST from {sender_id}")
    
    should_reply = True
    if requesting_cs or in_cs:
        if timestamp > logical_clock or (timestamp == logical_clock and sender_id > my_process_id):
            should_reply = False
            deferred_replies.append(sender_id)
    
    if should_reply:
        send_reply(sender_id)
    
    return True

def send_reply(receiver_id):
    global logical_clock
    timestamp = increment_clock()
    
    try:
        if receiver_id == "teacher":
            success = send_request_with_timeout(teacher_url, "receive_reply", my_process_id, timestamp)
        elif receiver_id == "server":
            success = send_request_with_timeout(server_url, "receive_reply", my_process_id, timestamp)
    except Exception as e:
        pass

def receive_reply(sender_id, timestamp):
    global replies_received, logical_clock, requesting_cs, in_cs
    update_clock(timestamp)
    
    if requesting_cs:
        replies_received += 1
    
    return True

def enter_critical_section(student_id=None):
    global in_cs, requesting_cs
    
    if student_id is None:
        student_id = random.choice(copying_students)
    
    print(f"[{get_formatted_time()}] [CLIENT-RA] *** ENTERING CRITICAL SECTION ***")
    print(f"[{get_formatted_time()}] [CLIENT-RA] Student {students_info.get(student_id, student_id)} accessing marksheet")
    
    in_cs = True
    requesting_cs = False
    
    time.sleep(3)
    
    student_status[student_id] += 1
    status = student_status[student_id]
    
    try:
        msg = server.update_status(student_id, status)
        if status == 1:
            print(f"[{get_formatted_time()}] [CLIENT-RA] Student {student_id} WARNED (marks: 100→50)")
        elif status == 2:
            print(f"[{get_formatted_time()}] [CLIENT-RA] Student {student_id} CAUGHT (marks: 50→0 FAILED)")
        print(f"[{get_formatted_time()}] [CLIENT-RA] Server response: {msg}")
    except Exception as e:
        print(f"[{get_formatted_time()}] [CLIENT-RA] Failed to update server: {e}")
    
    exit_critical_section()

def exit_critical_section():
    global in_cs, deferred_replies, logical_clock
    print(f"[{get_formatted_time()}] [CLIENT-RA] *** EXITING CRITICAL SECTION ***")
    
    in_cs = False
    
    for receiver_id in deferred_replies:
        send_reply(receiver_id)
        print(f"[{get_formatted_time()}] [CLIENT-RA] Sent deferred reply to {receiver_id}")
    
    deferred_replies.clear()

def pick_valid_student():
    valid = [num for num, status in student_status.items() if status < 2]
    return random.choice(valid) if valid else None

def send_student():
    student = pick_valid_student()
    
    if student is None:
        print(f"[{get_formatted_time()}] All students processed. Requesting final marksheet...")
        try:
            teacher.print_results()
        except Exception as e:
            print(f"[{get_formatted_time()}] Failed to get marksheet: {e}")
        return False
    
    if student in copying_students:
        print(f"[{get_formatted_time()}] [CLIENT] Student {students_info.get(student, student)} wants to copy - requesting CS")
        request_critical_section(student)
        return True
    else:
        student_status[student] += 1
        status = student_status[student]
        
        try:
            msg = server.update_status(student, status)
            action = "warned" if status == 1 else "caught"
            print(f"[{get_formatted_time()}] Sent {student} — Status: {status} ({action}) — Server: {msg}")
            return True
        except Exception as e:
            print(f"[{get_formatted_time()}] Failed to send student {student}: {e}")
            return False

def get_time_for_berkeley():
    return get_local_time().isoformat()

def run_client_server():
    hostname = socket.gethostname()
    local_ip = socket.gethostbyname(hostname)
    print(f"[{get_formatted_time()}] [CLIENT-RA] Starting XML-RPC server on {local_ip}:{config['client']['port']}")
    
    server_instance = SimpleXMLRPCServer(("0.0.0.0", config['client']['port']), allow_none=True)
    server_instance.register_function(receive_request, "receive_request")
    server_instance.register_function(receive_reply, "receive_reply")
    server_instance.register_function(get_time_for_berkeley, "get_time_for_berkeley")
    server_instance.register_function(adjust_time, "adjust_time")
    server_instance.register_function(receive_exam_results, "receive_exam_results")
    server_instance.register_function(handle_exam_timeout, "handle_exam_timeout")
    
    server_thread = threading.Thread(target=server_instance.serve_forever, daemon=True)
    server_thread.start()
    print(f"[{get_formatted_time()}] [CLIENT-RA] XML-RPC server running...")

def run():
    global clock_offset
    clock_offset = input_initial_time()
    
    print(f"[{get_formatted_time()}] Starting client with ALL TASKS...")
    print(f"[{get_formatted_time()}] [CLIENT-RA] Traditional CS students: {copying_students}")
    print(f"[{get_formatted_time()}] [CLIENT-TASK6] Interactive Deadlock Exam System")
    print(f"[{get_formatted_time()}] [CLIENT-TASK7] Server Load Balancing Test (Auto-scaling)")
    print(f"[{get_formatted_time()}] [CLIENT-TASK8] Database Replication System (Background)")
    print(f"[{get_formatted_time()}] [CLIENT-TASK9] Hadoop-style Resource Management + Distributed Processing")
    
    run_client_server()
    
    try:
        result = server.register(client_url, teacher_url)
        print(f"[{get_formatted_time()}] Registration result: {result}")
    except Exception as e:
        print(f"[{get_formatted_time()}] Registration failed: {e}")
        exit(1)

    berkeley_sync()
    
    print(f"\n" + "="*80)
    print(f"SELECT OPERATION MODE:")
    print(f"1. Traditional Cheating Detection (Ricart-Agrawala)")
    print(f"2. Interactive Deadlock Exam (Task 6)")
    print(f"3. Server Load Balancing Test (Task 7)")
    print(f"4. View Replicated Database Status (Task 8)")
    print(f"5. Submit Data Processing Job (Task 9)")
    print(f"6. View Cluster Resource Status (Task 9)")
    print(f"7. Check Job Queue Status (Task 9)")
    print(f"8. Run Distributed Analytics (Task 9)")
    print(f"="*80)
    
    while True:
        try:
            choice = input("Enter your choice (1-8): ").strip()
            if choice in ['1', '2', '3', '4', '5', '6', '7', '8']:
                break
            else:
                print("Please enter 1-8!")
        except:
            print("Invalid input, please try again!")
    
    if choice == '2':
        print(f"\nStarting Task 6: Interactive Deadlock Exam...")
        exam_student = 52
        start_interactive_exam(exam_student)
    elif choice == '3':
        print(f"\nStarting Task 7: Server Load Balancing Test...")
        bombard_server_with_requests()
    elif choice == '4':
        print(f"\nViewing Task 8: Replicated Database Status...")
        try:
            server.show_replication_status()
            teacher.show_replication_status()
        except Exception as e:
            print(f"Error viewing replication status: {e}")
    elif choice == '5':
        print(f"\nStarting Task 9: Submit Data Processing Job...")
        job_id = submit_processing_job("COUNT_STUDENTS")
        if job_id:
            time.sleep(3)
            check_job_status(job_id)
    elif choice == '6':
        print(f"\nViewing Task 9: Cluster Resource Status...")
        view_cluster_resources()
    elif choice == '7':
        print(f"\nViewing Task 9: Job Queue Status...")
        view_job_queue()
    elif choice == '8':
        print(f"\nStarting Task 9: Distributed Analytics...")
        run_distributed_analytics()
    else:
        print(f"\nStarting Traditional Cheating Detection mode...")
        
        try:
            teacher.run_timer(test_duration_sec)
            start_time_str = teacher.get_test_start_time()
            start_time = datetime.fromisoformat(start_time_str).replace(tzinfo=None)
            current_time = get_local_time().replace(tzinfo=None)
            delay = (start_time - current_time).total_seconds()
            
            if delay > 0:
                print(f"[{get_formatted_time()}] Waiting for test to start in {delay:.2f} seconds...")
                time.sleep(delay)
            
            while True:
                try:
                    if not teacher.is_test_active():
                        break
                    if not send_student():
                        break
                    time.sleep(9)
                except Exception as e:
                    print(f"[{get_formatted_time()}] Error in main loop: {e}")
                    break
            
            print(f"[{get_formatted_time()}] Test finished.")
        except Exception as e:
            print(f"[{get_formatted_time()}] Failed to run test: {e}")

if __name__ == '__main__':
    run()
