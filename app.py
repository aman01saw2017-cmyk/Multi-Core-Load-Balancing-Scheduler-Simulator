from flask import Flask, request, jsonify
from flask_cors import CORS
import threading
import time
import heapq
from collections import deque
import uuid

app = Flask(__name__)
# Allow connections from anywhere (for local development)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Simulation state
class Process:
    def __init__(self, pid, burst_time, priority=0):
        self.id = pid
        self.burst_time = burst_time
        self.remaining_time = burst_time
        self.priority = priority
        self.arrival_time = 0
        self.start_time = None
        self.end_time = None
        self.waiting_time = 0
        self.turnaround_time = 0
        self.core_id = None

class Core:
    def __init__(self, core_id):
        self.id = core_id
        self.current_process = None
        self.process_queue = deque()
        self.total_load = 0
        self.is_busy = False
        self.time_slice = 2
        self.remaining_quantum = 0

class Scheduler:
    def __init__(self, num_cores):
        self.cores = [Core(i) for i in range(num_cores)]
        self.ready_queue = []
        self.processes = {}
        self.running = False
        self.speed = 1.0
        self.time_unit = 0
        self.completed_processes = []
        self.migration_logs = []
        self.use_load_balancing = True
        
    def add_process(self, pid, burst_time, priority=0):
        process = Process(pid, burst_time, priority)
        self.processes[pid] = process
        heapq.heappush(self.ready_queue, (priority, len(self.processes), process))
        return process
    
    def find_least_loaded_core(self):
        loads = [(core.total_load, core.id) for core in self.cores]
        loads.sort()
        return self.cores[loads[0][1]]
    
    def migrate_processes(self):
        if not self.use_load_balancing:
            return
            
        loads = [(core.total_load, core.id, core) for core in self.cores]
        loads.sort()
        
        if len(loads) > 1 and loads[-1][0] > loads[0][0] * 2:
            overloaded_core = loads[-1][2]
            underloaded_core = loads[0][2]
            
            if overloaded_core.process_queue and overloaded_core.id != underloaded_core.id:
                process_to_migrate = overloaded_core.process_queue.popleft()
                underloaded_core.process_queue.append(process_to_migrate)
                process_to_migrate.core_id = underloaded_core.id
                
                log = f"🔄 Migrated Process {process_to_migrate.id} from Core {overloaded_core.id + 1} to Core {underloaded_core.id + 1}"
                self.migration_logs.append(log)
                
                overloaded_core.total_load -= process_to_migrate.remaining_time
                underloaded_core.total_load += process_to_migrate.remaining_time
    
    def schedule_to_core(self, process):
        target_core = self.find_least_loaded_core()
        process.core_id = target_core.id
        target_core.process_queue.append(process)
        target_core.total_load += process.remaining_time
        return target_core
    
    def simulate_step(self):
        if not self.running:
            return
        
        while self.ready_queue and len(self.ready_queue) > 0:
            priority, _, process = heapq.heappop(self.ready_queue)
            if process.remaining_time > 0 and process.end_time is None:
                self.schedule_to_core(process)
        
        for core in self.cores:
            if core.current_process is None and core.process_queue:
                core.current_process = core.process_queue.popleft()
                if core.current_process.start_time is None:
                    core.current_process.start_time = self.time_unit
                core.remaining_quantum = core.time_slice
                core.is_busy = True
            
            if core.current_process:
                exec_time = min(core.remaining_quantum, core.current_process.remaining_time, 0.1)
                core.current_process.remaining_time -= exec_time
                core.remaining_quantum -= exec_time
                
                core.total_load -= exec_time
                
                if core.current_process.remaining_time <= 0:
                    core.current_process.end_time = self.time_unit
                    core.current_process.turnaround_time = core.current_process.end_time - core.current_process.arrival_time
                    core.current_process.waiting_time = core.current_process.turnaround_time - core.current_process.burst_time
                    self.completed_processes.append(core.current_process)
                    core.current_process = None
                    core.is_busy = False
                elif core.remaining_quantum <= 0:
                    core.process_queue.append(core.current_process)
                    core.current_process = None
                    core.is_busy = False
        
        self.migrate_processes()
        self.time_unit += 0.1
    
    def start(self):
        self.running = True
        
    def pause(self):
        self.running = False
        
    def reset(self):
        self.running = False
        self.cores = [Core(i) for i in range(len(self.cores))]
        self.ready_queue = []
        self.processes = {}
        self.completed_processes = []
        self.migration_logs = []
        self.time_unit = 0
        
    def get_status(self):
        cores_status = []
        for core in self.cores:
            cores_status.append({
                'id': core.id,
                'current_process': core.current_process.id if core.current_process else None,
                'current_process_remaining': core.current_process.remaining_time if core.current_process else 0,
                'queue': [p.id for p in core.process_queue],
                'load': round(core.total_load, 2),
                'is_busy': core.is_busy
            })
        
        if self.completed_processes:
            avg_waiting = sum(p.waiting_time for p in self.completed_processes) / len(self.completed_processes)
            avg_turnaround = sum(p.turnaround_time for p in self.completed_processes) / len(self.completed_processes)
            throughput = len(self.completed_processes) / self.time_unit if self.time_unit > 0 else 0
        else:
            avg_waiting = 0
            avg_turnaround = 0
            throughput = 0
        
        busy_cores = sum(1 for core in self.cores if core.is_busy)
        cpu_util = (busy_cores / len(self.cores)) * 100
        
        return {
            'cores': cores_status,
            'stats': {
                'avg_waiting_time': round(avg_waiting, 2),
                'avg_turnaround_time': round(avg_turnaround, 2),
                'throughput': round(throughput, 2),
                'cpu_utilization': round(cpu_util, 2),
                'total_processes': len(self.completed_processes)
            },
            'migration_logs': self.migration_logs[-10:],
            'simulation_time': round(self.time_unit, 1)
        }

current_scheduler = None

@app.route('/api/init', methods=['POST'])
def init_scheduler():
    global current_scheduler
    data = request.json
    num_cores = data.get('num_cores', 4)
    current_scheduler = Scheduler(num_cores)
    return jsonify({'status': 'initialized', 'num_cores': num_cores})

@app.route('/api/add_process', methods=['POST'])
def add_process():
    global current_scheduler
    if not current_scheduler:
        return jsonify({'error': 'Scheduler not initialized'}), 400
    
    data = request.json
    pid = data.get('pid')
    burst_time = data.get('burst_time')
    priority = data.get('priority', 0)
    
    process = current_scheduler.add_process(pid, burst_time, priority)
    return jsonify({'status': 'added', 'process': {
        'id': process.id,
        'burst_time': process.burst_time,
        'priority': process.priority
    }})

@app.route('/api/control', methods=['POST'])
def control_simulation():
    global current_scheduler
    if not current_scheduler:
        return jsonify({'error': 'Scheduler not initialized'}), 400
    
    action = request.json.get('action')
    if action == 'start':
        current_scheduler.start()
    elif action == 'pause':
        current_scheduler.pause()
    elif action == 'reset':
        current_scheduler.reset()
    elif action == 'toggle_balancing':
        current_scheduler.use_load_balancing = not current_scheduler.use_load_balancing
        return jsonify({'use_load_balancing': current_scheduler.use_load_balancing})
    
    return jsonify({'status': action + 'ed'})

@app.route('/api/status', methods=['GET'])
def get_status():
    global current_scheduler
    if not current_scheduler:
        return jsonify({'error': 'Scheduler not initialized'}), 400
    
    return jsonify(current_scheduler.get_status())

@app.route('/api/test', methods=['GET'])
def test():
    return jsonify({'message': 'Backend is running!', 'status': 'online'})

def run_simulation():
    global current_scheduler
    while True:
        if current_scheduler and current_scheduler.running:
            current_scheduler.simulate_step()
        time.sleep(0.1)

sim_thread = threading.Thread(target=run_simulation, daemon=True)
sim_thread.start()

if __name__ == '__main__':
    print("=" * 50)
    print("🚀 Multi-Core Scheduler Backend")
    print("=" * 50)
    print("✅ Server starting on: http://localhost:5000")
    print("✅ Test URL: http://localhost:5000/api/test")
    print("=" * 50)
    app.run(debug=True, port=5000, host='localhost')