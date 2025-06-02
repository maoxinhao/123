#!/usr/bin/env python3

import socket
import sys
import time
import heapq
import math
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import List, Dict, Optional, Tuple


class Job:
    def __init__(self, id, submit_time, cores, memory, disk, est_runtime):
        self.id = id
        self.submit_time = submit_time
        self.cores = cores
        self.memory = memory
        self.disk = disk
        self.est_runtime = est_runtime

    def __repr__(self):
        return (f"Job(id={self.id}, submit_time={self.submit_time}, cores={self.cores}, "
                f"memory={self.memory}, disk={self.disk}, est_runtime={self.est_runtime})")


class Server:
    def __init__(self, type, id, state, start_time, cores, memory, disk, waiting_jobs, running_jobs):
        self.type = type
        self.id = id
        self.state = state
        self.start_time = start_time
        self.cores = cores
        self.memory = memory
        self.disk = disk
        self.waiting_jobs = waiting_jobs
        self.running_jobs = running_jobs

    def __repr__(self):
        return (f"Server(type={self.type}, id={self.id}, state={self.state}, start_time={self.start_time}, "
                f"cores={self.cores}, memory={self.memory}, disk={self.disk}, "
                f"waiting_jobs={self.waiting_jobs}, running_jobs={self.running_jobs})")

class MyScheduler:

    def __init__(self):
        self.servers = {}
        self.server_types = set()
        self.job_counter = 0
        self.failed_assignments = set()
        self.pending_jobs = []
        self.max_batch_size = 3
        self.batch_timeout = 0.001
        self.last_batch_time = time.time()

    def parse_server_info(self, data_lines):
        servers = defaultdict(list)

        for line in data_lines:
            if not line.strip():
                continue
            parts = [p for p in line.strip().split() if p]
            if len(parts) >= 9:
                server = Server(
                    type=parts[0],
                    id=int(parts[1]),
                    state=parts[2],
                    start_time=int(parts[3]),
                    cores=int(parts[4]),
                    memory=int(parts[5]),
                    disk=int(parts[6]),
                    waiting_jobs=int(parts[7]),
                    running_jobs=int(parts[8])
                )
                servers[server.type].append(server)
                self.server_types.add(server.type)

        self.servers = servers
        return servers

    def can_fit_job(self, server: Server, job: Job) -> bool:
        return (server.state != 'unavailable' and
                server.cores >= job.cores and
                server.memory >= job.memory and
                server.disk >= job.disk)

    def update_failed_assignments(self, failed_assignments):
        self.failed_assignments = failed_assignments

    def calculate_server_score(self, server: Server, job: Job) -> float:
        if server.state == 'idle':
            return 0
        elif server.state == 'active':
            total_load = server.waiting_jobs + server.running_jobs
            if total_load == 0:
                return 1
            elif total_load <= 2:
                return 5 + total_load
            else:
                return 15 + total_load
        elif server.state == 'booting':
            return 30
        elif server.state == 'inactive':
            return 60
        else:
            return 200

    def find_optimal_server(self, job: Job) -> Optional[Server]:
        candidates = []
        idle_servers = []
        active_servers = []
        inactive_servers = []

        for servers in self.servers.values():
            for server in servers:
                if (self.can_fit_job(server, job) and
                        (job.id, server.type, server.id) not in self.failed_assignments):
                    score = self.calculate_server_score(server, job)

                    if server.state == 'idle':
                        idle_servers.append((score, server))
                    elif server.state == 'active' and (server.waiting_jobs + server.running_jobs) <= 3:
                        active_servers.append((score, server))
                    elif server.state == 'inactive':
                        inactive_servers.append((score, server))
                    else:
                        candidates.append((score, server))

        all_candidates = []

        if idle_servers:
            idle_servers.sort(key=lambda x: x[0])
            all_candidates.extend(idle_servers)

        if active_servers:
            active_servers.sort(key=lambda x: x[0])
            all_candidates.extend(active_servers)

        if inactive_servers and len(all_candidates) < 2:
            inactive_servers.sort(key=lambda x: x[0])
            all_candidates.extend(inactive_servers[:2])

        if candidates:
            candidates.sort(key=lambda x: x[0])
            all_candidates.extend(candidates)

        if not all_candidates:
            return None

        return all_candidates[0][1]

    def schedule_job(self, job: Job) -> Tuple[str, int]:
        self.job_counter += 1

        if not self.servers:
            return "small", 0

        server = self.find_optimal_server(job)

        if server is None:
            for servers in self.servers.values():
                for s in servers:
                    if (s.state != 'unavailable' and
                            (job.id, s.type, s.id) not in self.failed_assignments):
                        return s.type, s.id
            return "small", 0

        return server.type, server.id

    def should_refresh_for_job(self, job: Job) -> bool:
        if job.est_runtime <= 300:
            return self.job_counter <= 2 or self.job_counter % 20 == 0
        else:
            return self.job_counter <= 3 or self.job_counter % 10 == 0

    def estimate_turnaround_time(self, job: Job, server: Server) -> float:
        if server.state == 'idle':
            return job.est_runtime
        elif server.state == 'active':
            if server.waiting_jobs == 0 and server.running_jobs == 1:
                estimated_remaining_time = 60
                return estimated_remaining_time + job.est_runtime
            else:
                estimated_queue_time = server.waiting_jobs * 10
                estimated_current_job_time = 60 if server.running_jobs > 0 else 0
                return estimated_queue_time + estimated_current_job_time + job.est_runtime
        elif server.state == 'booting':
            return 60 + job.est_runtime
        elif server.state == 'inactive':
            return 60 + job.est_runtime
        else:
            return 999999

    def calculate_resource_efficiency(self, job: Job, server: Server) -> float:
        if server.cores == 0 or server.memory == 0 or server.disk == 0:
            return 0

        cpu_efficiency = (job.cores / server.cores) * 100
        memory_efficiency = (job.memory / server.memory) * 100
        disk_efficiency = (job.disk / server.disk) * 100

        avg_efficiency = (cpu_efficiency + memory_efficiency + disk_efficiency) / 3
        return min(avg_efficiency, 100)

    def find_best_assignment(self, job: Job) -> Tuple[Optional[Server], float]:
        idle_candidates = []
        active_candidates = []
        boot_candidates = []
        inactive_candidates = []

        for servers in self.servers.values():
            for server in servers:
                if (self.can_fit_job(server, job) and
                        (job.id, server.type, server.id) not in self.failed_assignments):

                    turnaround = self.estimate_turnaround_time(job, server)
                    resource_efficiency = self.calculate_resource_efficiency(job, server)

                    adjusted_turnaround = turnaround - (resource_efficiency * 0.7)

                    if server.state == 'idle':
                        idle_candidates.append((adjusted_turnaround, server))
                    elif server.state == 'active':
                        active_candidates.append((adjusted_turnaround, server))
                    elif server.state == 'booting':
                        boot_candidates.append((adjusted_turnaround, server))
                    elif server.state == 'inactive':
                        inactive_candidates.append((adjusted_turnaround, server))

        all_candidates = []

        if idle_candidates:
            idle_candidates.sort(key=lambda x: x[0])
            return idle_candidates[0][1], idle_candidates[0][0]

        if active_candidates:
            active_candidates.sort(key=lambda x: x[0])
            for turnaround, server in active_candidates:
                if server.waiting_jobs == 0:
                    return server, turnaround
            return active_candidates[0][1], active_candidates[0][0]

        if boot_candidates:
            boot_candidates.sort(key=lambda x: x[0])
            return boot_candidates[0][1], boot_candidates[0][0]

        if inactive_candidates:
            inactive_candidates.sort(key=lambda x: x[0])
            return inactive_candidates[0][1], inactive_candidates[0][0]

        return None, float('inf')

    def optimize_job_order(self, jobs: List[Job]) -> List[Tuple[Job, str, int]]:
        job_assignments = []

        for job in jobs:
            server, turnaround = self.find_best_assignment(job)
            if server:
                job_assignments.append((job, server, turnaround))

        job_assignments.sort(key=lambda x: x[2])

        optimized_schedule = []
        for job, server, turnaround in job_assignments:
            optimized_schedule.append((job, server.type, server.id))

        return optimized_schedule

    def should_process_batch(self) -> bool:
        current_time = time.time()

        return (len(self.pending_jobs) >= self.max_batch_size or
                (self.pending_jobs and
                 current_time - self.last_batch_time > self.batch_timeout))

    def add_job_to_batch(self, job: Job) -> Optional[List[Tuple[Job, str, int]]]:
        self.pending_jobs.append(job)

        if self.should_process_batch():
            optimized_jobs = self.optimize_job_order(self.pending_jobs.copy())
            self.pending_jobs.clear()
            self.last_batch_time = time.time()
            return optimized_jobs

        return None

    def force_process_pending(self) -> List[Tuple[Job, str, int]]:
        if not self.pending_jobs:
            return []

        optimized_jobs = self.optimize_job_order(self.pending_jobs.copy())
        self.pending_jobs.clear()
        self.last_batch_time = time.time()
        return optimized_jobs

    def schedule_job_optimized(self, job: Job) -> Tuple[str, int]:
        self.job_counter += 1

        if not self.servers:
            return "small", 0

        best_server, best_turnaround = self.find_best_assignment(job)

        if best_server is None:
            for servers in self.servers.values():
                for s in servers:
                    if (s.state != 'unavailable' and
                            (job.id, s.type, s.id) not in self.failed_assignments):
                        return s.type, s.id
            return "small", 0

        return best_server.type, best_server.id


class DSClient:

    def __init__(self, port=50000):
        self.port = port
        self.socket = None
        self.scheduler = MyScheduler()
        self.servers_cached = False
        self.force_refresh_next = False
        self.failed_assignments = set()
        self.optimized_queue = deque()

    def connect(self):
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect(('localhost', self.port))
        except Exception as e:
            raise

    def send_message(self, message):
        msg = message + '\n'
        self.socket.send(msg.encode())

    def receive_message(self):
        try:
            data = self.socket.recv(2048)
            if not data:
                return ""

            decoded = data.decode().strip()
            return decoded

        except socket.timeout:
            return ""
        except Exception as e:
            return ""

    def handshake(self):
        try:
            self.send_message("HELO")
            response = self.receive_message()
            if response != "OK":
                raise Exception(f"Handshake failed: Expected 'OK', got '{response}'")

            self.send_message("AUTH 46768653")
            response = self.receive_message()
            if response != "OK":
                raise Exception(f"Authentication failed: Expected 'OK', got '{response}'")
        except Exception as e:
            raise

    def should_refresh_servers(self, job=None) -> bool:
        return True

    def get_servers(self):
        try:
            self.send_message("GETS All")
            response = self.receive_message()

            if response.startswith("DATA"):
                parts = response.split()
                num_records = int(parts[1])

                self.send_message("OK")
                server_data = self.receive_message()

                data_lines = []
                if server_data:
                    lines = server_data.split('\n')
                    for line in lines:
                        line = line.strip()
                        if line and line != ".":
                            data_lines.append(line)

                try:
                    self.socket.settimeout(0.001)
                    terminator = self.receive_message()
                    self.socket.settimeout(None)
                except:
                    self.socket.settimeout(None)

                self.send_message("OK")

                result = self.scheduler.parse_server_info(data_lines)
                return result

        except Exception as e:
            try:
                self.socket.settimeout(None)
            except:
                pass

        return {}

    def parse_job(self, job_msg):
        parts = job_msg.split()
        if len(parts) >= 7:
            return Job(
                id=int(parts[1]),
                submit_time=int(parts[2]),
                cores=int(parts[3]),
                memory=int(parts[4]),
                disk=int(parts[5]),
                est_runtime=int(parts[6])
            )
        return None

    def run(self):
        try:
            self.connect()
            self.handshake()

            servers = self.get_servers()
            if servers:
                self.servers_cached = True

            self.send_message("REDY")

            empty_response_count = 0

            while True:
                message = self.receive_message()

                if not message:
                    empty_response_count += 1
                    if empty_response_count >= 6:
                        try:
                            self.socket.close()
                            self.connect()
                            self.handshake()
                            self.send_message("REDY")
                            empty_response_count = 0
                            continue
                        except Exception as e:
                            break
                    else:
                        continue
                else:
                    empty_response_count = 0

                if message == "NONE":
                    self.send_message("QUIT")
                    break
                elif message == "QUIT":
                    break
                elif message == ".":
                    pass
                elif message == "OK":
                    self.send_message("REDY")
                elif message.startswith("JOBN") or message.startswith("JOBP"):
                    job = self.parse_job(message)
                    if job:
                        if self.should_refresh_servers(job):
                            servers = self.get_servers()
                            if servers:
                                self.servers_cached = True

                        self.scheduler.update_failed_assignments(self.failed_assignments)

                        server_type, server_id = self.scheduler.schedule_job_optimized(job)
                        self.send_message(f"SCHD {job.id} {server_type} {server_id}")

                        response = self.receive_message()

                        if not response:
                            continue

                        if response == ".":
                            response = self.receive_message()
                        elif response.startswith(".\n"):
                            lines = response.split('\n')
                            response = lines[1] if len(lines) > 1 else "OK"

                        if response == "OK":
                            self.send_message("REDY")
                        elif response.startswith("ERR:"):
                            failed_key = (job.id, server_type, server_id)
                            self.failed_assignments.add(failed_key)

                            retry_server_type, retry_server_id = self.scheduler.schedule_job_optimized(job)
                            self.send_message(f"SCHD {job.id} {retry_server_type} {retry_server_id}")

                            retry_response = self.receive_message()
                            if retry_response == ".":
                                retry_response = self.receive_message()
                            elif retry_response.startswith(".\n"):
                                lines = retry_response.split('\n')
                                retry_response = lines[1] if len(lines) > 1 else "OK"

                            if retry_response == "OK":
                                self.send_message("REDY")
                            else:
                                self.failed_assignments.add((job.id, retry_server_type, retry_server_id))
                                self.send_message("REDY")
                        else:
                            self.send_message("REDY")
                    else:
                        fallback_type = "small"
                        if self.scheduler.server_types:
                            preferred_types = ['small', 'medium', 'large', 'tiny', 'xlarge']
                            for pref_type in preferred_types:
                                if pref_type in self.scheduler.server_types:
                                    fallback_type = pref_type
                                    break
                            if fallback_type not in self.scheduler.server_types:
                                fallback_type = next(iter(self.scheduler.server_types))

                        self.send_message(f"SCHD 0 {fallback_type} 0")
                        response = self.receive_message()
                        if response == ".":
                            response = self.receive_message()
                        elif response.startswith(".\n"):
                            lines = response.split('\n')
                            response = lines[1] if len(lines) > 1 else "OK"

                        if response == "OK":
                            self.send_message("REDY")
                        else:
                            self.send_message("REDY")
                elif message.startswith("JCPL"):
                    if message.strip():
                        parts = message.split()
                        if len(parts) >= 4:
                            completed_job_id = int(parts[2])
                            self.failed_assignments = {
                                key for key in self.failed_assignments
                                if key[0] != completed_job_id
                            }

                    servers = self.get_servers()
                    if servers:
                        self.servers_cached = True

                    self.send_message("REDY")
                elif message.startswith("RESF") or message.startswith("RESR"):
                    self.servers_cached = False
                    self.force_refresh_next = True
                    self.send_message("REDY")
                else:
                    self.send_message("REDY")

        except KeyboardInterrupt:
            pass
        except Exception as e:
            pass
        finally:
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass


if __name__ == "__main__":
    port = 50000
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except ValueError:
            print("Error: Port number must be an integer")
            sys.exit(1)

    client = DSClient(port)
    client.run()