import socket
import sys


def send(sock, msg):
    sock.send((msg + "\n").encode())


def recv(sock):
    return sock.recv(2048).decode().strip()


def get_capable_servers(sock, job):
    req = f"GETS Capable {job['cores']} {job['mem']} {job['disk']}"
    send(sock, req)
    header = recv(sock)
    count = int(header.split()[1])
    send(sock, "OK")
    data = ""
    while len(data.strip().splitlines()) < count:
        data += recv(sock) + "\n"
    send(sock, "OK")
    recv(sock)
    return [line for line in data.strip().splitlines() if line != "."]


def parse_job(msg):
    parts = msg.split()
    return {
        'id': int(parts[1]),
        'submit': int(parts[2]),
        'cores': int(parts[3]),
        'mem': int(parts[4]),
        'disk': int(parts[5]),
        'est_runtime': int(parts[6])
    }


def classify_job(job):
    return 'short' if job['est_runtime'] < 500 else 'long'


def select_lsjf_server(servers, job):
    parsed = []
    for line in servers:
        parts = line.split()
        server = {
            'type': parts[0],
            'id': int(parts[1]),
            'state': parts[2],
            'start_time': int(parts[3]),
            'cores': int(parts[4]),
            'mem': int(parts[5]),
            'disk': int(parts[6]),
            'w': int(parts[7]),
            'r': int(parts[8]),
            'total_queue': int(parts[7]) + int(parts[8])
        }
        parsed.append(server)

    if not parsed:
        return "default", 0

    # Separate active and inactive servers, sort by resource size
    active_servers = sorted([s for s in parsed if s['state'] in ['active', 'idle']],
                            key=lambda s: s['cores'])
    inactive_servers = sorted([s for s in parsed if s['state'] in ['inactive', 'booting']],
                              key=lambda s: s['cores'])

    # Calculate server score (precise cost control)
    def calculate_server_score(server):
        # Base wait time
        if server['state'] == 'idle':
            wait_time = 0
        else:
            avg_time = 250 if job['est_runtime'] < 300 else 750
            wait_time = server['total_queue'] * avg_time

        # Precise resource matching score
        core_ratio = server['cores'] / max(job['cores'], 1)
        mem_ratio = server['mem'] / max(job['mem'], 1)
        disk_ratio = server['disk'] / max(job['disk'], 1)

        # Exclude if resources are insufficient
        if core_ratio < 1 or mem_ratio < 1 or disk_ratio < 1:
            return float('inf')

        core_efficiency = min(1.0, 1.0 / core_ratio)
        mem_efficiency = min(1.0, 1.0 / mem_ratio)
        disk_efficiency = min(1.0, 1.0 / disk_ratio)
        efficiency_bonus = (core_efficiency + mem_efficiency + disk_efficiency) * 50

        # Resource waste penalty (more precise)
        core_waste = max(0, core_ratio - 1) ** 1.5
        resource_waste = core_waste * 300 + max(0, mem_ratio - 1) * 80 + max(0, disk_ratio - 1) * 80

        # Server cost factor (based on actual configuration)
        if server['cores'] >= 16:  # xlarge
            cost_factor = 3.5
        elif server['cores'] >= 8:  # large
            cost_factor = 2.5
        elif server['cores'] >= 4:  # medium
            cost_factor = 1.8
        else:  # small
            cost_factor = 1.0

        # State penalty (considering job characteristics)
        if server['state'] == 'idle':
            state_penalty = 0
        elif server['state'] == 'active':
            state_penalty = 30 + server['total_queue'] * 20
        elif server['state'] == 'booting':
            state_penalty = 600 * cost_factor
        else:  # inactive
            state_penalty = 1200 * cost_factor

        # Total score = completion time + cost penalty - efficiency bonus
        completion_time = wait_time + job['est_runtime']
        total_penalty = resource_waste + state_penalty - efficiency_bonus

        return completion_time + total_penalty

    # Prefer to find the best match among active servers
    best_server = None
    best_score = float('inf')

    # First Fit: find the smallest active server that meets requirements
    for server in active_servers:
        # Quick filter: ensure resources are sufficient
        if (server['cores'] >= job['cores'] and
                server['mem'] >= job['mem'] and
                server['disk'] >= job['disk']):

            score = calculate_server_score(server)
            if score < best_score:
                best_score = score
                best_server = server

            # If a very good match is found, use it immediately (reduce computation)
            if (server['state'] == 'idle'):
                break

    # If active servers are not good enough, consider inactive servers
    if best_score > 3500:  # Adjust threshold
        for server in inactive_servers:
            if (server['cores'] >= job['cores'] and
                    server['mem'] >= job['mem'] and
                    server['disk'] >= job['disk']):

                score = calculate_server_score(server)
                if score < best_score:
                    best_score = score
                    best_server = server

    if best_server:
        return best_server['type'], best_server['id']
    else:
        maxest = max(parsed, key=lambda s: s['cores'])
        return maxest['type'], maxest['id']


def main():
    port = 50000
    if len(sys.argv) > 1:
        port = int(sys.argv[1])

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", port))

    send(sock, "HELO")
    recv(sock)
    send(sock, "AUTH student")
    recv(sock)

    send(sock, "REDY")
    job_queue = []

    while True:
        msg = recv(sock)
        if msg == "NONE":
            send(sock, "QUIT")
            recv(sock)
            break
        elif msg.startswith("JOBN"):
            job = parse_job(msg)
            job_queue.append(job)

            # Smart sorting: balance resource efficiency and turnaround time
            def job_priority(x):
                # Base priority: short jobs first
                time_priority = min(x['est_runtime'], 600)

                # Resource compactness: prefer jobs with smaller resource needs
                resource_compactness = x['cores'] + x['mem'] // 4000 + x['disk'] // 32000

                # Prefer jobs that can be handled by small servers
                small_server_friendly = 0
                if x['cores'] <= 2 and x['mem'] <= 4000:
                    small_server_friendly = -200  # Bonus
                elif x['cores'] <= 4 and x['mem'] <= 16000:
                    small_server_friendly = -100  # Small bonus

                return (time_priority, resource_compactness, small_server_friendly, x['submit'])

            job_queue.sort(key=job_priority)

            # Smart scheduling strategy: decide based on queue state and job characteristics
            scheduled_count = 0

            # Dynamically decide how many jobs to schedule
            if len(job_queue) >= 6:  # When queue is long
                if job['est_runtime'] < 120 and job['cores'] <= 2:  # Small job
                    max_schedule = min(3, len(job_queue))
                elif job['est_runtime'] < 300:  # Medium-short job
                    max_schedule = min(2, len(job_queue))
                else:
                    max_schedule = 2
            elif len(job_queue) >= 3:  # Medium queue
                if job['est_runtime'] < 60 and job['cores'] <= 2:  # Very short small job
                    max_schedule = min(2, len(job_queue))
                else:
                    max_schedule = 2
            else:  # Short queue, conservative scheduling
                max_schedule = 1

            while job_queue and scheduled_count < max_schedule:
                current_job = job_queue.pop(0)
                servers = get_capable_servers(sock, current_job)

                if servers:
                    stype, sid = select_lsjf_server(servers, current_job)
                    send(sock, f"SCHD {current_job['id']} {stype} {sid}")
                    recv(sock)
                    scheduled_count += 1
                else:
                    # Put back to the end of the queue, retry later
                    job_queue.append(current_job)
                    break

            send(sock, "REDY")
        elif msg.startswith("JCPL"):
            send(sock, "REDY")
        elif msg == "OK":
            continue


if __name__ == '__main__':
    main()
