from datetime import datetime

def parse_log_file(file_path):
    publish_events = {}
    latencies = []

    with open(file_path, 'r') as f:
        for line in f:
            parts = line.strip().split()
            timestamp_str = f"{parts[0]} {parts[1]}"
            event_type = parts[2]
            msg_id = parts[3].split('=')[1]

            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S.%f')

            if event_type == 'PUBLISH':
                publish_events[msg_id] = timestamp
            elif event_type == 'RECV' and msg_id in publish_events:
                latency = (timestamp - publish_events[msg_id]).total_seconds()
                latencies.append(latency)
                del publish_events[msg_id]  # Remove matched event to save memory

    return latencies

def calculate_average_latency(latencies):
    if not latencies:
        return 0.0
    return sum(latencies) / len(latencies)

# Example usage
file_path = 'benchmark_output.log'
latencies = parse_log_file(file_path)
avg_latency = calculate_average_latency(latencies)
print(f"Average latency: {avg_latency:.3f} seconds")
print(f"Number of matched messages: {len(latencies)}")
