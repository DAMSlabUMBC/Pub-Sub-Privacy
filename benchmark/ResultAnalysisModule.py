from os import path
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from GlobalDefs import *  # Assuming this contains constants or types you need

# Define data structures to hold the log data
@dataclass
class ClientEvent:
    client_id: str
    timestamp: float
    connected: bool  # True for connect, False for disconnect

@dataclass
class Subscription:
    client_id: str
    timestamp: float
    topic_filter: str
    purpose_filter: str

@dataclass
class Publication:
    client_id: str
    timestamp: float
    topic: str
    purpose: str

@dataclass
class ReceivedMessage:
    client_id: str
    timestamp: float
    topic: str
    purpose: str

def analyze_results(log_dir: str, out_file: Optional[str] = None) -> None:
    """
    Analyze benchmark log files and extract client events, subscriptions, publications, and received messages.
    
    Args:
        log_dir (str): Directory containing log files
        out_file (str, optional): Output file path for results. Defaults to timestamped file.
    """
    # Create result file path if it doesn't exist
    if out_file is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        out_file = path.join(log_dir, f"BenchmarkResults_{timestring}.txt")

    # Data storage
    client_events: List[ClientEvent] = []
    subscriptions: List[Subscription] = []
    publications: List[Publication] = []
    received_messages: List[ReceivedMessage] = []

    # TODO: Replace with actual log file discovery logic
    log_files = [f for f in path.listdir(log_dir) if f.endswith('.log')]

    for log_file in log_files:
        full_path = path.join(log_dir, log_file)
        with open(full_path, 'r') as f:
            for line in f:
                # Parse log line (assuming a format like: "TIMESTAMP EVENT_TYPE DATA")
                # This is a placeholder - adjust based on your actual log format
                try:
                    parts = line.strip().split()
                    if len(parts) < 3:
                        continue  # Skip malformed lines
                    
                    timestamp = float(parts[0])  # Assuming timestamp is first
                    event_type = parts[1]
                    data = " ".join(parts[2:])

                    # Process based on event type
                    if event_type == "CONNECT":
                        client_id = data  # Assuming data is client_id
                        client_events.append(ClientEvent(client_id, timestamp, True))
                    
                    elif event_type == "DISCONNECT":
                        client_id = data
                        client_events.append(ClientEvent(client_id, timestamp, False))
                    
                    elif event_type == "SUBSCRIBE":
                        # Assuming data format: "client_id topic_filter purpose_filter"
                        client_id, topic_filter, purpose_filter = data.split(" ", 2)
                        subscriptions.append(Subscription(client_id, timestamp, topic_filter, purpose_filter))
                    
                    elif event_type == "PUBLISH":
                        # Assuming data format: "client_id topic purpose"
                        client_id, topic, purpose = data.split(" ", 2)
                        publications.append(Publication(client_id, timestamp, topic, purpose))
                    
                    elif event_type == "RECEIVE":
                        # Assuming data format: "client_id topic purpose"
                        client_id, topic, purpose = data.split(" ", 2)
                        received_messages.append(ReceivedMessage(client_id, timestamp, topic, purpose))
                
                except (ValueError, IndexError) as e:
                    print(f"Error parsing line in {log_file}: {line.strip()} - {e}")
                    continue

    # Print summary (for now - later we'll calculate metrics)
    print(f"Processed {len(log_files)} log files")
    print(f"Client events: {len(client_events)}")
    print(f"Subscriptions: {len(subscriptions)}")
    print(f"Publications: {len(publications)}")
    print(f"Received messages: {len(received_messages)}")

    # Write results to file
    with open(out_file, 'w') as f:
        f.write(f"Analysis completed on {time.ctime()}\n")
        f.write(f"Log files processed: {len(log_files)}\n")
        f.write(f"Client events: {len(client_events)}\n")
        f.write(f"Subscriptions: {len(subscriptions)}\n")
        f.write(f"Publications: {len(publications)}\n")
        f.write(f"Received messages: {len(received_messages)}\n")

    # TODO: Add correctness, latency, and throughput calculations here

    print(f"Results written to {out_file}")

if __name__ == "__main__":
    # Example usage
    analyze_results("./logs")
