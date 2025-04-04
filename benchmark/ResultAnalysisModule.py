from os import path
import time
from typing import Dict, List, Optional
from dataclasses import dataclass
from GlobalDefs import * 

@dataclass
class ClientEvent:
    client_id: str
    timestamp: float
    connected: bool  

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
    message_id: Optional[str] = None  

@dataclass
class ReceivedMessage:
    client_id: str
    timestamp: float
    topic: str
    purpose: str
    message_id: Optional[str] = None 
    pub_timestamp: Optional[float] = None 

@dataclass
class ClientStatus:
    client_id: str
    is_online: bool
    last_connect: float
    last_disconnect: float
    total_online_time: float = 0

class BenchmarkAnalyzer:
    def __init__(self):
        self.client_events: List[ClientEvent] = []
        self.subscriptions: List[Subscription] = []
        self.publications: List[Publication] = []
        self.received_messages: List[ReceivedMessage] = []
        self.client_status: Dict[str, ClientStatus] = {}

    def process_log_file(self, log_file_path: str) -> None:
        """Process a single log file and populate data structures."""
        with open(log_file_path, 'r') as f:
            for line in f:
                try:
                    parts = line.strip().split()
                    if len(parts) < 3:
                        continue
                    
                    timestamp = float(parts[0])
                    event_type = parts[1]
                    data = " ".join(parts[2:])

                    if event_type == "CONNECT":
                        client_id = data
                        self.client_events.append(ClientEvent(client_id, timestamp, True))
                        self._update_client_status(client_id, True, timestamp)

                    elif event_type == "DISCONNECT":
                        client_id = data
                        self.client_events.append(ClientEvent(client_id, timestamp, False))
                        self._update_client_status(client_id, False, timestamp)

                    elif event_type == "SUBSCRIBE":
                        client_id, topic_filter, purpose_filter = data.split(" ", 2)
                        self.subscriptions.append(Subscription(client_id, timestamp, topic_filter, purpose_filter))

                    elif event_type == "PUBLISH":
                        # Current format: "client_id topic purpose" (message_id to be added later)
                        client_id, topic, purpose = data.split(" ", 2)
                        self.publications.append(Publication(client_id, timestamp, topic, purpose))

                    elif event_type == "RECEIVE":
                        # Current format: "client_id topic purpose" (message_id to be added later)
                        client_id, topic, purpose = data.split(" ", 2)
                        self.received_messages.append(ReceivedMessage(client_id, timestamp, topic, purpose))

                except (ValueError, IndexError) as e:
                    print(f"Error parsing line in {log_file_path}: {line.strip()} - {e}")

    def _update_client_status(self, client_id: str, connected: bool, timestamp: float) -> None:
        """Update client online/offline status and calculate online time."""
        if client_id not in self.client_status:
            self.client_status[client_id] = ClientStatus(client_id, False, 0, 0)
        
        status = self.client_status[client_id]
        
        if connected:
            status.is_online = True
            status.last_connect = timestamp
            if status.last_disconnect > status.last_connect:
                status.total_online_time += timestamp - status.last_disconnect
        else:
            status.is_online = False
            status.last_disconnect = timestamp
            if status.last_connect > status.last_disconnect:
                status.total_online_time += timestamp - status.last_connect

    def calculate_metrics(self) -> Dict:
        """Calculate basic metrics (latency will be added with message IDs)."""
        metrics = {
            "total_clients": len(self.client_status),
            "online_clients": sum(1 for s in self.client_status.values() if s.is_online),
            "total_online_time": sum(s.total_online_time for s in self.client_status.values()),
            "total_subscriptions": len(self.subscriptions),
            "total_publications": len(self.publications),
            "total_received": len(self.received_messages),
            "avg_latency": 0.0  # Placeholder until message IDs are available
        }
        return metrics

def analyze_results(log_dir: str, out_file: Optional[str] = None) -> None:
    """
    Analyze benchmark log files and write results for benchmark.py's analyze command.
    
    Args:
        log_dir (str): Directory containing log files
        out_file (str, optional): Output file path for results
    """
    if not path.isdir(log_dir):
        print(f"Error: {log_dir} is not a directory")
        return

    if out_file is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        out_file = path.join(log_dir, f"BenchmarkResults_{timestring}.txt")

    analyzer = BenchmarkAnalyzer()
    log_files = [f for f in path.listdir(log_dir) if f.endswith('.log')]

    for log_file in log_files:
        analyzer.process_log_file(path.join(log_dir, log_file))

    metrics = analyzer.calculate_metrics()

    with open(out_file, 'w') as f:
        f.write(f"Analysis completed on {time.ctime()}\n")
        f.write(f"Log files processed: {len(log_files)}\n")
        f.write(f"Total clients: {metrics['total_clients']}\n")
        f.write(f"Online clients: {metrics['online_clients']}\n")
        f.write(f"Total online time: {metrics['total_online_time']:.2f} seconds\n")
        f.write(f"Subscriptions: {metrics['total_subscriptions']}\n")
        f.write(f"Publications: {metrics['total_publications']}\n")
        f.write(f"Received messages: {metrics['total_received']}\n")
        f.write("Average latency: Not available (message IDs pending)\n")

    print(f"Results written to {out_file}")

if __name__ == "__main__":
    analyze_results("./logs")
