import sys
from os import path
import time
from io import TextIOWrapper
from paho.mqtt.client import topic_matches_sub as paho_topic_matches_sub
from pathlib import Path
from sortedcontainers import SortedDict
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import GlobalDefs
import LoggingModule
from rich.progress import Progress
from rich.progress import TextColumn
from rich.progress import BarColumn
from rich.progress import TaskProgressColumn
from rich.progress import SpinnerColumn

# TODO REMOVE
# @dataclass
# class ConnectEvent:
#     client_id: str
#     timestamp: float 
    
# @dataclass
# class DisconnectEvent:
#     client_id: str
#     timestamp: float

@dataclass(frozen=True)
class SubscribeEvent:
    client_id: str
    timestamp: float
    topic_filter: str
    purpose_filter: str
    sub_id: int

@dataclass(frozen=True)
class PublishEvent:
    client_id: str
    timestamp: float
    topic: str
    purpose: str
    message_type: str
    correlation_id: int

@dataclass(frozen=True)
class RecvMessageEvent:
    recv_client_id: str
    sending_client_id: str
    timestamp: float
    topic: str
    message_type: str
    correlation_id: int
    sub_id: int

@dataclass(frozen=True)
class ClientStatus:
    client_id: str
    is_online: bool
    online_since_timestamp: float
    
class ClientLatencyResults:
    total_latency: float
    recv_message_count: int
    
    def __init__(self):
        self.total_latency = 0.0
        self.recv_message_count = 0
    
    def add_message_to_client_latency(self, latency: float) -> None:
        self.total_latency += latency
        self.recv_message_count += 1
    
    def get_average_latency_for_client(self) -> float:
        return (self.total_latency / self.recv_message_count)

       
class TestLatencyResults:
    latency_by_client: SortedDict[str, ClientLatencyResults]
    
    def __init__(self):
        self.latency_by_client = SortedDict()
    
    def add_message_to_test_latency(self, client: str, latency: float) -> None:
        if client not in self.latency_by_client:
            self.latency_by_client[client] = ClientLatencyResults()
        self.latency_by_client[client].add_message_to_client_latency(latency)
        
    def get_client_count(self) -> int:
        return len(self.latency_by_client)
    
    def get_message_count(self) -> int:
        total_messages: int = 0
        for client in self.latency_by_client:
            client_latency_result = self.latency_by_client[client]
            total_messages = total_messages + client_latency_result.recv_message_count
        return total_messages
    
    def get_average_latency(self) -> float: 
        total_latency: float = 0.0
        for client in self.latency_by_client:
            client_latency_result = self.latency_by_client[client]
            total_latency = total_latency + client_latency_result.get_average_latency_for_client()
        return (total_latency / self.get_client_count())
    
    
class ClientThroughputResults:
    msg_count_per_second: List[int]
    
    def __init__(self):
        self.msg_count_per_second = list()
    
    def add_msgs_per_second_count(self, messages_sent_in_one_sec: int) -> None:
        self.msg_count_per_second.append(messages_sent_in_one_sec)
        
    def get_total_interval_length_seconds(self) -> int:
        return (len(self.msg_count_per_second))
    
    def get_average_throughput_per_second_for_client(self) -> float:
        average = sum(self.msg_count_per_second) / len(self.msg_count_per_second)
        return average
    
    def get_total_messages(self) -> int:
        return sum(self.msg_count_per_second)
    
    
class TestThroughputResults:
    throughput_by_client: SortedDict[str, ClientThroughputResults]
    
    def __init__(self):
        self.throughput_by_client = SortedDict()
    
    def add_msgs_per_second_count(self, client: str, messages_sent_in_one_sec: int) -> None:
        if client not in self.throughput_by_client:
            self.throughput_by_client[client] = ClientThroughputResults()
        self.throughput_by_client[client].add_msgs_per_second_count(messages_sent_in_one_sec)
        
    def get_client_count(self) -> int:
        return len(self.throughput_by_client)
    
    def get_average_throughput_per_second(self) -> float: 
        total_throughput: float = 0.0
        for client in self.throughput_by_client:
            client_throuhgput_result = self.throughput_by_client[client]
            total_throughput = total_throughput + client_throuhgput_result.get_average_throughput_per_second_for_client()
        return (total_throughput / self.get_client_count())    
   
    
class ClientPBACCorrectnessResults:
    properly_matched: int
    improperly_matched: int
    not_matched: int
    
    def __init__(self):
        self.properly_matched = 0
        self.improperly_matched = 0
        self.not_matched = 0
        
    def add_pbac_correctness_metrics(self, properly_matched: int, improperly_matched: int, not_matched: int) -> None:
        self.properly_matched += properly_matched
        self.improperly_matched += improperly_matched
        self.not_matched += not_matched
    
    def get_total_messages(self) -> int:
        return self.properly_matched + self.improperly_matched + self.not_matched
        
    
class TestPBACCorrectnessResults:
    pbac_correctness_by_client: SortedDict[str, ClientPBACCorrectnessResults]
    
    def __init__(self):
        self.pbac_correctness_by_client = SortedDict()
    
    def add_pbac_correctness_metrics(self, client: str, properly_matched: int, improperly_matched: int, not_matched: int) -> None:
        if client not in self.pbac_correctness_by_client:
            self.pbac_correctness_by_client[client] = ClientPBACCorrectnessResults()
        self.pbac_correctness_by_client[client].add_pbac_correctness_metrics(properly_matched, improperly_matched, not_matched)

    def get_client_count(self) -> int:
        return len(self.pbac_correctness_by_client)
    
    def get_total_correctness(self) -> Tuple[int, int, int, int]: 
        total_properly_matched: int = 0
        total_improperly_matched: int = 0
        total_not_matched: int = 0
        for client in self.pbac_correctness_by_client:
            client_pbac_result:ClientPBACCorrectnessResults = self.pbac_correctness_by_client[client]
            total_properly_matched += client_pbac_result.properly_matched
            total_improperly_matched += client_pbac_result.improperly_matched
            total_not_matched += client_pbac_result.not_matched
        total_messages = total_properly_matched + total_improperly_matched + total_not_matched  
        
        return total_properly_matched, total_improperly_matched, total_not_matched, total_messages
    

class ResultsAnalyzer:
    
    # Setup progress bar
    correlation_progress = Progress(
        TextColumn("[red][progress.description]{task.description}"),
        SpinnerColumn(),
        BarColumn(),
        TaskProgressColumn()
    )
    
    purpose_management_method: str | None
    file_to_seed_mapping: Dict[str, int]
    
    # TODO REMOVE
    # Each of this dictionaries is a map of timestamp -> list of events happening at that timestamp
    #all_connect_events: SortedDict[float, List[ConnectEvent]] = SortedDict()
    #all_disconnect_events: SortedDict[float, List[DisconnectEvent]] = SortedDict()
    #all_subscribe_events: SortedDict[float, List[SubscribeEvent]] = SortedDict()
    #all_recv_events: SortedDict[float, List[RecvMessageEvent]] = SortedDict()
    
    all_publish_events: SortedDict[float, List[PublishEvent]]
    all_client_statuses: Dict[str, SortedDict[float, ClientStatus]]
    subs_regged_by_client: Dict[str, SortedDict[float, SubscribeEvent]]
    msgs_sent_by_client: Dict[str, SortedDict[float, List[PublishEvent]]]
    msgs_recv_by_client: Dict[str, SortedDict[float, List[RecvMessageEvent]]]
    
    pub_to_subscriptions_mapping: Dict[PublishEvent, List[SubscribeEvent]]
    pub_to_recv_mapping: Dict[PublishEvent, List[RecvMessageEvent]]
    
    pub_event_count: int
    data_pub_event_count: int
    op_pub_event_count: int
    
    # == Result structures ==
    test_latency: TestLatencyResults
    test_throughput: TestThroughputResults
    test_pbac_correctness: TestPBACCorrectnessResults
    
    def __init__(self):
        self.purpose_management_method = None
        self.file_to_seed_mapping = dict()
        
        # TODO REMOVE
        # Each of this dictionaries is a map of timestamp -> list of events happening at that timestamp
        #all_connect_events: SortedDict[float, List[ConnectEvent]] = SortedDict()
        #all_disconnect_events: SortedDict[float, List[DisconnectEvent]] = SortedDict()
        #all_subscribe_events: SortedDict[float, List[SubscribeEvent]] = SortedDict()
        #all_recv_events: SortedDict[float, List[RecvMessageEvent]] = SortedDict()
        
        self.all_publish_events = SortedDict()
        self.all_client_statuses = dict()
        self.subs_regged_by_client = dict()
        self.msgs_sent_by_client = dict()
        self.msgs_recv_by_client = dict()
        
        self.pub_to_subscriptions_mapping = dict()
        self.pub_to_recv_mapping = dict()
        
        self.pub_event_count = 0
        self.data_pub_event_count = 0
        self.op_pub_event_count = 0
        
        # == Result structures ==
        self.test_latency = TestLatencyResults()
        self.test_throughput = TestThroughputResults()
        self.test_pbac_correctness = TestPBACCorrectnessResults()
        
    
    def parse_log_directory(self, log_directory):
        
        # Verify directory exists
        log_dir_path = Path(log_directory)
        if not log_dir_path.exists() or not log_dir_path.is_dir():
            print(f"Directory {log_directory} not found")
            sys.exit(GlobalDefs.ExitCode.BAD_ARGUMENT)
            
        # Parse all log files for events
        log_files = log_dir_path.rglob('*.log')
        found_file = False
        for log_file in log_files:
            found_file = True
            self.parse_log_file(log_file)
            
        if not found_file:
            print(f"No log files found in {log_directory}")
            sys.exit(GlobalDefs.ExitCode.BAD_ARGUMENT)


    def parse_log_file(self, log_file_path: Path) -> None:
        """Process a single log file and populate data structures."""
        with open(log_file_path, 'r') as f:
            
            allowed_types = [LoggingModule.SEED_LABEL, LoggingModule.PM_METHOD_LABEL, LoggingModule.CONNECT_LABEL, LoggingModule.DISCONNECT_LABEL, LoggingModule.SUBSCRIBE_LABEL,
                             LoggingModule.PUBLISH_LABEL, LoggingModule.OP_PUBLISH_LABEL, LoggingModule.RECV_LABEL, LoggingModule.OP_RECV_LABEL]
            
            # Initalize variables
            last_client_status: ClientStatus | None = None
            line_index = 0
            
            # Parse each line
            for line in f:
                line_index += 1
                
                try:
                    parts = line.strip().split(LoggingModule.SEPARATOR)
                    parts_len = len(parts)
                    
                    # Skip empty lines
                    if parts_len == 0:
                        continue
                    
                    # Ensure the line is a known type
                    line_type = parts[0]
                    if line_type not in allowed_types:
                        print(f"Unknown line type {line_type} found in file {log_file_path}. Aborting.")
                        sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                    # Parse the line
                    if line_type == LoggingModule.SEED_LABEL:
                        
                        if parts_len != 2:
                            print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                        # There should only be one of these
                        if log_file_path.name in self.file_to_seed_mapping:
                            print(f"Multiple seeds defined in {log_file_path}. File corrupted. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        else:
                            self.file_to_seed_mapping[log_file_path.name] = int(parts[1])
                            
                    elif line_type == LoggingModule.PM_METHOD_LABEL:
                        
                        if parts_len != 2:
                            print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                        # There can only be one method across all files
                        if self.purpose_management_method == None:
                            self.purpose_management_method = parts[1]
                        elif self.purpose_management_method != parts[1]:
                            print(f"Multiple purpose management methods defined across the input log files. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.CONFLICTING_LOG_FILES)
                    
                    elif line_type == LoggingModule.CONNECT_LABEL:
                        
                        if parts_len != 4:
                            print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                        # Create the event and add it to structures
                        timestamp = float(parts[1])
                        client_id = parts[3]

                        # Add the event
                        # TODO REMOVE
                        # con_event = ConnectEvent(client_id, timestamp)
                        # if timestamp not in self.all_connect_events:
                        #     self.all_connect_events[timestamp] = list()
                        # self.all_connect_events[timestamp].append(con_event)
                        
                        # We also want to add the client status
                        # Check "online since" timestamp
                        if last_client_status is not None and last_client_status.is_online:
                            # Carry timestamp over, there was no disconnect
                            online_since_timestamp = last_client_status.online_since_timestamp 
                        else:
                            # This is a new connect
                            online_since_timestamp = timestamp        
                        
                        client_status = ClientStatus(client_id, True, online_since_timestamp)
                        
                        if client_id not in self.all_client_statuses:
                            self.all_client_statuses[client_id] = SortedDict()
                        self.all_client_statuses[client_id][timestamp] = client_status
                        
                        # We want to keep track of the last status for disconnect tracking
                        last_client_status = client_status
                        
                    elif line_type == LoggingModule.DISCONNECT_LABEL:
                        
                        if parts_len != 4:
                            print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                        # Create the event and add it to structures
                        timestamp = float(parts[1])
                        client_id = parts[3]

                        # Add the event
                        # TODO REMOVE
                        # discon_event = DisconnectEvent(client_id, timestamp)
                        # if timestamp not in self.all_disconnect_events:
                        #     self.all_disconnect_events[timestamp] = list()
                        # self.all_disconnect_events[timestamp].append(discon_event)
                        
                        # We also want to add the client status
                        # Note that "offline" statuses don't need a "online since" they are not online
                        client_status = ClientStatus(client_id, False, 0)
                        
                        if client_id not in self.all_client_statuses:
                            self.all_client_statuses[client_id] = SortedDict()
                        self.all_client_statuses[client_id][timestamp] = client_status
                        
                        # We want to keep track of the last status for disconnect tracking
                        last_client_status = client_status
                        
                    elif line_type == LoggingModule.SUBSCRIBE_LABEL:
                        
                        if parts_len != 7:
                            print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                        # Create the event and add it to structures
                        timestamp = float(parts[1])
                        client_id = parts[3]
                        topic_filter = parts[4]
                        purpose_filter = parts[5]
                        sub_id = int(parts[6])
                        sub_event = SubscribeEvent(client_id, timestamp, topic_filter, purpose_filter, sub_id)

                        # Add the event
                        # TODO REMOVE
                        # if timestamp not in self.all_subscribe_events:
                        #     self.all_subscribe_events[timestamp] = list()
                        # self.all_subscribe_events[timestamp].append(sub_event)
                        
                        # Also add to subs regged
                        if client_id not in self.subs_regged_by_client:
                            self.subs_regged_by_client[client_id] = SortedDict()
                        if timestamp not in self.subs_regged_by_client[client_id]:
                            self.subs_regged_by_client[client_id][timestamp] = list()
                        self.subs_regged_by_client[client_id][timestamp].append(sub_event)
                    
                    elif line_type == LoggingModule.PUBLISH_LABEL or line_type == LoggingModule.OP_PUBLISH_LABEL:
                        
                        if parts_len != 8:
                            print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                        # Create the event and add it to structures
                        timestamp = float(parts[1])
                        client_id = parts[3]
                        topic = parts[4]
                        purpose = parts[5]
                        msg_type = parts[6]
                        corr_data = int(parts[7])
                        pub_event = PublishEvent(client_id, timestamp, topic, purpose, msg_type, corr_data)

                        # Add the event
                        if timestamp not in self.all_publish_events:
                            self.all_publish_events[timestamp] = list()
                        self.all_publish_events[timestamp].append(pub_event)
                        
                        # Also add to messages sent by client
                        if client_id not in self.msgs_sent_by_client:
                            self.msgs_sent_by_client[client_id] = SortedDict()
                        if timestamp not in self.msgs_sent_by_client[client_id]:
                            self.msgs_sent_by_client[client_id][timestamp] = list()
                        self.msgs_sent_by_client[client_id][timestamp].append(pub_event)
                        
                        # # Update metrics
                        self.pub_event_count += 1
                        if line_type == LoggingModule.PUBLISH_LABEL:
                            self.data_pub_event_count += 1
                        elif line_type == LoggingModule.OP_PUBLISH_LABEL:
                            self.op_pub_event_count += 1
                        
                    elif line_type == LoggingModule.RECV_LABEL or line_type == LoggingModule.OP_RECV_LABEL:
                              
                        if parts_len != 9:
                            print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                            sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                        
                        # Create the event and add it to structures
                        timestamp = float(parts[1])
                        recv_client_id = parts[3]
                        send_client_id = parts[4]
                        topic = parts[5]
                        sub_id = int(parts[6])
                        msg_type = parts[7]
                        corr_data = int(parts[8])
                        recv_event = RecvMessageEvent(recv_client_id, send_client_id, timestamp, topic, msg_type, corr_data, sub_id)

                        # Add the event
                        # TODO REMOVE
                        # if timestamp not in self.all_recv_events:
                        #     self.all_recv_events[timestamp] = list()
                        # self.all_recv_events[timestamp].append(recv_event)
                        
                        # Also add to messages received by client
                        if recv_client_id not in self.msgs_recv_by_client:
                            self.msgs_recv_by_client[recv_client_id] = SortedDict()
                        if timestamp not in self.msgs_recv_by_client[recv_client_id]:
                            self.msgs_recv_by_client[recv_client_id][timestamp] = list()
                        self.msgs_recv_by_client[recv_client_id][timestamp].append(recv_event)                

                except Exception as e:
                    print(f"Improper format for line {line_index} found in file {log_file_path}. Aborting.")
                    sys.exit(GlobalDefs.ExitCode.MALFORMED_LOG_FILE)
                    

    def correlate_messages(self) -> None:
        
        with self.correlation_progress:
            correlation_task = self.correlation_progress.add_task("Correlating Messages", total=len(self.all_publish_events))
            for pub_timestamp in self.all_publish_events:
                pub_events_at_time = self.all_publish_events[pub_timestamp]
                pub_event: PublishEvent
                for pub_event in pub_events_at_time:
                    
                    for recv_client in self.msgs_recv_by_client:            
                        recv_events_for_client = self.msgs_recv_by_client[recv_client]
                        
                        # Start by correlating recv events with send events
                        # We only care about messages after the pub_event
                        possible_recv_events = {x: recv_events_for_client[x] for x in recv_events_for_client.keys() if x >= pub_timestamp}
                        self._correlate_pub_to_client_recv(pub_event, possible_recv_events)

                        # Now correlate pub events with the clients that should have received the messages   
                        # If there are no subscriptions for this client, we couldn't have recv'ed
                        if recv_client not in self.subs_regged_by_client:
                            continue
                        
                        # Check if the client was connected when the event happened
                        status_at_pub = self._get_client_status_at_pub_time(pub_event, recv_client)

                        # The client could have only recv'ed the message if it was connected
                        if status_at_pub is None or not status_at_pub.is_online:
                            continue
                        
                        # If the client was online, check for an active subscription for the pubbed message
                        # Find the time range the subscription must have occured in 
                        earliest_sub_time = status_at_pub.online_since_timestamp
                        latest_sub_time = pub_timestamp
                        
                        # Find all possible subscriptions from this client and correlate them
                        sub_events_for_client = self.subs_regged_by_client[recv_client]
                        possible_sub_events = {x: sub_events_for_client[x] for x in sub_events_for_client.keys() if x <= latest_sub_time and (earliest_sub_time == None or x >= earliest_sub_time)}
                        self._correlate_pub_to_client_subs(pub_event, possible_sub_events)
                        
                self.correlation_progress.update(correlation_task, advance=1)
                        
                       
    def _correlate_pub_to_client_recv(self, pub_event: PublishEvent, recv_events_by_timestamp: SortedDict[float, List[RecvMessageEvent]]) -> None:

        # Store event data
        pub_sending_client = pub_event.client_id
        pub_corr_data = pub_event.correlation_id
        pub_message_type = pub_event.message_type
        pub_topic = pub_event.topic
        
        for recv_event_list in recv_events_by_timestamp.values():
            for recv_event in recv_event_list:
                
                # Store event data
                recv_sending_client = recv_event.sending_client_id
                recv_corr_data = recv_event.correlation_id
                recv_message_type = recv_event.message_type
                recv_topic = recv_event.topic
                
                # Check for correlation
                if pub_sending_client == recv_sending_client and pub_corr_data == recv_corr_data and pub_message_type == recv_message_type and pub_topic == recv_topic:
                                        
                    # Add to mapping
                    if pub_event not in self.pub_to_recv_mapping:
                        self.pub_to_recv_mapping[pub_event] = list()
                    self.pub_to_recv_mapping[pub_event].append(recv_event)
                    
                    
    def _get_client_status_at_pub_time(self, pub_event: PublishEvent, client_to_check_id: str) -> ClientStatus | None:
        
        status_at_pub: ClientStatus | None = None
        
        # Look for the events covering the timestamp of the published message
        if client_to_check_id in self.all_client_statuses:
            recv_client_statuses = self.all_client_statuses[client_to_check_id]
            for status_timestamp in recv_client_statuses:
                
                # Keep track of the last status we saw
                if status_timestamp < pub_event.timestamp:
                    status_at_pub = recv_client_statuses[status_timestamp]
                    continue
                
                # When we pass the timestamp, we found the status during which the message was sent
                if status_timestamp >= pub_event.timestamp:
                    break
                
        return status_at_pub
     
    
    def _correlate_pub_to_client_subs(self, pub_event: PublishEvent, sub_events_by_timestamp: SortedDict[float, List[SubscribeEvent]]) -> None:
        
        # Store event data
        pub_topic = pub_event.topic
        
        for sub_event_list in sub_events_by_timestamp.values():
            sub_event: SubscribeEvent
            for sub_event in sub_event_list:
                
                # Store event data
                sub_topic_filter = sub_event.topic_filter
                
                # Need to determine if topics are compatible (purposes will be checked for PBAC correctness)
                topic_valid = paho_topic_matches_sub(sub_topic_filter, pub_topic)

                # Check if the sub would've recv'ed this message
                if topic_valid:
                    
                    # Add to map
                    if pub_event not in self.pub_to_subscriptions_mapping:
                        self.pub_to_subscriptions_mapping[pub_event] = list()
                    self.pub_to_subscriptions_mapping[pub_event].append(sub_event)
                    

    def calculate_latency(self) -> None:

        # Calculate the latency for each correlated message
        for pub_event in self.pub_to_recv_mapping:
            for recv_event in self.pub_to_recv_mapping[pub_event]:
                message_latency = recv_event.timestamp - pub_event.timestamp
                self.test_latency.add_message_to_test_latency(recv_event.recv_client_id, message_latency)
                
                
    def calculate_throughput(self) -> None:
        
        # Determine how many messages are recieved by a client in a second
        for client in self.msgs_recv_by_client:
            timestamp_to_recv_mapping = self.msgs_recv_by_client[client]
            
            # Get per second values
            curr_timestamp = timestamp_to_recv_mapping.keys()[0]
            final_timestamp = timestamp_to_recv_mapping.keys()[-1]
            
            while curr_timestamp <= final_timestamp:
                interval_start = curr_timestamp
                interval_end = curr_timestamp + 1.0
                
                # Get all recv events within this range
                timestamps_in_interval = {x: timestamp_to_recv_mapping[x] for x in timestamp_to_recv_mapping.keys() if x >= interval_start and x < interval_end}
                events_in_interval = 0
                for event_list in timestamps_in_interval.values():
                    events_in_interval = events_in_interval + len(event_list)
                    
                # Add all events to the results
                self.test_throughput.add_msgs_per_second_count(client, events_in_interval)
                
                # Increase current timestamp index
                curr_timestamp += 1
                
                
    def calculate_pbac_correctness(self) -> None:
        
        # We've already correlated published messages to matching subscriptions based on topic
        # and published messages to received messages based on correlation data
        # In this calculation, we only need to check that the purposes of messages are valid
        # by linking received message events with subscription events
        
        # For each message recieved, ensure the message should have been recieved according to purposes
        for pub_event in self.pub_to_recv_mapping:
  
            all_clients_for_pub_event = set()
            
            # Find subscription IDs for subscriptions that should match
            matched_subscriptions_for_clients: Dict[str, List[SubscribeEvent]] = dict()
            for sub_event in self.pub_to_subscriptions_mapping[pub_event]:
                
                # Assume we cannot receive the client's own message
                if pub_event.client_id == sub_event.client_id:
                    continue
                
                # Check if the purpose matches the subscription, if so, we should have a message tied to this sub
                purpose_valid = GlobalDefs.purpose_described_by_filter(pub_event.purpose, sub_event.purpose_filter)
                if purpose_valid:
                    if sub_event.client_id not in matched_subscriptions_for_clients:
                        matched_subscriptions_for_clients[sub_event.client_id] = list()
                    matched_subscriptions_for_clients[sub_event.client_id].append(sub_event)
                    all_clients_for_pub_event.add(sub_event.client_id)
            
            # Get dict of list of clients for the recv events
            recv_events_for_clients: Dict[str, List[RecvMessageEvent]] = dict()
            for recv_event in self.pub_to_recv_mapping[pub_event]:
                if recv_event.recv_client_id not in recv_events_for_clients:
                    recv_events_for_clients[recv_event.recv_client_id] = list()
                recv_events_for_clients[recv_event.recv_client_id].append(recv_event)
                all_clients_for_pub_event.add(recv_event.recv_client_id)
                    
            # Calculate correct and incorrect messages
            for client in all_clients_for_pub_event:
                
                correctly_matched = 0
                not_matched_error = 0
                improperly_matched_error = 0
                
                # If no messages were expected or received, we're good
                if client not in recv_events_for_clients and client not in matched_subscriptions_for_clients:
                    continue
                
                # If we didn't recv any messages for this client when we should've
                elif client not in recv_events_for_clients and client in matched_subscriptions_for_clients:
                    
                    # All messages were not matched properly
                    not_matched_error += len(matched_subscriptions_for_clients[client])
                        
                # If we did recv messages when we shouldn't have
                elif client in recv_events_for_clients and client not in matched_subscriptions_for_clients:
                    
                    # All messages were improperly matched properly
                    improperly_matched_error += len(recv_events_for_clients[client])
                        
                # Otherwise, check correlation
                else:
                    
                    matched_sub_ids = list()
                    found_match = False
                    
                    # For each recv
                    for recv_event in recv_events_for_clients[client]:
                        
                        # Search through subs for a matching sub id
                        found_match = False
                        for sub_event in matched_subscriptions_for_clients[client]:
                            if recv_event.sub_id == sub_event.sub_id:
                                found_match = True
                                correctly_matched += 1
                                matched_sub_ids.append(recv_event.sub_id)
                                break
                
                        # If one wasn't found, this wasn't matched when it should've been
                        if not found_match:
                            improperly_matched_error =+ 1
                            
                    # For each sub
                    for sub_event in matched_subscriptions_for_clients[client]:
                        
                        # First make sure we didn't already match with this 
                        if sub_event.sub_id in matched_sub_ids:
                            continue
                        
                        # Otherwise this should've been matched with a message and wasn't
                        not_matched_error += 1
                        
                # Save results
                self.test_pbac_correctness.add_pbac_correctness_metrics(client, correctly_matched, improperly_matched_error, not_matched_error)


    def export_results(self, out_file: TextIOWrapper) -> None:
        
        lines_to_write: List[str] = list()
        
        lines_to_write.append(f"=============== MQTT-DAP Benchmark Results ===============")
        lines_to_write.append(f"Purpose Management Method: {self.purpose_management_method}")
        
        # We broker assisted operations are currently assumed when PM method > 1
        if self.purpose_management_method == GlobalDefs.PurposeManagementMethod.PM_0 or self.purpose_management_method == GlobalDefs.PurposeManagementMethod.PM_1:
            lines_to_write.append(f"Broker Assisted Operations: Disabled")
        else:
            lines_to_write.append(f"Broker Assisted Operations: Enabled")
        
        lines_to_write.append(f"----- Files Parsed -----")
        for file in self.file_to_seed_mapping:
            seed = self.file_to_seed_mapping[file]
            lines_to_write.append(f"File: {file} - With Seed: {seed}")
            
        lines_to_write.append(f"----- Overall Statistics -----")    
        num_clients = len(self.all_client_statuses)
        lines_to_write.append(f"Client Count: {num_clients}")
        lines_to_write.append(f"Total Published Messages: {self.pub_event_count}")
        lines_to_write.append(f"Data Message Publish Count: {self.data_pub_event_count}")
        lines_to_write.append(f"Operational Message Publish Count: {self.op_pub_event_count}")
            
        lines_to_write.append(f"----- PBAC Correctness Statistics -----")
        # Per client correctness
        for client in self.test_pbac_correctness.pbac_correctness_by_client:
            client_pbac_results: ClientPBACCorrectnessResults = self.test_pbac_correctness.pbac_correctness_by_client[client]
            correct_count = client_pbac_results.properly_matched
            improperly_matched_count = client_pbac_results.improperly_matched
            not_matched_count = client_pbac_results.not_matched
            total_messages = client_pbac_results.get_total_messages()
            pct_correct = (float(correct_count) / float(total_messages)) * 100.0

            lines_to_write.append(f"Average PBAC Correctness for Client {client}: {pct_correct}% (Total Messages: {total_messages} - Correct: {correct_count} - Erroneously Allowed {improperly_matched_count} - Erroneously Rejected {not_matched_count})")
            
        # Overall correctness
        correct_count, improperly_matched_count, not_matched_count, total_messages = self.test_pbac_correctness.get_total_correctness()
        pct_correct = (float(correct_count) / float(total_messages))  * 100.0
        client_count = self.test_pbac_correctness.get_client_count()
        lines_to_write.append(f"Average PBAC Correctness: {pct_correct}% over {client_count} clients (Total Messages: {total_messages} - Correct: {correct_count} - Erroneously Allowed {improperly_matched_count} - Erroneously Rejected {not_matched_count})")
        
        lines_to_write.append(f"----- Operation Corectness Statistics -----")
        # TODO Output stats
        
        lines_to_write.append(f"----- Latency Statistics -----")
        # Per client latency
        for client in self.test_latency.latency_by_client:
            client_latency_results = self.test_latency.latency_by_client[client]
            latency = client_latency_results.get_average_latency_for_client()
            msg_count = client_latency_results.recv_message_count
            lines_to_write.append(f"Average latency for Client {client}: {latency}s over {msg_count} received messages")
            
        # Overall latency
        latency = self.test_latency.get_average_latency()
        client_count = self.test_latency.get_client_count()
        msg_count = self.test_latency.get_message_count()
        lines_to_write.append(f"Total average latency: {latency}s across {client_count} clients over {msg_count} received messages")
        
        lines_to_write.append(f"----- Throughput Statistics -----")
        # Per client throughput
        for client in self.test_throughput.throughput_by_client:
            client_throughput_results = self.test_throughput.throughput_by_client[client]
            throughput = client_throughput_results.get_average_throughput_per_second_for_client()
            interval_length = client_throughput_results.get_total_interval_length_seconds()
            msg_count = client_throughput_results.get_total_messages()
            lines_to_write.append(f"Average throughput for Client {client}: {throughput} msgs/s over {interval_length}s and {msg_count} received messages")
            
        # Overall throughput
        throughput = self.test_throughput.get_average_throughput_per_second()
        client_count = self.test_throughput.get_client_count()
        lines_to_write.append(f"Total average throughput: {throughput} msgs/s per client across {client_count} clients")
        
        # Write with newlines
        out_file.writelines(f"{line}\n" for line in lines_to_write)
        
        

def analyze_results(log_dir: str, out_file_path: Optional[str] = None) -> None:
    """
    Analyze benchmark log files and write results for benchmark.py's analyze command.
    
    Args:
        log_dir (str): Directory containing log files
        out_file_path (str, optional): Output file path for results
    """

    # Define results file if not provided
    if out_file_path is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        out_file_path = path.join(log_dir, f"BenchmarkResults_{timestring}.txt")
        
    # We're going to make sure we can open the file now so we don't spend a lot of time
    # caluculating results to fail on writing
    try:
        out_file = open(out_file_path, "x")
    except FileExistsError:
        print(f"Requested output file {out_file_path} already exists")
        sys.exit(GlobalDefs.ExitCode.BAD_ARGUMENT)

    analyzer = ResultsAnalyzer()
    
    # Pre-process the results files
    analyzer.parse_log_directory(log_dir)
    analyzer.correlate_messages()
    
    # Calculate metrics
    analyzer.calculate_latency()
    analyzer.calculate_throughput()
    analyzer.calculate_pbac_correctness()
    #analyzer.calculate_operation_correctness()
    
    # Print results
    analyzer.export_results(out_file)
    

    # metrics = analyzer.calculate_metrics()

    # with open(out_file, 'w') as f:
    #     f.write(f"Analysis completed on {time.ctime()}\n")
    #     f.write(f"Log files processed: {len(log_files)}\n")
    #     f.write(f"Total clients: {metrics['total_clients']}\n")
    #     f.write(f"Online clients: {metrics['online_clients']}\n")
    #     f.write(f"Total online time: {metrics['total_online_time']:.2f} seconds\n")
    #     f.write(f"SubscribeEvents: {metrics['total_SubscribeEvents']}\n")
    #     f.write(f"PublishEvents: {metrics['total_PublishEvents']}\n")
    #     f.write(f"Received messages: {metrics['total_received']}\n")
    #     f.write("Average latency: Not available (message IDs pending)\n")

    # print(f"Results written to {out_file}")

