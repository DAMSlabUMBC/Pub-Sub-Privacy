import re
import pandas as pd

file_paths = [
    "C:/Users/Leon/Downloads/benchmark-PM_1-1_2025-04-03_13-02-16.log",
    "C:/Users/Leon/Downloads/benchmark-PM_1-2_2025-04-03_13-02-20.log"
]

publish_pattern = re.compile(r"PUBLISH\*(?P<timestamp>\d+\.\d+)\*.*?\*(?P<msg_id>\d+)\*(?P<topic>[^\*]+)\*.*?\*DATA")
recv_pattern = re.compile(r"RECV\*(?P<timestamp>\d+\.\d+)\*.*?\*(?P<msg_id>\d+)\*(?P<topic>[^\*]+)\*DATA")
connect_pattern = re.compile(r"CONNECT\*(?P<timestamp>\d+\.\d+)\*.*?\*(?P<client_id>[^\*]+)")
disconnect_pattern = re.compile(r"DISCONNECT\*(?P<timestamp>\d+\.\d+)\*.*?\*(?P<client_id>[^\*]+)")
subscribe_pattern = re.compile(r"SUBSCRIBE\*(?P<timestamp>\d+\.\d+)\*.*?\*(?P<client_id>[^\*]+)\*(?P<topic>[^\*]+)")

publish_events = []
recv_events = []
connect_events = []
disconnect_events = []
subscribe_events = []

for path in file_paths:
    with open(path, "r") as file:
        for line in file:
            pub_match = publish_pattern.match(line)
            recv_match = recv_pattern.match(line)
            conn_match = connect_pattern.match(line)
            disc_match = disconnect_pattern.match(line)
            sub_match = subscribe_pattern.match(line)
            if pub_match:
                event = pub_match.groupdict()
                event['timestamp'] = float(event['timestamp'])
                publish_events.append(event)
            elif recv_match:
                event = recv_match.groupdict()
                event['timestamp'] = float(event['timestamp'])
                recv_events.append(event)
            elif conn_match:
                event = conn_match.groupdict()
                event['timestamp'] = float(event['timestamp'])
                connect_events.append(event)
            elif disc_match:
                event = disc_match.groupdict()
                event['timestamp'] = float(event['timestamp'])
                disconnect_events.append(event)
            elif sub_match:
                event = sub_match.groupdict()
                event['timestamp'] = float(event['timestamp'])
                subscribe_events.append(event)

pub_df = pd.DataFrame(publish_events) if publish_events else pd.DataFrame(columns=['timestamp', 'msg_id', 'topic'])
recv_df = pd.DataFrame(recv_events) if recv_events else pd.DataFrame(columns=['timestamp', 'msg_id', 'topic'])
conn_df = pd.DataFrame(connect_events) if connect_events else pd.DataFrame(columns=['timestamp', 'client_id'])
disc_df = pd.DataFrame(disconnect_events) if disconnect_events else pd.DataFrame(columns=['timestamp', 'client_id'])
sub_df = pd.DataFrame(subscribe_events) if subscribe_events else pd.DataFrame(columns=['timestamp', 'client_id', 'topic'])

if not pub_df.empty:
    pub_df = pub_df.sort_values(by="timestamp")
if not recv_df.empty:
    recv_df = recv_df.sort_values(by="timestamp")
if not conn_df.empty:
    conn_df = conn_df.sort_values(by="timestamp")
if not disc_df.empty:
    disc_df = disc_df.sort_values(by="timestamp")
if not sub_df.empty:
    sub_df = sub_df.sort_values(by="timestamp")

client_subscriptions = {}
client_connections = {}

for _, sub in sub_df.iterrows():
    client_id = sub['client_id']
    if client_id not in client_subscriptions:
        client_subscriptions[client_id] = {}
    client_subscriptions[client_id][sub['topic']] = sub['timestamp']

for _, conn in conn_df.iterrows():
    client_connections[conn['client_id']] = conn['timestamp']

for _, disc in disc_df.iterrows():
    if disc['client_id'] in client_connections:
        client_connections[disc['client_id']] = None

latencies = []
matched_pub_indices = set()
missed_due_to_disconnect = 0
missed_due_to_no_subscription = 0

for _, recv_row in recv_df.iterrows():
    recv_time = recv_row['timestamp']
    topic = recv_row['topic']
    matching_pubs = pub_df[(pub_df['topic'] == topic) & (pub_df['timestamp'] <= recv_time)]
    if not matching_pubs.empty:
        closest_pub = matching_pubs.iloc[-1]
        pub_index = closest_pub.name
        if pub_index not in matched_pub_indices:
            latency = recv_time - closest_pub['timestamp']
            latencies.append({
                'topic': topic,
                'publish_time': closest_pub['timestamp'],
                'recv_time': recv_time,
                'latency': latency
            })
            matched_pub_indices.add(pub_index)

for _, pub_row in pub_df.iterrows():
    if pub_row.name not in matched_pub_indices:
        pub_time = pub_row['timestamp']
        topic = pub_row['topic']
        missed = True
        for client_id, conn_time in client_connections.items():
            if conn_time is not None and pub_time >= conn_time:
                if (client_id in client_subscriptions and
                    topic in client_subscriptions[client_id] and
                    pub_time >= client_subscriptions[client_id][topic]):
                    missed = False
                else:
                    missed_due_to_no_subscription += 1
                    missed = False
            else:
                disc_times = disc_df[disc_df['client_id'] == client_id]['timestamp']
                if not disc_times.empty and any(pub_time > disc_time for disc_time in disc_times):
                    missed_due_to_disconnect += 1
                    missed = False
        if missed:
            missed_due_to_no_subscription += 1

latency_df = pd.DataFrame(latencies)

total_published = len(pub_df)
total_received = len(latency_df)

if total_published > 0 and not pub_df.empty:
    pub_time_span = pub_df['timestamp'].max() - pub_df['timestamp'].min()
    pub_throughput = total_published / pub_time_span if pub_time_span > 0 else 0
else:
    pub_throughput = 0

if total_received > 0 and not recv_df.empty:
    recv_time_span = recv_df['timestamp'].max() - recv_df['timestamp'].min()
    recv_throughput = total_received / recv_time_span if recv_time_span > 0 else 0
else:
    recv_throughput = 0

average_latency = latency_df['latency'].mean() if not latency_df.empty else 0
min_latency = latency_df['latency'].min() if not latency_df.empty else 0
max_latency = latency_df['latency'].max() if not latency_df.empty else 0
std_latency = latency_df['latency'].std() if not latency_df.empty else 0
delivery_rate = (total_received / total_published) * 100 if total_published > 0 else 0

print(latency_df.head())
print(f"\nAverage latency: {average_latency:.6f} seconds")
print(f"Minimum latency: {min_latency:.6f} seconds")
print(f"Maximum latency: {max_latency:.6f} seconds")
print(f"Standard deviation (jitter): {std_latency:.6f} seconds")
print(f"Messages published: {total_published}")
print(f"Messages received (matched): {total_received}")
print(f"Messages missed due to disconnects: {missed_due_to_disconnect}")
print(f"Messages missed due to no subscription: {missed_due_to_no_subscription}")
print(f"Delivery rate: {delivery_rate:.2f}%")
print(f"Publish throughput: {pub_throughput:.2f} messages/second")
print(f"Receive throughput: {recv_throughput:.2f} messages/second")

summary_data = [
    {"Metric": "Average Latency (seconds)", "Value": f"{average_latency:.6f}"},
    {"Metric": "Minimum Latency (seconds)", "Value": f"{min_latency:.6f}"},
    {"Metric": "Maximum Latency (seconds)", "Value": f"{max_latency:.6f}"},
    {"Metric": "Standard Deviation (jitter, seconds)", "Value": f"{std_latency:.6f}"},
    {"Metric": "Messages Published", "Value": str(total_published)},
    {"Metric": "Messages Received (matched)", "Value": str(total_received)},
    {"Metric": "Messages Missed Due to Disconnects", "Value": str(missed_due_to_disconnect)},
    {"Metric": "Messages Missed Due to No Subscription", "Value": str(missed_due_to_no_subscription)},
    {"Metric": "Delivery Rate (%)", "Value": f"{delivery_rate:.2f}"},
    {"Metric": "Publish Throughput (messages/second)", "Value": f"{pub_throughput:.2f}"},
    {"Metric": "Receive Throughput (messages/second)", "Value": f"{recv_throughput:.2f}"}
]
summary_df = pd.DataFrame(summary_data)

output_path = "C:/Users/Leon/Downloads/latency_results.csv"
latency_df.to_csv(output_path, index=False)
with open(output_path, 'a') as f:
    f.write("\n")
    f.write("Summary Statistics\n")
    summary_df.to_csv(f, index=False)
