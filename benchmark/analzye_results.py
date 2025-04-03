import re
import pandas as pd

file_paths = [
    "C:/Users/Leon/Downloads/benchmark-PM_1-1_2025-04-03_13-02-16.log",
    "C:/Users/Leon/Downloads/benchmark-PM_1-2_2025-04-03_13-02-20.log"
]

publish_pattern = re.compile(r"PUBLISH\*(?P<timestamp>\d+\.\d+)\*.*?\*(?P<msg_id>\d+)\*(?P<topic>[^\*]+)\*.*?\*DATA")
recv_pattern = re.compile(r"RECV\*(?P<timestamp>\d+\.\d+)\*.*?\*(?P<msg_id>\d+)\*(?P<topic>[^\*]+)\*DATA")

publish_events = []
recv_events = []

for path in file_paths:
    with open(path, "r") as file:
        for line in file:
            pub_match = publish_pattern.match(line)
            recv_match = recv_pattern.match(line)
            if pub_match:
                event = pub_match.groupdict()
                event['timestamp'] = float(event['timestamp'])
                publish_events.append(event)
            elif recv_match:
                event = recv_match.groupdict()
                event['timestamp'] = float(event['timestamp'])
                recv_events.append(event)

pub_df = pd.DataFrame(publish_events)
recv_df = pd.DataFrame(recv_events)

pub_df = pub_df.sort_values(by="timestamp")
recv_df = recv_df.sort_values(by="timestamp")

latencies = []
matched_pub_indices = set()
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

latency_df = pd.DataFrame(latencies)

average_latency = latency_df['latency'].mean()
min_latency = latency_df['latency'].min()
max_latency = latency_df['latency'].max()
std_latency = latency_df['latency'].std()
total_published = len(pub_df)
total_received = len(latency_df)
delivery_rate = (total_received / total_published) * 100 if total_published > 0 else 0

print(latency_df.head())
print(f"\nAverage latency: {average_latency:.6f} seconds")
print(f"Minimum latency: {min_latency:.6f} seconds")
print(f"Maximum latency: {max_latency:.6f} seconds")
print(f"Standard deviation (jitter): {std_latency:.6f} seconds")
print(f"Messages published: {total_published}")
print(f"Messages received (matched): {total_received}")
print(f"Delivery rate: {delivery_rate:.2f}%")

latency_df.to_csv("latency_results.csv", index=False)
