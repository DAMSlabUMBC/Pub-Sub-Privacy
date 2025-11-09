import statistics
from dataclasses import dataclass, field
from typing import Dict, List, Set, Optional, Tuple
from pathlib import Path
import GlobalDefs
from LoggingModule import (
    SEPARATOR, SEED_LABEL, PM_METHOD_LABEL, CPU_METRICS_LABEL, MEM_METRICS_LABEL,
    CONNECT_LABEL, DISCONNECT_LABEL, SUBSCRIBE_LABEL, PUBLISH_LABEL,
    OP_PUBLISH_LABEL, RECV_LABEL, OP_RECV_LABEL
)


@dataclass
class PublishEvent:
    """Message publication event"""
    timestamp: float
    benchmark_id: str
    client_id: str
    topic: str
    purpose: str
    msg_type: str
    corr_data: int


@dataclass
class RecvEvent:
    """Message reception event"""
    timestamp: float
    benchmark_id: str
    recv_client_id: str
    sending_client_id: str
    topic: str
    sub_id: str
    msg_type: str
    corr_data: int


@dataclass
class SubscribeEvent:
    """Subscription event"""
    timestamp: float
    benchmark_id: str
    client_id: str
    topic_filter: str
    purpose_filter: str
    sub_id: str


@dataclass
class OperationPublishEvent:
    """Operational message publication event"""
    timestamp: float
    benchmark_id: str
    client_id: str
    topic: str
    purpose: str
    op_type: str
    op_category: str
    corr_data: int


@dataclass
class OperationRecvEvent:
    """Operational message reception event"""
    timestamp: float
    benchmark_id: str
    recv_client_id: str
    sending_client_id: str
    topic: str
    sub_id: str
    op_type: str
    op_category: str
    op_status: str
    corr_data: int


@dataclass
class BrokerStats:
    """Broker resource usage stats"""
    cpu_min: float = 0.0
    cpu_max: float = 0.0
    cpu_avg: float = 0.0
    cpu_variance: float = 0.0
    mem_min: float = 0.0
    mem_max: float = 0.0
    mem_avg: float = 0.0
    mem_variance: float = 0.0


@dataclass
class MessagingStats:
    """Messaging performance stats"""
    latency_min_ms: float = 0.0
    latency_max_ms: float = 0.0
    latency_avg_ms: float = 0.0
    latency_variance_ms: float = 0.0
    throughput_msgs_per_sec: float = 0.0
    avg_header_size_bytes: float = 0.0
    non_data_msg_count: int = 0
    total_data_msg_count: int = 0


@dataclass
class SubscriberPurposeCorrectness:
    """Purpose correctness per subscriber"""
    subscriber_id: str
    valid_recv_count: int = 0
    invalid_recv_count: int = 0
    expected_msg_count: int = 0
    total_recv_count: int = 0
    false_accept: float = 0.0
    false_reject: float = 0.0


@dataclass
class DPCorrectnessMetrics:
    """DP correctness per request"""
    request_id: str
    expected_responses: int = 0
    actual_responses: int = 0
    relevant_responses: int = 0
    irrelevant_responses: int = 0
    all_subscribers: int = 0
    coverage: float = 0.0
    leakage: float = 0.0
    completion: float = 0.0
    lost: float = 0.0


@dataclass
class TestMetrics:
    """Test metrics"""
    test_name: str
    pm_method: str
    broker_stats: BrokerStats = field(default_factory=BrokerStats)
    messaging_stats: MessagingStats = field(default_factory=MessagingStats)
    purpose_correctness_per_sub: Dict[str, SubscriberPurposeCorrectness] = field(default_factory=dict)
    dp_correctness: Dict[str, DPCorrectnessMetrics] = field(default_factory=dict)


class MetricsCalculator:
    """Calculate metrics from test logs"""

    def __init__(self):
        self.publish_events: List[PublishEvent] = []
        self.recv_events: List[RecvEvent] = []
        self.subscribe_events: List[SubscribeEvent] = []
        self.op_publish_events: List[OperationPublishEvent] = []
        self.op_recv_events: List[OperationRecvEvent] = []

        self.pm_method: Optional[str] = None
        self.cpu_metrics: Optional[Tuple[float, float, float, float]] = None
        self.mem_metrics: Optional[Tuple[float, float, float, float]] = None

        self.subscriber_subscriptions: Dict[str, List[SubscribeEvent]] = {}

    def parse_log_file(self, log_file_path: str) -> bool:
        """Parse log file and extract events"""
        log_path = Path(log_file_path)
        if not log_path.exists():
            print(f"Log file not found: {log_file_path}")
            return False

        with open(log_path, 'r') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                parts = line.split(SEPARATOR)
                if len(parts) < 2:
                    continue

                label = parts[0]

                try:
                    if label == PM_METHOD_LABEL:
                        self.pm_method = parts[1]

                    elif label == CPU_METRICS_LABEL:
                        self.cpu_metrics = (
                            float(parts[1]),  # min
                            float(parts[2]),  # max
                            float(parts[3]),  # avg
                            float(parts[4])   # variance
                        )

                    elif label == MEM_METRICS_LABEL:
                        self.mem_metrics = (
                            float(parts[1]),  # min
                            float(parts[2]),  # max
                            float(parts[3]),  # avg
                            float(parts[4])   # variance
                        )

                    elif label == PUBLISH_LABEL:
                        # PUBLISH@@timestamp@@benchmark_id@@client_id@@topic@@purpose@@msg_type@@corr_data
                        event = PublishEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic=parts[4],
                            purpose=parts[5],
                            msg_type=parts[6],
                            corr_data=int(parts[7])
                        )
                        self.publish_events.append(event)

                    elif label == RECV_LABEL:
                        # RECV@@timestamp@@benchmark_id@@recv_client@@sending_client@@topic@@sub_id@@msg_type@@corr_data
                        event = RecvEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            recv_client_id=parts[3],
                            sending_client_id=parts[4],
                            topic=parts[5],
                            sub_id=parts[6],
                            msg_type=parts[7],
                            corr_data=int(parts[8])
                        )
                        self.recv_events.append(event)

                    elif label == SUBSCRIBE_LABEL:
                        # SUBSCRIBE@@timestamp@@benchmark_id@@client_id@@topic_filter@@purpose_filter@@sub_id
                        event = SubscribeEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic_filter=parts[4],
                            purpose_filter=parts[5],
                            sub_id=parts[6]
                        )
                        self.subscribe_events.append(event)

                        # Track subscriber subscriptions
                        if event.client_id not in self.subscriber_subscriptions:
                            self.subscriber_subscriptions[event.client_id] = []
                        self.subscriber_subscriptions[event.client_id].append(event)

                    elif label == OP_PUBLISH_LABEL:
                        # PUBLISH_OP@@timestamp@@benchmark_id@@client_id@@topic@@purpose@@op_type@@op_category@@corr_data
                        event = OperationPublishEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            client_id=parts[3],
                            topic=parts[4],
                            purpose=parts[5],
                            op_type=parts[6],
                            op_category=parts[7],
                            corr_data=int(parts[8])
                        )
                        self.op_publish_events.append(event)

                    elif label == OP_RECV_LABEL:
                        # RECV_OP@@timestamp@@benchmark_id@@recv_client@@sending_client@@topic@@sub_id@@op_type@@op_category@@op_status@@corr_data
                        event = OperationRecvEvent(
                            timestamp=float(parts[1]),
                            benchmark_id=parts[2],
                            recv_client_id=parts[3],
                            sending_client_id=parts[4],
                            topic=parts[5],
                            sub_id=parts[6],
                            op_type=parts[7],
                            op_category=parts[8],
                            op_status=parts[9],
                            corr_data=int(parts[10])
                        )
                        self.op_recv_events.append(event)

                except (IndexError, ValueError) as e:
                    print(f"Error parsing line: {line}")
                    print(f"Error: {e}")
                    continue

        return True

    def calculate_broker_stats(self) -> BrokerStats:
        """Calculate broker resource usage"""
        stats = BrokerStats()

        if self.cpu_metrics:
            stats.cpu_min = self.cpu_metrics[0]
            stats.cpu_max = self.cpu_metrics[1]
            stats.cpu_avg = self.cpu_metrics[2]
            stats.cpu_variance = self.cpu_metrics[3]

        if self.mem_metrics:
            stats.mem_min = self.mem_metrics[0]
            stats.mem_max = self.mem_metrics[1]
            stats.mem_avg = self.mem_metrics[2]
            stats.mem_variance = self.mem_metrics[3]

        return stats

    def calculate_messaging_stats(self) -> MessagingStats:
        """Calculate messaging performance"""
        stats = MessagingStats()

        # Calculate latencies by matching publish and recv events
        latencies: List[float] = []

        # Create mapping of (client_id, corr_data) to publish timestamp
        publish_map: Dict[Tuple[str, int], float] = {}
        for pub_event in self.publish_events:
            key = (pub_event.client_id, pub_event.corr_data)
            publish_map[key] = pub_event.timestamp

        # Match recv events with publish events
        for recv_event in self.recv_events:
            key = (recv_event.sending_client_id, recv_event.corr_data)
            if key in publish_map:
                latency_ms = (recv_event.timestamp - publish_map[key]) * 1000.0
                latencies.append(latency_ms)

        # Calculate latency statistics
        if latencies:
            stats.latency_min_ms = min(latencies)
            stats.latency_max_ms = max(latencies)
            stats.latency_avg_ms = statistics.mean(latencies)
            if len(latencies) > 1:
                stats.latency_variance_ms = statistics.variance(latencies)

        # Calculate throughput
        stats.total_data_msg_count = len(self.publish_events)
        if self.publish_events:
            test_duration_s = (
                self.publish_events[-1].timestamp - self.publish_events[0].timestamp
            )
            if test_duration_s > 0:
                stats.throughput_msgs_per_sec = len(self.publish_events) / test_duration_s

        # Count non-data messages
        stats.non_data_msg_count = len(self.op_publish_events)

        # Calculate average header size (estimate based on MQTT v5)
        # This is a simplified estimation
        header_sizes: List[int] = []
        for pub_event in self.publish_events:
            # MQTT v5 fixed header: ~2-5 bytes
            # Variable header for PUBLISH: topic length + topic + properties
            # Properties: purpose (user property) + correlation data
            base_header = 5
            topic_bytes = len(pub_event.topic.encode('utf-8')) + 2
            purpose_bytes = len(pub_event.purpose.encode('utf-8')) + 4  # user property overhead
            corr_data_bytes = 8  # correlation data size

            total_header = base_header + topic_bytes + purpose_bytes + corr_data_bytes
            header_sizes.append(total_header)

        if header_sizes:
            stats.avg_header_size_bytes = statistics.mean(header_sizes)

        return stats

    def calculate_purpose_correctness(self) -> Dict[str, SubscriberPurposeCorrectness]:
        """Calculate purpose correctness per subscriber"""
        results: Dict[str, SubscriberPurposeCorrectness] = {}

        # For each subscriber, check if received messages match their purpose filter
        for subscriber_id, subscriptions in self.subscriber_subscriptions.items():
            metrics = SubscriberPurposeCorrectness(subscriber_id=subscriber_id)

            # Get all messages received by this subscriber
            subscriber_recvs = [
                recv for recv in self.recv_events
                if recv.recv_client_id == subscriber_id
            ]

            metrics.total_recv_count = len(subscriber_recvs)

            # For each received message, check if purpose matches any subscription
            for recv_event in subscriber_recvs:
                # Find the corresponding publish event to get the purpose
                matching_pubs = [
                    pub for pub in self.publish_events
                    if pub.client_id == recv_event.sending_client_id
                    and pub.corr_data == recv_event.corr_data
                ]

                if not matching_pubs:
                    continue

                pub_event = matching_pubs[0]

                # Check if this message's purpose matches any of the subscriber's filters
                is_valid = False
                for sub in subscriptions:
                    if GlobalDefs.purpose_described_by_filter(pub_event.purpose, sub.purpose_filter):
                        is_valid = True
                        break

                if is_valid:
                    metrics.valid_recv_count += 1
                else:
                    metrics.invalid_recv_count += 1

            # Calculate expected message count
            # This requires knowing which messages should have been received
            # For now, use valid_recv_count as minimum expectation
            # A more sophisticated approach would require test configuration
            metrics.expected_msg_count = metrics.valid_recv_count

            # Calculate false accept and false reject
            if metrics.total_recv_count > 0:
                metrics.false_accept = metrics.invalid_recv_count / metrics.total_recv_count
            else:
                metrics.false_accept = 0.0

            if metrics.expected_msg_count > 0:
                metrics.false_reject = 1.0 - (metrics.valid_recv_count / metrics.expected_msg_count)
            else:
                metrics.false_reject = 0.0

            results[subscriber_id] = metrics

        return results

    def calculate_dp_correctness(self) -> Dict[str, DPCorrectnessMetrics]:
        """Calculate DP correctness per request"""
        results: Dict[str, DPCorrectnessMetrics] = {}

        # Group operation requests by correlation data (request ID)
        request_pubs: Dict[int, OperationPublishEvent] = {}
        for op_pub in self.op_publish_events:
            request_pubs[op_pub.corr_data] = op_pub

        # For each request, calculate correctness metrics
        for corr_data, req_pub in request_pubs.items():
            request_id = f"req_{corr_data}"
            metrics = DPCorrectnessMetrics(request_id=request_id)

            # Find all responses to this request
            # NOTE: Filter for op_resp topics to get actual responses, not the original requests
            # Had a bug here before where I was counting RECV_OP events instead of response publishes
            responses = [
                op_pub for op_pub in self.op_publish_events
                if op_pub.corr_data == corr_data and 'op_resp' in op_pub.topic
            ]

            metrics.actual_responses = len(responses)

            # Determine relevant vs irrelevant responses
            # Relevant responses have matching purpose filters
            for resp in responses:
                # Check if responder (publisher) should have data for the requester
                resp_client = resp.client_id
                if resp_client in self.subscriber_subscriptions:
                    subs = self.subscriber_subscriptions[resp_client]
                    is_relevant = any(
                        GlobalDefs.purpose_described_by_filter(req_pub.purpose, sub.purpose_filter)
                        for sub in subs
                    )

                    if is_relevant:
                        metrics.relevant_responses += 1
                    else:
                        metrics.irrelevant_responses += 1
                else:
                    # If responder is not in subscriptions, it's likely a publisher
                    # All publisher responses are considered relevant for now
                    metrics.relevant_responses += 1

            # Count all subscribers
            metrics.all_subscribers = len(self.subscriber_subscriptions)

            # Expected responses = subscribers with matching purpose filter
            metrics.expected_responses = 0
            for subscriber_id, subs in self.subscriber_subscriptions.items():
                for sub in subs:
                    if GlobalDefs.purpose_described_by_filter(req_pub.purpose, sub.purpose_filter):
                        metrics.expected_responses += 1
                        break

            # Calculate derived metrics
            if metrics.expected_responses > 0:
                metrics.coverage = metrics.relevant_responses / metrics.expected_responses
                metrics.completion = metrics.relevant_responses / metrics.expected_responses
                metrics.lost = 1.0 - metrics.completion
            else:
                metrics.coverage = 0.0
                metrics.completion = 0.0
                metrics.lost = 1.0

            non_expected_subs = metrics.all_subscribers - metrics.expected_responses
            if non_expected_subs > 0:
                metrics.leakage = metrics.irrelevant_responses / non_expected_subs
            else:
                metrics.leakage = 0.0

            results[request_id] = metrics

        return results

    def calculate_all_metrics(self, log_file_path: str, test_name: str = "test") -> Optional[TestMetrics]:
        """Calculate all metrics"""
        if not self.parse_log_file(log_file_path):
            return None

        metrics = TestMetrics(
            test_name=test_name,
            pm_method=self.pm_method or "Unknown"
        )

        metrics.broker_stats = self.calculate_broker_stats()
        metrics.messaging_stats = self.calculate_messaging_stats()
        metrics.purpose_correctness_per_sub = self.calculate_purpose_correctness()
        metrics.dp_correctness = self.calculate_dp_correctness()

        return metrics

    def print_metrics(self, metrics: TestMetrics):
        """Print metrics"""
        print(f"\n{'='*80}")
        print(f"METRICS REPORT: {metrics.test_name}")
        print(f"Purpose Management Method: {metrics.pm_method}")
        print(f"{'='*80}")

        # Broker Stats
        print(f"\n--- Broker Statistics ---")
        print(f"CPU Usage:")
        print(f"  Min:      {metrics.broker_stats.cpu_min:.2f}%")
        print(f"  Max:      {metrics.broker_stats.cpu_max:.2f}%")
        print(f"  Average:  {metrics.broker_stats.cpu_avg:.2f}%")
        print(f"  Variance: {metrics.broker_stats.cpu_variance:.2f}")
        print(f"\nMemory Usage:")
        print(f"  Min:      {metrics.broker_stats.mem_min:.2f} MB")
        print(f"  Max:      {metrics.broker_stats.mem_max:.2f} MB")
        print(f"  Average:  {metrics.broker_stats.mem_avg:.2f} MB")
        print(f"  Variance: {metrics.broker_stats.mem_variance:.2f}")

        # Messaging Stats
        print(f"\n--- Messaging Statistics ---")
        print(f"Latency:")
        print(f"  Min:      {metrics.messaging_stats.latency_min_ms:.2f} ms")
        print(f"  Max:      {metrics.messaging_stats.latency_max_ms:.2f} ms")
        print(f"  Average:  {metrics.messaging_stats.latency_avg_ms:.2f} ms")
        print(f"  Variance: {metrics.messaging_stats.latency_variance_ms:.2f}")
        print(f"\nThroughput: {metrics.messaging_stats.throughput_msgs_per_sec:.2f} msgs/sec")
        print(f"Average Header Size: {metrics.messaging_stats.avg_header_size_bytes:.2f} bytes")
        print(f"Data Messages: {metrics.messaging_stats.total_data_msg_count}")
        print(f"Non-Data Messages: {metrics.messaging_stats.non_data_msg_count}")

        # Purpose Correctness Summary
        print(f"\n--- Purpose Correctness Summary ---")
        if metrics.purpose_correctness_per_sub:
            total_subs = len(metrics.purpose_correctness_per_sub)
            total_valid = sum(pc.valid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
            total_invalid = sum(pc.invalid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
            total_expected = sum(pc.expected_msg_count for pc in metrics.purpose_correctness_per_sub.values())
            total_received = sum(pc.total_recv_count for pc in metrics.purpose_correctness_per_sub.values())
            avg_false_accept = sum(pc.false_accept for pc in metrics.purpose_correctness_per_sub.values()) / total_subs
            avg_false_reject = sum(pc.false_reject for pc in metrics.purpose_correctness_per_sub.values()) / total_subs

            print(f"Total Subscribers:       {total_subs}")
            print(f"Total Valid Messages:    {total_valid}")
            print(f"Total Invalid Messages:  {total_invalid}")
            print(f"Total Expected:          {total_expected}")
            print(f"Total Received:          {total_received}")
            print(f"Avg False Accept Rate:   {avg_false_accept:.4f} ({avg_false_accept*100:.2f}%)")
            print(f"Avg False Reject Rate:   {avg_false_reject:.4f} ({avg_false_reject*100:.2f}%)")
        else:
            print("No purpose correctness data available")

        # DP Correctness Summary
        if metrics.dp_correctness:
            print(f"\n--- DP Correctness Summary ---")
            total_requests = len(metrics.dp_correctness)
            total_expected_resp = sum(dp.expected_responses for dp in metrics.dp_correctness.values())
            total_actual_resp = sum(dp.actual_responses for dp in metrics.dp_correctness.values())
            total_relevant_resp = sum(dp.relevant_responses for dp in metrics.dp_correctness.values())
            total_irrelevant_resp = sum(dp.irrelevant_responses for dp in metrics.dp_correctness.values())
            avg_coverage = sum(dp.coverage for dp in metrics.dp_correctness.values()) / total_requests
            avg_leakage = sum(dp.leakage for dp in metrics.dp_correctness.values()) / total_requests
            avg_completion = sum(dp.completion for dp in metrics.dp_correctness.values()) / total_requests
            avg_lost = sum(dp.lost for dp in metrics.dp_correctness.values()) / total_requests

            print(f"Total Operational Requests: {total_requests}")
            print(f"Total Expected Responses:   {total_expected_resp}")
            print(f"Total Actual Responses:     {total_actual_resp}")
            print(f"Total Relevant Responses:   {total_relevant_resp}")
            print(f"Total Irrelevant Responses: {total_irrelevant_resp}")
            print(f"Avg Coverage:               {avg_coverage:.4f} ({avg_coverage*100:.2f}%)")
            print(f"Avg Leakage:                {avg_leakage:.4f} ({avg_leakage*100:.2f}%)")
            print(f"Avg Completion:             {avg_completion:.4f} ({avg_completion*100:.2f}%)")
            print(f"Avg Lost:                   {avg_lost:.4f} ({avg_lost*100:.2f}%)")

            # Show the breakdown by category (C1_REG, C2, C3) to see what's going on
            self._print_dp_by_category(metrics)

        print(f"\n{'='*80}\n")

    def _get_dp_category_stats(self, metrics: TestMetrics) -> dict:
        """Break down DP correctness by operation category (C1_REG, C2, C3)

        Basically just groups all the operational requests by category and adds up
        the metrics for each one. Makes it easier to see which types have issues.
        """
        category_stats = {}

        # Go through all the operational requests and sort them by category
        for op_pub in self.op_publish_events:
            if 'op_resp' in op_pub.topic:
                continue  # Skip responses, we only want the original requests

            category = op_pub.op_category
            if category not in category_stats:
                category_stats[category] = {
                    'requests': 0,
                    'expected': 0,
                    'actual': 0,
                    'relevant': 0,
                    'irrelevant': 0
                }

            category_stats[category]['requests'] += 1

            # Grab the metrics for this specific request
            req_id = f"req_{op_pub.corr_data}"
            if req_id in metrics.dp_correctness:
                dp_metrics = metrics.dp_correctness[req_id]
                category_stats[category]['expected'] += dp_metrics.expected_responses
                category_stats[category]['actual'] += dp_metrics.actual_responses
                category_stats[category]['relevant'] += dp_metrics.relevant_responses
                category_stats[category]['irrelevant'] += dp_metrics.irrelevant_responses

        return category_stats

    def _print_dp_by_category(self, metrics: TestMetrics):
        """Print DP correctness breakdown by category

        Shows each operation category (C1_REG, C2, C3) separately so I can
        see which types are having coverage problems.
        """
        if not metrics.dp_correctness:
            return

        category_stats = self._get_dp_category_stats(metrics)

        print(f"\n--- DP Correctness by Operation Category ---")
        for category in sorted(category_stats.keys()):
            stats = category_stats[category]
            if stats['requests'] == 0:
                continue

            # Calculate the percentages
            coverage = (stats['relevant'] / stats['expected'] * 100) if stats['expected'] > 0 else 0
            leakage = (stats['irrelevant'] / max(1, stats['expected']) * 100)

            print(f"\n{category} Operations:")
            print(f"  Requests:            {stats['requests']}")
            print(f"  Expected Responses:  {stats['expected']}")
            print(f"  Actual Responses:    {stats['actual']}")
            print(f"  Relevant Responses:  {stats['relevant']}")
            print(f"  Coverage:            {coverage:.2f}%")
            print(f"  Leakage:             {leakage:.2f}%")

    def _export_dp_by_category_to_csv(self, writer, metrics: TestMetrics):
        """Export DP correctness breakdown by category to CSV

        Writes a row for each category with all the stats so I can look at them
        in Excel or whatever.
        """
        if not metrics.dp_correctness:
            return

        category_stats = self._get_dp_category_stats(metrics)

        # Write out each category's metrics
        for category in sorted(category_stats.keys()):
            stats = category_stats[category]
            if stats['requests'] == 0:
                continue

            coverage = (stats['relevant'] / stats['expected']) if stats['expected'] > 0 else 0
            leakage = (stats['irrelevant'] / max(1, stats['expected']))

            # Write all the stats for this category
            writer.writerow([f"DP {category}", "Requests", f"{stats['requests']}"])
            writer.writerow([f"DP {category}", "Expected Responses", f"{stats['expected']}"])
            writer.writerow([f"DP {category}", "Actual Responses", f"{stats['actual']}"])
            writer.writerow([f"DP {category}", "Relevant Responses", f"{stats['relevant']}"])
            writer.writerow([f"DP {category}", "Coverage", f"{coverage:.4f}"])
            writer.writerow([f"DP {category}", "Leakage", f"{leakage:.4f}"])

    def export_metrics_to_csv(self, metrics: TestMetrics, output_path: str):
        """Export metrics to CSV"""
        import csv

        with open(output_path, 'w', newline='') as f:
            writer = csv.writer(f)

            # Header
            writer.writerow(["Metric Category", "Metric Name", "Value"])

            # Broker Stats
            writer.writerow(["Broker", "CPU Min (%)", f"{metrics.broker_stats.cpu_min:.2f}"])
            writer.writerow(["Broker", "CPU Max (%)", f"{metrics.broker_stats.cpu_max:.2f}"])
            writer.writerow(["Broker", "CPU Avg (%)", f"{metrics.broker_stats.cpu_avg:.2f}"])
            writer.writerow(["Broker", "CPU Variance", f"{metrics.broker_stats.cpu_variance:.2f}"])
            writer.writerow(["Broker", "Memory Min (MB)", f"{metrics.broker_stats.mem_min:.2f}"])
            writer.writerow(["Broker", "Memory Max (MB)", f"{metrics.broker_stats.mem_max:.2f}"])
            writer.writerow(["Broker", "Memory Avg (MB)", f"{metrics.broker_stats.mem_avg:.2f}"])
            writer.writerow(["Broker", "Memory Variance", f"{metrics.broker_stats.mem_variance:.2f}"])

            # Messaging Stats
            writer.writerow(["Messaging", "Latency Min (ms)", f"{metrics.messaging_stats.latency_min_ms:.2f}"])
            writer.writerow(["Messaging", "Latency Max (ms)", f"{metrics.messaging_stats.latency_max_ms:.2f}"])
            writer.writerow(["Messaging", "Latency Avg (ms)", f"{metrics.messaging_stats.latency_avg_ms:.2f}"])
            writer.writerow(["Messaging", "Latency Variance", f"{metrics.messaging_stats.latency_variance_ms:.2f}"])
            writer.writerow(["Messaging", "Throughput (msgs/sec)", f"{metrics.messaging_stats.throughput_msgs_per_sec:.2f}"])
            writer.writerow(["Messaging", "Avg Header Size (bytes)", f"{metrics.messaging_stats.avg_header_size_bytes:.2f}"])
            writer.writerow(["Messaging", "Data Messages", f"{metrics.messaging_stats.total_data_msg_count}"])
            writer.writerow(["Messaging", "Non-Data Messages", f"{metrics.messaging_stats.non_data_msg_count}"])

            # Purpose Correctness Summary
            if metrics.purpose_correctness_per_sub:
                total_subs = len(metrics.purpose_correctness_per_sub)
                total_valid = sum(pc.valid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
                total_invalid = sum(pc.invalid_recv_count for pc in metrics.purpose_correctness_per_sub.values())
                total_expected = sum(pc.expected_msg_count for pc in metrics.purpose_correctness_per_sub.values())
                total_received = sum(pc.total_recv_count for pc in metrics.purpose_correctness_per_sub.values())
                avg_false_accept = sum(pc.false_accept for pc in metrics.purpose_correctness_per_sub.values()) / total_subs
                avg_false_reject = sum(pc.false_reject for pc in metrics.purpose_correctness_per_sub.values()) / total_subs

                writer.writerow(["Purpose Correctness", "Total Subscribers", f"{total_subs}"])
                writer.writerow(["Purpose Correctness", "Total Valid Messages", f"{total_valid}"])
                writer.writerow(["Purpose Correctness", "Total Invalid Messages", f"{total_invalid}"])
                writer.writerow(["Purpose Correctness", "Total Expected", f"{total_expected}"])
                writer.writerow(["Purpose Correctness", "Total Received", f"{total_received}"])
                writer.writerow(["Purpose Correctness", "Avg False Accept Rate", f"{avg_false_accept:.4f}"])
                writer.writerow(["Purpose Correctness", "Avg False Reject Rate", f"{avg_false_reject:.4f}"])

            # DP Correctness Summary
            if metrics.dp_correctness:
                total_requests = len(metrics.dp_correctness)
                total_expected_resp = sum(dp.expected_responses for dp in metrics.dp_correctness.values())
                total_actual_resp = sum(dp.actual_responses for dp in metrics.dp_correctness.values())
                total_relevant_resp = sum(dp.relevant_responses for dp in metrics.dp_correctness.values())
                total_irrelevant_resp = sum(dp.irrelevant_responses for dp in metrics.dp_correctness.values())
                avg_coverage = sum(dp.coverage for dp in metrics.dp_correctness.values()) / total_requests
                avg_leakage = sum(dp.leakage for dp in metrics.dp_correctness.values()) / total_requests
                avg_completion = sum(dp.completion for dp in metrics.dp_correctness.values()) / total_requests
                avg_lost = sum(dp.lost for dp in metrics.dp_correctness.values()) / total_requests

                writer.writerow(["DP Correctness", "Total Operational Requests", f"{total_requests}"])
                writer.writerow(["DP Correctness", "Total Expected Responses", f"{total_expected_resp}"])
                writer.writerow(["DP Correctness", "Total Actual Responses", f"{total_actual_resp}"])
                writer.writerow(["DP Correctness", "Total Relevant Responses", f"{total_relevant_resp}"])
                writer.writerow(["DP Correctness", "Total Irrelevant Responses", f"{total_irrelevant_resp}"])
                writer.writerow(["DP Correctness", "Avg Coverage", f"{avg_coverage:.4f}"])
                writer.writerow(["DP Correctness", "Avg Leakage", f"{avg_leakage:.4f}"])
                writer.writerow(["DP Correctness", "Avg Completion", f"{avg_completion:.4f}"])
                writer.writerow(["DP Correctness", "Avg Lost", f"{avg_lost:.4f}"])

                # Add the category breakdown (C1_REG, C2, C3) to the CSV too
                self._export_dp_by_category_to_csv(writer, metrics)
