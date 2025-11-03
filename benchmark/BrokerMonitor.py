import time
import requests
from typing import Dict, List, Optional
from dataclasses import dataclass


@dataclass
class BrokerMetricsSample:
    """A single sample of broker metrics"""
    timestamp: float
    cpu_usage_percent: Optional[float] = None
    memory_used_bytes: Optional[float] = None
    network_rx_bytes: Optional[float] = None
    network_tx_bytes: Optional[float] = None
    raw_metrics: Dict[str, float] = None

    def __post_init__(self):
        if self.raw_metrics is None:
            self.raw_metrics = {}


class BrokerMonitor:
    """Monitors broker resource usage via Prometheus Node Exporter"""

    def __init__(self, node_exporter_url: str = "http://localhost:9100/metrics"):
        """Initialize broker monitor

        Parameters
        ----------
        node_exporter_url : str
            Node Exporter metrics endpoint URL
        """
        self.node_exporter_url = node_exporter_url
        self.samples: List[BrokerMetricsSample] = []
        self.is_monitoring = False
        self.last_sample_time = 0.0
        print(f"[BrokerMonitor] Initialized with URL: {node_exporter_url}")

    def start_monitoring(self):
        """Start monitoring (allows samples to be collected)"""
        self.is_monitoring = True
        self.last_sample_time = time.time()
        print(f"[BrokerMonitor] Started monitoring")

    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_monitoring = False
        print(f"[BrokerMonitor] Stopped monitoring (collected {len(self.samples)} samples)")

    def collect_sample(self) -> Optional[BrokerMetricsSample]:
        """Collect current broker metrics from Node Exporter

        Returns
        -------
        BrokerMetricsSample or None
            Metrics sample, or None if collection failed
        """
        if not self.is_monitoring:
            return None

        try:
            # Fetch metrics from Node Exporter
            response = requests.get(self.node_exporter_url, timeout=5)
            response.raise_for_status()

            # Parse Prometheus metrics format
            raw_metrics = self._parse_prometheus_metrics(response.text)

            # Extract relevant metrics
            sample = BrokerMetricsSample(
                timestamp=time.time(),
                cpu_usage_percent=self._calculate_cpu_usage(raw_metrics),
                memory_used_bytes=self._calculate_memory_usage(raw_metrics),
                network_rx_bytes=raw_metrics.get('node_network_receive_bytes_total'),
                network_tx_bytes=raw_metrics.get('node_network_transmit_bytes_total'),
                raw_metrics=raw_metrics
            )

            self.samples.append(sample)
            self.last_sample_time = sample.timestamp

            return sample

        except requests.exceptions.RequestException as e:
            print(f"[BrokerMonitor] ERROR: Failed to collect metrics: {e}")
            return None
        except Exception as e:
            print(f"[BrokerMonitor] ERROR: Unexpected error collecting metrics: {e}")
            return None

    def should_collect_sample(self, interval_ms: float) -> bool:
        """Check if enough time passed to collect another sample

        Parameters
        ----------
        interval_ms : float
            Minimum time between samples in ms

        Returns
        -------
        bool
            True if should collect now
        """
        if not self.is_monitoring:
            return False

        elapsed_ms = (time.time() - self.last_sample_time) * 1000.0
        return elapsed_ms >= interval_ms

    def get_samples(self) -> List[BrokerMetricsSample]:
        """Get all collected samples"""
        return self.samples

    def get_average_metrics(self) -> Dict[str, float]:
        """Calculate average metrics across all samples

        Returns
        -------
        dict
            Average values for each metric
        """
        if not self.samples:
            return {}

        total_samples = len(self.samples)
        averages = {
            'cpu_usage_percent': 0.0,
            'memory_used_bytes': 0.0,
            'network_rx_bytes': 0.0,
            'network_tx_bytes': 0.0
        }

        for sample in self.samples:
            if sample.cpu_usage_percent is not None:
                averages['cpu_usage_percent'] += sample.cpu_usage_percent
            if sample.memory_used_bytes is not None:
                averages['memory_used_bytes'] += sample.memory_used_bytes
            if sample.network_rx_bytes is not None:
                averages['network_rx_bytes'] += sample.network_rx_bytes
            if sample.network_tx_bytes is not None:
                averages['network_tx_bytes'] += sample.network_tx_bytes

        # Calculate averages
        for key in averages:
            averages[key] /= total_samples

        return averages

    def clear_samples(self):
        """Clear all collected samples"""
        sample_count = len(self.samples)
        self.samples = []
        print(f"[BrokerMonitor] Cleared {sample_count} samples")

    def _parse_prometheus_metrics(self, text: str) -> Dict[str, float]:
        """Parse Prometheus metrics format

        Parameters
        ----------
        text : str
            Raw text from metrics endpoint

        Returns
        -------
        dict
            Metric names to values
        """
        metrics = {}
        for line in text.split('\n'):
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue

            # Parse metric lines
            # Format: metric_name{labels} value timestamp
            # We'll do simple parsing without label support for now
            parts = line.split()
            if len(parts) >= 2:
                metric_name = parts[0].split('{')[0]  # Remove labels
                try:
                    metric_value = float(parts[1])
                    metrics[metric_name] = metric_value
                except ValueError:
                    continue

        return metrics

    def _calculate_cpu_usage(self, metrics: Dict[str, float]) -> Optional[float]:
        """Calculate CPU usage percentage (simplified - requires delta tracking)

        Parameters
        ----------
        metrics : dict
            Raw metrics

        Returns
        -------
        float or None
            CPU usage percentage estimate
        """
        # For a simple estimate, we look at the idle time
        # In a real implementation, you'd calculate delta between samples
        cpu_idle = metrics.get('node_cpu_seconds_total')
        if cpu_idle is not None:
            # This is a very rough estimate - proper calculation requires deltas
            return 0.0  # Placeholder - proper implementation would track deltas

        return None

    def _calculate_memory_usage(self, metrics: Dict[str, float]) -> Optional[float]:
        """Calculate memory usage in bytes

        Parameters
        ----------
        metrics : dict
            Raw metrics

        Returns
        -------
        float or None
            Memory used in bytes
        """
        mem_total = metrics.get('node_memory_MemTotal_bytes')
        mem_available = metrics.get('node_memory_MemAvailable_bytes')

        if mem_total is not None and mem_available is not None:
            return mem_total - mem_available

        return None

    def print_summary(self):
        """Print a summary of collected metrics"""
        if not self.samples:
            print("[BrokerMonitor] No samples collected")
            return

        averages = self.get_average_metrics()

        print("\n[BrokerMonitor] Metrics Summary")
        print("-" * 60)
        print(f"Total samples: {len(self.samples)}")
        print(f"Avg CPU usage: {averages['cpu_usage_percent']:.2f}%")
        print(f"Avg Memory used: {averages['memory_used_bytes'] / 1024 / 1024:.2f} MB")
        print(f"Avg Network RX: {averages['network_rx_bytes'] / 1024 / 1024:.2f} MB")
        print(f"Avg Network TX: {averages['network_tx_bytes'] / 1024 / 1024:.2f} MB")
        print("-" * 60)
