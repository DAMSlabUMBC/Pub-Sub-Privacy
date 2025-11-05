import time
from statistics import mean, variance
import requests
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import GlobalDefs
from LoggingModule import console_log, ConsoleLogLevel


@dataclass
class BrokerMetricsSample:
    """A single sample of broker metrics"""
    timestamp: float
    cpu_usage_percent: Optional[float] = None
    mem_usage_percent: Optional[float] = None

class BrokerMonitor:
    """Monitors broker resource usage via Prometheus Node Exporter"""
    
    last_cycle_cpu_active_time = None
    last_cycle_cpu_idle_time = None

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
        console_log(ConsoleLogLevel.INFO, f"Initialized with URL: {node_exporter_url}", __name__)

    def start_monitoring(self):
        """Start monitoring (allows samples to be collected)"""
        self.is_monitoring = True
        self.last_sample_time = time.time()
        console_log(ConsoleLogLevel.INFO, f"Started monitoring", __name__)

    def stop_monitoring(self):
        """Stop monitoring"""
        self.is_monitoring = False
        console_log(ConsoleLogLevel.INFO, f"Stopped monitoring (collected {len(self.samples)} samples)", __name__)

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
            sample = self._parse_prometheus_metrics(response.text)
   
            self.samples.append(sample)
            self.last_sample_time = sample.timestamp

            return sample

        except requests.exceptions.RequestException as e:
            console_log(ConsoleLogLevel.ERROR, f"Failed to collect metrics: {e}", __name__)
            return None
        except Exception as e:
            console_log(ConsoleLogLevel.ERROR, f"Unexpected error collecting metrics: {e}", __name__)
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

    def calculate_metrics(self) -> Tuple[Tuple[float, float, float, float], Tuple[float, float, float, float]]:
        """Calculate metrics across all samples

        Returns
        -------
        Tuple, Tuple
            Min/Max/Average/Variance values for cpu and mem respectively
        """
        if not self.samples:
            return ((-1, -1, -1, -1), (-1, -1, -1, -1))
        
        cpu_metrics: List[float] = list()
        mem_metrics: List[float] = list()

        for sample in self.samples:
            if sample.cpu_usage_percent is not None:
                cpu_metrics.append(sample.cpu_usage_percent)
            if sample.mem_usage_percent is not None:
                mem_metrics.append(sample.mem_usage_percent)
                
        cpu_tuple = (min(cpu_metrics), max(cpu_metrics), mean(cpu_metrics), variance(cpu_metrics))
        mem_tuple = (min(mem_metrics), max(mem_metrics), mean(mem_metrics), variance(mem_metrics))

        return cpu_tuple, mem_tuple

    def clear_samples(self):
        """Clear all collected samples"""
        sample_count = len(self.samples)
        self.samples = []
        console_log(ConsoleLogLevel.DEBUG, f"Cleared {sample_count} samples", __name__)

    def _parse_prometheus_metrics(self, text: str) -> BrokerMetricsSample:
        """Parse Prometheus metrics format

        Parameters
        ----------
        text : str
            Raw text from metrics endpoint

        Returns
        -------
        BrokerMetricsSample
            Sample of metrics
        """
        
        # Initialize counters for metrics that are on more than one line
        active_cpu_time = 0.0
        idle_cpu_time = 0.0
        mem_total = 0.0
        mem_free = 0.0
        
        for line in text.split('\n'):
            line = line.strip()

            # Skip comments and empty lines
            if not line or line.startswith('#'):
                continue

            # Parse metric lines
            parts = line.split()
            if len(parts) >= 2:
                
                # Check if it's a metric we need
                # CPU Usage
                if parts[0].startswith("node_cpu_seconds_total"):
                    if 'mode="idle"' in parts[0]:
                        idle_cpu_time += float(parts[1])
                    else:
                        active_cpu_time += float(parts[1])
                        
                # Memory usage
                if parts[0].startswith("node_memory_MemFree_bytes"):
                    mem_free = float(parts[1])
                    
                if parts[0].startswith("node_memory_MemTotal_bytes"):
                    mem_total = float(parts[1])
                
        # CPU usage must be calculated in terms of previous cycles since prometheus counts total time
        cpu_usage_pct = None
        if self.last_cycle_cpu_active_time is not None and self.last_cycle_cpu_idle_time is not None:
            delta_active = active_cpu_time - self.last_cycle_cpu_active_time
            delta_idle = idle_cpu_time - self.last_cycle_cpu_idle_time
            cpu_usage_pct = (delta_active / (delta_active + delta_idle)) * 100.0
            
        self.last_cycle_cpu_active_time = active_cpu_time
        self.last_cycle_cpu_idle_time = idle_cpu_time
        
        # Memory
        mem_usage_pct = (1.0 - (mem_free / mem_total)) * 100
        
        sample = BrokerMetricsSample(
            timestamp=time.time(),
            cpu_usage_percent=cpu_usage_pct,
            mem_usage_percent=mem_usage_pct
        )

        return sample

    def log_summary(self):
        """Log a summary of collected metrics"""
        if not self.samples:
            console_log(ConsoleLogLevel.WARNING, "No samples collected", __name__)
            return

        cpu_metrics, mem_metrics = self.calculate_metrics()
        GlobalDefs.LOGGING_MODULE.log_cpu_metrics(cpu_metrics[0], cpu_metrics[1], cpu_metrics[2], cpu_metrics[3])
        GlobalDefs.LOGGING_MODULE.log_mem_metrics(mem_metrics[0], mem_metrics[1], mem_metrics[2], mem_metrics[3])
