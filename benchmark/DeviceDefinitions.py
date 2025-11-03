from dataclasses import dataclass, field
from typing import Dict, Optional
import time


@dataclass
class PurposeDefinition:
    """Defines a purpose for data collection/usage"""
    id: str
    description: str


@dataclass
class DeviceDefinition:
    """Base class for device definitions"""
    id: str
    device_type: str  # "publisher" or "subscriber"


@dataclass
class PublisherDefinition(DeviceDefinition):
    """Publisher device with specific publication rate and payload size"""
    topic: str = ""
    initial_purpose: str = ""
    pub_period_ms: int = 1000
    min_payload_bytes: int = 100
    max_payload_bytes: int = 1000
    device_type: str = "publisher"


@dataclass
class SubscriberDefinition(DeviceDefinition):
    """Subscriber device with topic filter and purpose filter"""
    topic_filter: str = ""
    purpose_filter: str = ""
    device_type: str = "subscriber"


@dataclass
class DeviceInstance:
    """Active device instance - tracks runtime state"""
    instance_id: str
    device_definition: DeviceDefinition
    mqtt_client: any  # Will be mqtt.Client
    mqtt_client_name: str
    is_connected: bool = False
    is_publishing: bool = False
    current_purpose: Optional[str] = None
    last_publish_time_ms: float = 0.0
    message_count: int = 0
    subscribed_topics: Dict[str, str] = field(default_factory=dict)  # topic_filter -> purpose_filter

    def should_publish_now(self, current_time_ms: float) -> bool:
        """Check if device should publish based on publication period

        Parameters
        ----------
        current_time_ms : float
            Current time in ms since test start

        Returns
        -------
        bool
            True if enough time elapsed since last publish
        """
        if not self.is_connected or not self.is_publishing:
            return False

        if not isinstance(self.device_definition, PublisherDefinition):
            return False

        elapsed_ms = current_time_ms - self.last_publish_time_ms
        return elapsed_ms >= self.device_definition.pub_period_ms

    def mark_published(self, current_time_ms: float):
        """Update last publish time and increment message count"""
        self.last_publish_time_ms = current_time_ms
        self.message_count += 1

    def get_next_publish_time_ms(self) -> float:
        """Get the next time this device should publish"""
        if isinstance(self.device_definition, PublisherDefinition):
            return self.last_publish_time_ms + self.device_definition.pub_period_ms
        return float('inf')


class DeviceManager:
    """Manages device definitions and instances"""

    def __init__(self):
        self.device_definitions: Dict[str, DeviceDefinition] = {}
        self.device_instances: Dict[str, DeviceInstance] = {}
        self.purpose_definitions: Dict[str, PurposeDefinition] = {}

    def register_device_definition(self, device_def: DeviceDefinition):
        """Register a device definition"""
        self.device_definitions[device_def.id] = device_def
        print(f"[DeviceManager] Registered device definition: {device_def.id} ({device_def.device_type})")

    def register_purpose_definition(self, purpose_def: PurposeDefinition):
        """Register a purpose definition"""
        self.purpose_definitions[purpose_def.id] = purpose_def
        print(f"[DeviceManager] Registered purpose: {purpose_def.id}")

    def create_device_instance(self, device_def_id: str, instance_id: str,
                               mqtt_client: any, mqtt_client_name: str) -> DeviceInstance:
        """Create a device instance from a definition

        Parameters
        ----------
        device_def_id : str
            Device definition ID
        instance_id : str
            Instance ID
        mqtt_client : mqtt.Client
            MQTT client
        mqtt_client_name : str
            Client name

        Returns
        -------
        DeviceInstance
            Created device instance
        """
        if device_def_id not in self.device_definitions:
            raise ValueError(f"Unknown device definition: {device_def_id}")

        device_def = self.device_definitions[device_def_id]

        # Set initial purpose for publishers
        initial_purpose = None
        if isinstance(device_def, PublisherDefinition):
            initial_purpose = device_def.initial_purpose

        instance = DeviceInstance(
            instance_id=instance_id,
            device_definition=device_def,
            mqtt_client=mqtt_client,
            mqtt_client_name=mqtt_client_name,
            current_purpose=initial_purpose
        )

        self.device_instances[instance_id] = instance
        print(f"[DeviceManager] Created device instance: {instance_id} (from {device_def_id})")

        return instance

    def get_device_instance(self, instance_id: str) -> Optional[DeviceInstance]:
        """Get a device instance by ID"""
        return self.device_instances.get(instance_id)

    def get_all_publishers(self) -> list[DeviceInstance]:
        """Get all publisher instances"""
        return [inst for inst in self.device_instances.values()
                if isinstance(inst.device_definition, PublisherDefinition)]

    def get_all_subscribers(self) -> list[DeviceInstance]:
        """Get all subscriber instances"""
        return [inst for inst in self.device_instances.values()
                if isinstance(inst.device_definition, SubscriberDefinition)]

    def get_all_instances(self) -> list[DeviceInstance]:
        """Get all device instances"""
        return list(self.device_instances.values())

    def get_publishers_ready_to_publish(self, current_time_ms: float) -> list[DeviceInstance]:
        """Get all publishers that should publish now

        Parameters
        ----------
        current_time_ms : float
            Current time in ms since test start

        Returns
        -------
        list[DeviceInstance]
            Publishers due to publish
        """
        publishers = self.get_all_publishers()
        return [pub for pub in publishers if pub.should_publish_now(current_time_ms)]

    def clear(self):
        """Clear all definitions and instances"""
        self.device_definitions.clear()
        self.device_instances.clear()
        self.purpose_definitions.clear()
        print(f"[DeviceManager] Cleared all devices and purposes")
