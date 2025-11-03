import yaml
import os
import sys
from typing import List, Dict
from GlobalDefs import ExitCode, PurposeManagementMethod
from ConfigParsingModule import ConfigParser, BenchmarkConfiguration
from DeterministicTestExecutor import DeterministicTestConfiguration


class DeterministicConfigParser(ConfigParser):
    """Extended ConfigParser with deterministic test support"""

    def parse_config(self, file_path) -> BenchmarkConfiguration:
        """Parse config file with legacy and deterministic format support"""
        # Verify file exists
        if not os.path.exists(file_path):
            print(f"Configuration file not found at {file_path}")
            sys.exit(ExitCode.BAD_ARGUMENT)

        # Load YAML
        with open(file_path, "r") as file:
            data = yaml.safe_load(file)
            if data is None:
                print(f"Configuration file could not be parsed")
                sys.exit(ExitCode.MALFORMED_CONFIG)

        # Parse base configuration
        self._parse_config_yaml(data)

        # Check if test uses deterministic mode
        test_data = data.get("test", {})
        if test_data.get("use_deterministic_scheduling", False):
            # Parse deterministic test configuration
            self._parse_deterministic_test(data, test_data)

        return self.the_config

    def _parse_deterministic_test(self, data: Dict, test_data: Dict):
        """Parse deterministic test config with devices and events"""
        # Get the test config that was already created
        test_config = self.the_config.test_list[0]

        # Convert to DeterministicTestConfiguration
        det_config = DeterministicTestConfiguration(
            name=test_config.name,
            test_duration_ms=test_config.test_duration_ms,
            client_count=0  # Not used in deterministic mode
        )

        # Copy over base fields
        det_config.qos = test_config.qos

        # Set deterministic mode flag
        det_config.use_deterministic_scheduling = True

        # Parse purpose definitions
        det_config.purpose_definitions = self._parse_purpose_definitions(data)

        # Parse device definitions
        det_config.device_definitions = self._parse_device_definitions(data)

        # Parse device instances
        det_config.device_instances_config = self._parse_device_instances(test_data)

        # Parse scheduled events
        det_config.scheduled_events = self._parse_scheduled_events(test_data)

        # Parse broker monitoring settings
        det_config.monitor_broker = data.get('monitor_broker', False)
        det_config.node_exporter_url = data.get('node_exporter_url', 'http://localhost:9100/metrics')
        det_config.monitor_interval_ms = data.get('monitor_interval_ms', 1000)

        # Replace test config with deterministic version
        self.the_config.test_list[0] = det_config

        print(f"[DeterministicConfigParser] Parsed deterministic configuration:")
        print(f"  - Purposes: {len(det_config.purpose_definitions)}")
        print(f"  - Device definitions: {len(det_config.device_definitions)}")
        print(f"  - Device instances: {len(det_config.device_instances_config)}")
        print(f"  - Scheduled events: {len(det_config.scheduled_events)}")

    def _parse_purpose_definitions(self, data: Dict) -> Dict:
        """Parse purpose definitions from config"""
        purposes = {}

        purpose_defs = data.get('purpose_definitions', [])
        for purpose_def in purpose_defs:
            purpose_id = purpose_def.get('id')
            if not purpose_id:
                raise Exception("purpose_definitions entry missing 'id'")

            purposes[purpose_id] = {
                'description': purpose_def.get('description', '')
            }

        return purposes

    def _parse_device_definitions(self, data: Dict) -> Dict:
        """Parse device definitions from config"""
        devices = {}

        device_defs = data.get('device_definitions', [])
        for device_def in device_defs:
            device_id = device_def.get('id')
            if not device_id:
                raise Exception("device_definitions entry missing 'id'")

            device_type = device_def.get('type')
            if not device_type:
                raise Exception(f"device_definitions entry {device_id} missing 'type'")

            if device_type == 'publisher':
                devices[device_id] = self._parse_publisher_definition(device_def)
            elif device_type == 'subscriber':
                devices[device_id] = self._parse_subscriber_definition(device_def)
            else:
                raise Exception(f"Unknown device type: {device_type}")

        return devices

    def _parse_publisher_definition(self, device_def: Dict) -> Dict:
        """Parse publisher device definition"""
        device_id = device_def['id']

        # Required fields
        required_fields = ['topic', 'initial_purpose', 'pub_period_ms',
                          'min_payload_bytes', 'max_payload_bytes']
        for field in required_fields:
            if field not in device_def:
                raise Exception(f"Publisher {device_id} missing required field: {field}")

        return {
            'type': 'publisher',
            'topic': device_def['topic'],
            'initial_purpose': device_def['initial_purpose'],
            'pub_period_ms': device_def['pub_period_ms'],
            'min_payload_bytes': device_def['min_payload_bytes'],
            'max_payload_bytes': device_def['max_payload_bytes']
        }

    def _parse_subscriber_definition(self, device_def: Dict) -> Dict:
        """Parse subscriber device definition"""
        device_id = device_def['id']

        # Required fields
        required_fields = ['topic_filter', 'purpose_filter']
        for field in required_fields:
            if field not in device_def:
                raise Exception(f"Subscriber {device_id} missing required field: {field}")

        return {
            'type': 'subscriber',
            'topic_filter': device_def['topic_filter'],
            'purpose_filter': device_def['purpose_filter']
        }

    def _parse_device_instances(self, test_data: Dict) -> List:
        """Parse device instance configurations"""
        instances = []

        device_instances = test_data.get('device_instances', [])
        for instance_config in device_instances:
            device_def_id = instance_config.get('device_def_id')
            if not device_def_id:
                raise Exception("device_instances entry missing 'device_def_id'")

            instance_id = instance_config.get('instance_id')
            if not instance_id:
                raise Exception("device_instances entry missing 'instance_id'")

            instances.append({
                'device_def_id': device_def_id,
                'instance_id': instance_id,
                'count': instance_config.get('count', 1)
            })

        return instances

    def _parse_scheduled_events(self, test_data: Dict) -> List:
        """Parse scheduled events"""
        events = []

        scheduled_events = test_data.get('scheduled_events', [])
        for event_config in scheduled_events:
            time_ms = event_config.get('time_ms')
            if time_ms is None:
                raise Exception("scheduled_events entry missing 'time_ms'")

            event_type = event_config.get('type')
            if not event_type:
                raise Exception("scheduled_events entry missing 'type'")

            # Copy entire config as params
            event = {
                'time_ms': time_ms,
                'type': event_type,
                'description': event_config.get('description', '')
            }

            # Add type-specific parameters
            if event_type in ['connect', 'disconnect', 'reconnect',
                            'start_publishing', 'stop_publishing']:
                event['devices'] = event_config.get('devices', [])

            elif event_type == 'change_purpose':
                event['device'] = event_config.get('device')
                event['new_purpose'] = event_config.get('new_purpose')
                if not event['device'] or not event['new_purpose']:
                    raise Exception("change_purpose event requires 'device' and 'new_purpose'")

            events.append(event)

        return events
