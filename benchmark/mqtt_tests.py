from time import sleep

# Local imports
from BenchmarkClient import *

## Purpose Management ##

def test_purpose_management_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:

    for client in clients:

        # Set up for test
        client.initialize_test(f"Purpose Management Correctness - Method {method.value}")

        # Subscribe to test purpose filter
        client.subscribe_with_purpose(topic="data/location", purpose_filter="{billing,ads}/{auto,profiling}", method=method)
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback

        # Give time for clients to subscribe
        sleep(1)

        # Publish messages
        client.publish_with_purpose(topic="data/location", purpose="billing/auto", msg="GOOD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="ads/auto", msg="GOOD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="billing/profiling", msg="GOOD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="ads/profiling", msg="GOOD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="billing", msg="BAD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="ads", msg="BAD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="tracking/auto", msg="BAD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="tracking", msg="BAD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="billing/auto/additional", msg="BAD", method=method)
        client.publish_with_purpose(topic="data/location", purpose="billing/middle/auto", msg="BAD", method=method)

        # Give time for clients to respond
        sleep(1)

        # Check expected results
        print(client.current_correcness_test_results.get_results())


def test_purpose_management_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

## C1 Rights ##

def test_right_to_be_informed_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_be_informed_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

## C2 Rights ##

def test_right_of_access_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_of_access_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_portability_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_portability_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

## C3 Rights ##

def test_right_to_rectification_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_rectification_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_erasure_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_erasure_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_restriction_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_restriction_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_object_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return

def test_right_to_object_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return
