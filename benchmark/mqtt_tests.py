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
    for client in clients:
        # Start timing the test
        start_time = time.time()
        
        client.initialize_test(f"Purpose Management Speed - Method {method.value}")

        client.subscribe_with_purpose(
            topic="data/location",
            purpose_filter="{billing,ads}/{auto,profiling}",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback

        sleep(1)

        for i in range(10):  
            client.publish_with_purpose(topic="data/location", purpose="billing/auto", msg="TestMessage", method=method)
            sleep(0.1)  

        elapsed_time = time.time() - start_time
        print(f"Time taken for purpose management test with method {method.value}: {elapsed_time:.2f} seconds")



def test_right_to_be_informed_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:
        client.initialize_test(f"Right to Be Informed Correctness - Method {method.value}")

        client.subscribe_with_purpose(
            topic="data/information",
            purpose_filter="informing",
            method=method
        )
        
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback

        sleep(1)

        client.publish_with_purpose(topic="data/information", purpose="informing", msg="Information", method=method)
        
        sleep(1)
        
        print(client.current_correcness_test_results.get_results())

    

def test_right_to_be_informed_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    return



def test_right_of_access_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:
        # Initialize the test for the right of access
        client.initialize_test(f"Right of Access Correctness - Method {method.value}")

        # Subscribe to a data topic with the specified purpose
        client.subscribe_with_purpose(
            topic="data/access",
            purpose_filter="data_access",
            method=method
        )
        
        # Set callback to handle access verification
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback

        # Wait for subscriptions to be activated
        sleep(1)

        # Publish an accessible data message
        client.publish_with_purpose(topic="data/access", purpose="data_access", msg="AccessibleData", method=method)

        # Publish a non-accessible data message (should be ignored)
        client.publish_with_purpose(topic="data/access", purpose="restricted_data", msg="RestrictedData", method=method)
        

        sleep(1)


        print(client.current_correcness_test_results.get_results())



def test_right_of_access_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:
        start_time = time.time()


        client.initialize_test(f"Right of Access Speed - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/access",
            purpose_filter="data_access",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback

        sleep(1)


        for i in range(20): 
            client.publish_with_purpose(topic="data/access", purpose="data_access", msg=f"AccessMessage_{i}", method=method)
            sleep(0.1)  


        elapsed_time = time.time() - start_time
        print(f"Time taken for right of access speed test with method {method.value}: {elapsed_time:.2f} seconds")


def test_right_to_portability_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Portability Correctness - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/portability",
            purpose_filter="data_portability",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)

        client.publish_with_purpose(topic="data/portability", purpose="data_portability", msg="PortableData", method=method)


        sleep(1)


        print(client.current_correcness_test_results.get_results())


def test_right_to_portability_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:
        start_time = time.time()


        client.initialize_test(f"Right to Portability Speed - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/portability",
            purpose_filter="data_portability",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        for i in range(20):
            client.publish_with_purpose(topic="data/portability", purpose="data_portability", msg=f"PortableMessage_{i}", method=method)
            sleep(0.1)

        elapsed_time = time.time() - start_time
        print(f"Time taken for right to portability speed test with method {method.value}: {elapsed_time:.2f} seconds")




def test_right_to_rectification_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Rectification Correctness - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/rectification",
            purpose_filter="original_data",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        client.publish_with_purpose(topic="data/rectification", purpose="original_data", msg="OriginalData", method=method)


        client.publish_with_purpose(topic="data/rectification", purpose="original_data", msg="UpdatedData", method=method)


        sleep(1)


        print(client.current_correcness_test_results.get_results())


def test_right_to_rectification_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:
        start_time = time.time()


        client.initialize_test(f"Right to Rectification Speed - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/rectification",
            purpose_filter="rectifiable_data",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        for i in range(20):
            client.publish_with_purpose(topic="data/rectification", purpose="rectifiable_data", msg=f"Update_{i}", method=method)
            sleep(0.1)


        elapsed_time = time.time() - start_time
        print(f"Time taken for right to rectification speed test with method {method.value}: {elapsed_time:.2f} seconds")


def test_right_to_erasure_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Erasure Correctness - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/erasure",
            purpose_filter="erasable_data",
            method=method
        )
        

        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback

        sleep(1)

  
        client.publish_with_purpose(topic="data/erasure", purpose="erasable_data", msg="pre_erasure", method=method)
        

        client.mqtt_client.unsubscribe("data/erasure")
        print("Client unsubscribed from data/erasure for erasure compliance")


        client.publish_with_purpose(topic="data/erasure", purpose="erasable_data", msg="post_erasure", method=method)


        sleep(1)


        print(client.current_correcness_test_results.get_results())


def test_right_to_erasure_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Erasure Speed - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/erasure",
            purpose_filter="erasable_data",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        start_time = time.time()
        client.mqtt_client.unsubscribe("data/erasure")
        print("Client unsubscribed from data/erasure")


        for i in range(10):
            client.publish_with_purpose(topic="data/erasure", purpose="erasable_data", msg=f"PostErasure_{i}", method=method)
            sleep(0.1)


        elapsed_time = time.time() - start_time
        print(f"Time taken for right to erasure speed test with method {method.value}: {elapsed_time:.2f} seconds")


def test_right_to_restriction_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Restriction Correctness - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/restriction",
            purpose_filter="unrestricted_data",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        client.publish_with_purpose(topic="data/restriction", purpose="unrestricted_data", msg="UnrestrictedData", method=method)


        client.mqtt_client.unsubscribe("data/restriction")
        print("Client restricted from data/restriction")


        client.publish_with_purpose(topic="data/restriction", purpose="restricted_data", msg="RestrictedData", method=method)

        sleep(1)
        print(client.current_correcness_test_results.get_results())


def test_right_to_restriction_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Restriction Speed - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/restriction",
            purpose_filter="unrestricted_data",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        start_time = time.time()


        client.mqtt_client.unsubscribe("data/restriction")
        print("Client restricted from receiving further messages on data/restriction")


        for i in range(10):
            client.publish_with_purpose(topic="data/restriction", purpose="restricted_data", msg=f"RestrictedMessage_{i}", method=method)
            sleep(0.1)


        elapsed_time = time.time() - start_time
        print(f"Time taken for right to restriction speed test with method {method.value}: {elapsed_time:.2f} seconds")


def test_right_to_object_correctness(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Object Correctness - Method {method.value}")

        client.subscribe_with_purpose(
            topic="data/object",
            purpose_filter="objectable_data",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        client.publish_with_purpose(topic="data/object", purpose="objectable_data", msg="ObjectableDataBeforeObjection", method=method)

        client.mqtt_client.unsubscribe("data/object")
        print("Client has objected to receiving data on data/object and unsubscribed")


        client.publish_with_purpose(topic="data/object", purpose="objectable_data", msg="ObjectableDataAfterObjection", method=method)


        sleep(1)


        print(client.current_correcness_test_results.get_results())


def test_right_to_object_speed(clients: list[BenchmarkClient], method: PurposeManagementMethod) -> None:
    for client in clients:

        client.initialize_test(f"Right to Object Speed - Method {method.value}")


        client.subscribe_with_purpose(
            topic="data/object",
            purpose_filter="objectable_data",
            method=method
        )
        client.mqtt_client.on_message = client.purpose_management_correctness_message_callback


        sleep(1)


        start_time = time.time()


        client.mqtt_client.unsubscribe("data/object")
        print("Client has objected to data on data/object and unsubscribed")

        for i in range(10):
            client.publish_with_purpose(topic="data/object", purpose="objectable_data", msg=f"ObjectedMessage_{i}", method=method)
            sleep(0.1)

        elapsed_time = time.time() - start_time
        print(f"Time taken for right to object speed test with method {method.value}: {elapsed_time:.2f} seconds")
