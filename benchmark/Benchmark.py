from enum import Enum
import importlib
import argparse

# Define enums for easier referencing later
class PurposeManagementMethod(Enum):
    PM_0 = "None"
    PM_1 = "Purpose-Enconding Topics"
    PM_2 = "Per-Message Declaration"
    PM_3 = "Registration by Message"
    PM_4 = "Registration by Topic"

class C1RightsMethod(Enum):
    C1_0 = "None"
    C1_1 = "Direct Publication"
    C1_2 = "Pre-Registration"

class C2RightsMethod(Enum):
    C2_0 = "None"
    C2_1 = "Direct Publication"
    C2_2 = "Broker-Facilitatied"

class C3RightsMethod(Enum):
    C3_0 = "None"
    C3_1 = "Direct Publication"
    C3_2 = "Broker-Facilitatied"

# Notes
# - Should add multiple test sizes by default, then allow tuning in config
# - Log level is required by module driver
# - Command line parameters override config

def main():
    
    # Read Command Line
    
    
    # Read Config
    
    
    # Configure Logging Module
    
    
    # Load Client Interface
    
    
    # Begin the Ready Notification Thread
    
    
    # Create Clients
    
    
    # Instantiate Subscribers
    
    
    # Notify Ready and Wait
    
    
    # All Ready - Start tests
    
    
    # Notify Done and Wait
    
    
    # All Done - Exit
    return 0
    
    

    # Reading from config should go here
    
    client_module = importlib.import_module(module_name)
    client_module.create_client()
    client_module.publish_with_purpose("msg1", "billing")
    client_module.subscribe_with_purpose("topic1", "billing2")
    

if __name__ == "__main__":
    main()