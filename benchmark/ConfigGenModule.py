import yaml
import os
import sys
from pathlib import Path
from GlobalDefs import ExitCode

"""Verifies the configuration exists and loads it in YAML format

Parameters
----------
file_path : str
    The path to the full configuration file
"""
def _load_full_config(file_path):

    # Verify the file exists
    if not os.path.exists(file_path):
        print(f"Full configuration file not found at {file_path}")
        sys.exit(ExitCode.BAD_ARGUMENT)

    # Attempt to load the file into memory
    with open(file_path, "r") as file:
        data = yaml.safe_load(file)
        if data is None:
            print(f"Full configuration file could not be parsed")
            sys.exit(ExitCode.MALFORMED_CONFIG)

        return data
    
# TODO: Currently splits every value equally, will need to update
#       when method of splitting is solidified
def _split_overall_properties(total, num_parts):
    base_value = total // num_parts
    remainder = total % num_parts
    distribution = [base_value] * num_parts

    for i in range(remainder):  
        distribution[i] += 1

    return distribution


"""Splits the full configuration file into per-node configuration files

Parameters
----------
full_config : str
    The path to the full configuration file
num_benchmarks : int
    The number of nodes among which to split the config
out_dir : str
    The output directory for the configuration files
"""
def split_config(full_config: str, num_benchmarks: int | None, out_dir: str):

    # Load the full config in YAML format
    full_config = _load_full_config(full_config)

    # Make output dir if it doesn't exist
    out_path = Path(out_dir)
    if out_path.exists() and not out_path.is_dir():
        print(f"Specified output directory {out_path} exists and is not a directory")
        sys.exit(ExitCode.BAD_ARGUMENT)

    out_path.mkdir(exist_ok=True, parents=True)

    # Find the name for each node
    node_names = list()

    # If the config specifies the IDs
    if "benchmark-ids" in full_config:

        # Make sure no number of nodes was provided or the number matches the ID count
        if num_benchmarks is not None and len(full_config["benchmark-ids"] != num_benchmarks):
            print(f"Specified node count {num_benchmarks} does not match the number of IDs in the config file. Did you mean to specifiy -n?")
            sys.exit(ExitCode.BAD_ARGUMENT)

        # Store the names
        node_names = full_config["benchmark-ids"]
        num_benchmarks = len(node_names)

    # Otherwise we need to manually assign IDs
    else:

        # Make sure we have a count
        if num_benchmarks is None:
            print(f"Node count must be specified with -n if the config file does not contain a list of IDs")
            sys.exit(ExitCode.BAD_ARGUMENT)

        for i in range(num_benchmarks):
            node_names.append(f"BenchmarkNode_{i+1}")

    # Parse overall properties
    properties = full_config["overall_properties"]
    split_data = {key: _split_overall_properties(value, num_benchmarks) for key, value in properties.items()}

    # Write config
    all_success = True
    for name in node_names:

        node_config = {"ID": i+1}  # make id 
        node_config.update({key: split_data[key][i] for key in split_data}) 

        # Write output file
        file_path = os.path.join(out_path, f"{name}.yml")
        try:
            with open(file_path, "w") as file:
                yaml.dump(node_config, file, default_flow_style=False)
        except IOError as e:
            print(f"Failed to write config for node {name} - {e}")
            all_success = False
            

    if not all_success:
        print("One or more files failed to generate.")


    print(f"Successfully generated node configuration files in {out_path}")