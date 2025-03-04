import sys
from os import path
import argparse
from GlobalDefs import *
import ConfigGenModule
import TestManagementModule
import ResultAnalysisModule

def main():

    # Read Command Line
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands',
                                       help="Specify whether to generate configuration files, run the benchmark, or process benchmark outputs")
    subparsers.required = True
    subparsers.dest = "command"

    split_config_parser = subparsers.add_parser("genconfig")
    split_config_parser.add_argument("full_config", help="The path to the top level configuration file")
    split_config_parser.add_argument("-n" "--node_count", dest="node_count", type=int, help="The number of nodes across which to split the configuration file (required if benchmark-ids are not defined in the config)")
    split_config_parser.add_argument("-o", "--out_dir", dest="out_dir", default="./configs", help="The directory in which to store the configs (default: ./configs)")

    run_benchmark_parser = subparsers.add_parser("run")
    run_benchmark_parser.add_argument("config", help="The path to the node-specific configuration file")
    run_benchmark_parser.add_argument("broker_address", help="The IP or FQDN of the broker")
    run_benchmark_parser.add_argument("-p", "--port", dest="port", default=1883, help="The port on which to connect (default: 1883)")
    run_benchmark_parser.add_argument("-o", "--logfile", dest="logfile", help="The logfile in which to store the output (default: 'logs/<benchmark_id_from_cfg>_YYYY-MM-DD_HH-MM-SS.log')")

    analyze_results_parser = subparsers.add_parser("analyze")
    analyze_results_parser.add_argument("log_dir", help="The path to the directory holding the log files to analyze")
    analyze_results_parser.add_argument("-o" "--outfile", dest="outfile", help="The file in which to store the results (default: 'BenchmarkResults_YYYY-MM-DD_HH-MM-SS.txt')")

    args = parser.parse_args()

    # Validate arguments
    if not _validate_arguments(args):
        sys.exit(ExitCode.BAD_ARGUMENT)

    # Perform relevation operation
    if args.command == "genconfig":
        ConfigGenModule.split_config(args.full_config, args.node_count, args.out_dir)
    elif args.command == "run":
        TestManagementModule.configure_and_run_tests(args.config, args.broker_address, args.port, args.logfile)
    elif args.command == "analyze":
        ResultAnalysisModule.analyze_results(args.log_dir, args.outfile)
    else:
        # We should never get here as the argument validation should handle 
        # existing on malformed arguments
        sys.exit(ExitCode.UNKNOWN_ERROR)

    # If we completed, everything was okay
    sys.exit(ExitCode.SUCCESS)


"""Validates the basic format of the arguments. This function does not validate that files can be opened or that IP addresses can be reached.

Parameters
----------
args : argparse.Namespace
    The parsed arguments

Returns
----------
bool
    True if the arguments are valid, false otherwise
"""
def _validate_arguments(args: argparse.Namespace) -> bool:

    # Check subcommand
    if args.command == "genconfig":
        
        # Full configuration must exist
        if not path.isfile(args.full_config):
            print(f"Cannot find configuration file at {args.full_config}")
            return False
        
        # Benchmark number must be positive
        if args.node_count is not None and args.node_count <= 0:
            print(f"Node count must be positive")
            return False
        
        # Out dir must either not exist or be a directory
        if not path.isdir(args.out_dir) and path.exists(args.out_dir):
            print(f"{args.out_dir} exists and is not a directory")
            return False

    elif args.command == "run":

        # Node configuration must exist
        if not path.isfile(args.config):
            print(f"Cannot find configuration file at {args.config}")
            return False
        
        # Since broker address can be a FQDN, we don't validate here
        
        # Port must be valid
        if not 1 <= args.port <= 65535:
            print(f"Port must be in the range [1-66535]")
            return False
        
        # Logfile will be validated on open

    elif args.command == "analyze":
                
        # Log directories must exist
        if not path.isdir(args.log_dir):
            print(f"Cannot find log file directory at {args.log_dir}")
            return False

        # Outfile will be validated on open

    # Invalid subcommand
    else:
        return False
    
    # All passed
    return True


if __name__ == "__main__":
    main()