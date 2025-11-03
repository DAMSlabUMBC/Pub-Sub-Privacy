import sys
from os import path
import argparse
from GlobalDefs import *
import TestManagementModule
import ResultAnalysisModule

def main():

    # Read Command Line
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title='subcommands',
                                       help="Specify whether to run the benchmark or process benchmark outputs")
    subparsers.required = True
    subparsers.dest = "command"

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
    if args.command == "run":
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
    if args.command == "run":

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