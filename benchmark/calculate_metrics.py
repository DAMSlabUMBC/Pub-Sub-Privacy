#!/usr/bin/env python3
"""Calculate metrics from benchmark log files"""

import sys
import argparse
from pathlib import Path

# Add benchmark directory to path
sys.path.insert(0, str(Path(__file__).parent))

from MetricsCalculator import MetricsCalculator


def main():
    parser = argparse.ArgumentParser(
        description="Calculate system and correctness metrics from benchmark logs"
    )
    parser.add_argument(
        "log_file",
        type=str,
        help="Path to the benchmark log file"
    )
    parser.add_argument(
        "--test-name",
        type=str,
        default="test",
        help="Test name for report (default: test)"
    )
    parser.add_argument(
        "--csv-output",
        type=str,
        default=None,
        help="Path to export metrics as CSV (optional)"
    )
    parser.add_argument(
        "--no-print",
        action="store_true",
        help="Don't print metrics to console"
    )

    args = parser.parse_args()

    # Make sure the log file actually exists
    log_path = Path(args.log_file)
    if not log_path.exists():
        print(f"Error: Log file not found: {args.log_file}")
        sys.exit(1)

    # Run the metrics calculation
    print(f"Processing log file: {args.log_file}")
    calculator = MetricsCalculator()
    metrics = calculator.calculate_all_metrics(args.log_file, args.test_name)

    if metrics is None:
        print("Error: Failed to calculate metrics")
        sys.exit(1)

    # Print the results to console
    if not args.no_print:
        calculator.print_metrics(metrics)

    # Save to CSV if they want that
    if args.csv_output:
        calculator.export_metrics_to_csv(metrics, args.csv_output)
        print(f"Metrics exported to: {args.csv_output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
