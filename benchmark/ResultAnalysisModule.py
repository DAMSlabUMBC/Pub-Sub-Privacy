from os import path
import time
from GlobalDefs import *

def analyze_results(log_dir: str, out_file : str | None):

    # Create result file path if it doesn't exist
    if out_file is None:
        timestring = time.strftime("%Y-%m-%d_%H-%M-%S")
        out_file = f"BenchmarkResults_{timestring}.txt"

    # TODO
    print("In calc")

    return