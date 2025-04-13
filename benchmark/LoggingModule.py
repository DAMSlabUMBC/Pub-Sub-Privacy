import threading
import queue
import sys
from pathlib import Path
from typing import TextIO
from GlobalDefs import ExitCode

SEED_LABEL: str = "SET_SEED"
PM_METHOD_LABEL: str = "SET_PURPOSE_MANAGEMENT_METHOD"
CONNECT_LABEL: str = "CONNECT"
DISCONNECT_LABEL: str = "DISCONNECT"
SUBSCRIBE_LABEL: str = "SUBSCRIBE"
PUBLISH_LABEL: str = "PUBLISH"
OP_PUBLISH_LABEL: str = "PUBLISH_OP"
RECV_LABEL: str = "RECV"
OP_RECV_LABEL: str = "RECV_OP"
SEPARATOR: str = "@@"

class ResultLogger:

    running: bool = False
    file_handle: TextIO | None = None
    log_queue: queue.Queue = queue.Queue()
    logging_thread: threading.Thread | None = None

    def __init__(self):
        self.logging_thread = threading.Thread(target=self._write_logs)
        self.logging_thread.daemon = True
        return

    """Opens the log file at filename and starts the logging thread
    
    Parameters
    ----------
    filename : str
        The filename to open for logging

    Raises
    ----------
    IOError
        If the file cannot be opened for writing
    RuntimeError
        If the thread cannot be started

    """
    def start(self, filename):

        # Don't set up if we're currently logging
        if self.running:
            raise RuntimeError("Attempted to start logging while already running")
        
        # If we're not running and the file is open, close it
        if self.file_handle is not None:
            self.file_handle.close()

        # Open the file for writing and start the processing thread
        try:
            # Make directories if needed
            logpath = Path(filename)
            if logpath.exists() and not logpath.is_dir():
                print(f"Specified output directory {logpath} exists and is not a directory")
                sys.exit(ExitCode.BAD_ARGUMENT)

            logpath.parent.mkdir(exist_ok=True, parents=True)

            # Open file and start logging
            self.file_handle = open(logpath, 'w')
            self.logging_thread.start()
            self.running = True
        except Exception:
            raise
        
        return


    """Flags the logging thread for shutdown, waits for termination, and closes log file"""
    def shutdown(self):
        # Append object to terminate queue processing when the final process is complete
        self.log_queue.put(None)

        # Block until all messages have been logged and thread has terminated
        self.logging_thread.join()

        # Close file handle
        if self.file_handle is not None and not self.file_handle.closed:
            self.file_handle.close()

        self.running = False
        self.file_handle = None

        return
            
    """The logging thread working function. Prints logs messages to file until None object is received"""
    def _write_logs(self):
        while True:

            # Blocking wait for log message
            message = self.log_queue.get()

            # If None received, end the thread
            if message is None:
                break

            # Log if file is open
            if self.file_handle is not None and not self.file_handle.closed:
                self.file_handle.write(message + '\n')

            # Notify queue that processing is complete
            self.log_queue.task_done()

        # Flush all pending writes before shutting down
        if self.file_handle is not None and not self.file_handle.closed:
            self.file_handle.flush()

        return
    
    # TODO finalize these
    def log_seed(self, seed):
        message = f"{SEED_LABEL}{SEPARATOR}{seed}"
        self.log_queue.put(message)
        
    def log_pm_method(self, pm_method):
        message = f"{PM_METHOD_LABEL}{SEPARATOR}{pm_method}"
        self.log_queue.put(message)
    
    def log_connect(self, timestamp, benchmark_id, client_id):
        message = f"{CONNECT_LABEL}{SEPARATOR}{timestamp}{SEPARATOR}{benchmark_id}{SEPARATOR}{client_id}"
        self.log_queue.put(message)
        
    def log_disconnect(self, timestamp, benchmark_id, client_id):
        message = f"{DISCONNECT_LABEL}{SEPARATOR}{timestamp}{SEPARATOR}{benchmark_id}{SEPARATOR}{client_id}"
        self.log_queue.put(message)

    def log_subscribe(self, timestamp, benchmark_id, client_id, topic_filter, purpose_filter, sub_id):
        message = f"{SUBSCRIBE_LABEL}{SEPARATOR}{timestamp}{SEPARATOR}{benchmark_id}{SEPARATOR}{client_id}{SEPARATOR}{topic_filter}{SEPARATOR}{purpose_filter}{SEPARATOR}{sub_id}"
        self.log_queue.put(message)

    def log_publish(self, timestamp, benchmark_id, client_id, corr_data, topic_name, purpose, msg_type):
        message = f"{PUBLISH_LABEL}{SEPARATOR}{timestamp}{SEPARATOR}{benchmark_id}{SEPARATOR}{client_id}{SEPARATOR}{topic_name}{SEPARATOR}{purpose}{SEPARATOR}{msg_type}{SEPARATOR}{corr_data}"
        self.log_queue.put(message)
        
    def log_operation_publish(self, timestamp, benchmark_id, client_id, corr_data, topic_name, purpose, op_type, op_category):
        message = f"{OP_PUBLISH_LABEL}{SEPARATOR}{timestamp}{SEPARATOR}{benchmark_id}{SEPARATOR}{client_id}{SEPARATOR}{topic_name}{SEPARATOR}{purpose}{SEPARATOR}{op_type}{SEPARATOR}{op_category}{SEPARATOR}{corr_data}"
        self.log_queue.put(message)
        
    def log_recv(self, timestamp, benchmark_id, recv_client_id, sending_client_id, corr_data, topic_name, msg_type, sub_id):
        message = f"{RECV_LABEL}{SEPARATOR}{timestamp}{SEPARATOR}{benchmark_id}{SEPARATOR}{recv_client_id}{SEPARATOR}{sending_client_id}{SEPARATOR}{topic_name}{SEPARATOR}{sub_id}{SEPARATOR}{msg_type}{SEPARATOR}{corr_data}"
        self.log_queue.put(message)
        
    def log_operation_recv(self, timestamp, benchmark_id, recv_client_id, sending_client_id, corr_data, topic_name, op_type, op_category, op_status, sub_id):
        message = f"{OP_RECV_LABEL}{SEPARATOR}{timestamp}{SEPARATOR}{benchmark_id}{SEPARATOR}{recv_client_id}{SEPARATOR}{sending_client_id}{SEPARATOR}{topic_name}{SEPARATOR}{sub_id}{SEPARATOR}{op_type}{SEPARATOR}{op_category}{SEPARATOR}{op_status}{SEPARATOR}{corr_data}"
        self.log_queue.put(message)
