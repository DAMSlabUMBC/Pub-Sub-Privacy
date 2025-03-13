import threading
import queue
import sys
from pathlib import Path
from typing import TextIO
from GlobalDefs import ExitCode

class ResultLogger:
    
    SEED_LABEL: str = "SET_SEED"
    CONNECT_LABEL: str = "CONNECT"
    DISCONNECT_LABEL: str = "DISCONNECT"
    SUBSCRIBE_LABEL: str = "SUBSCRIBE"
    PUBLISH_LABEL: str = "PUBLISH"
    RECV_LABEL: str = "RECV"

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
        message = f"{self.SEED_LABEL}*{seed}"
        self.log_queue.put(message)
    
    def log_connect(self, timestamp, benchmark_id, client_id):
        message = f"{self.CONNECT_LABEL}*{timestamp}*{benchmark_id}*{client_id}"
        self.log_queue.put(message)
        
    def log_disconnect(self, timestamp, benchmark_id, client_id):
        message = f"{self.DISCONNECT_LABEL}*{timestamp}*{benchmark_id}*{client_id}"
        self.log_queue.put(message)

    def log_subscribe(self, timestamp, benchmark_id, client_id, topic_filter, purpose_filter):
        message = f"{self.SUBSCRIBE_LABEL}*{timestamp}*{benchmark_id}*{client_id}*{topic_filter}*{purpose_filter}"
        self.log_queue.put(message)

    def log_publish(self, timestamp, benchmark_id, client_id, message_id, topic_name, purpose, msg_type):
        message = f"{self.PUBLISH_LABEL}*{timestamp}*{benchmark_id}*{client_id}*{message_id}*{topic_name}*{purpose}*{msg_type}"
        self.log_queue.put(message)
        
    def log_recv(self, timestamp, benchmark_id, client_id, message_id, topic_name, msg_type):
        message = f"{self.RECV_LABEL}*{timestamp}*{benchmark_id}*{client_id}*{message_id}*{topic_name}*{msg_type}"
        self.log_queue.put(message)
