import traceback
from datetime import datetime
from pathlib import Path
import sys

class ProcessingError(Exception):
    """Custom error for processing failures"""
    def __init__(self, message, function_name, file_name):
        self.message = message
        self.function_name = function_name
        self.file_name = file_name
        self.timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        super().__init__(self.message)

    def __str__(self):
        return f"""
{'='*50}
Error Details:
Time: {self.timestamp}
File: {self.file_name}
Function: {self.function_name}
Message: {self.message}
{'='*50}
"""

def handle_error(e, function_name, file_name):
    """Format and print error details"""
    try:
        error_msg = f"""
{'='*50}
Processing Error Detected!
Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Location: {file_name} -> {function_name}
Error: {str(e)}

Stack Trace:
{traceback.format_exc()}
{'='*50}
"""
        # Print to console using safe characters
        safe_msg = error_msg.encode('ascii', 'ignore').decode('ascii')
        print(safe_msg)

        # Log error to file with UTF-8 encoding
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / f"error_log_{datetime.now().strftime('%Y%m%d')}.txt"

        # Write with UTF-8 encoding and newline
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(error_msg + "\n")

    except Exception as logging_error:
        # Fallback error handling
        fallback_msg = f"""
{'='*50}
Error occurred while logging:
Original error: {str(e)}
Logging error: {str(logging_error)}
{'='*50}
"""
        print(fallback_msg)

def log_info(message, function_name=None):
    """Log informational messages"""
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_msg = f"[{timestamp}] {message}"
        if function_name:
            log_msg = f"[{timestamp}] [{function_name}] {message}"

        # Print to console using safe characters
        safe_msg = log_msg.encode('ascii', 'ignore').decode('ascii')
        print(safe_msg)

        # Log to file
        log_dir = Path('logs')
        log_dir.mkdir(exist_ok=True)
        info_log = log_dir / f"info_log_{datetime.now().strftime('%Y%m%d')}.txt"

        with open(info_log, 'a', encoding='utf-8') as f:
            f.write(log_msg + "\n")

    except Exception as e:
        print(f"Warning: Failed to log message - {str(e)}")

def format_message(message, message_type="INFO"):
    """Format messages consistently"""
    return f"""
{'='*50}
{message_type}: {message}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
{'='*50}
"""