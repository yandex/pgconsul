"""
Async logging module with bounded QueueHandler.
"""
# encoding: utf-8

import atexit
import logging
import logging.handlers
import queue
import sys
import threading
import traceback
from configparser import RawConfigParser
from logging.handlers import QueueHandler, QueueListener


_queue_listener: QueueListener | None = None


class _SilentDropQueueHandler(QueueHandler):
    """
    QueueHandler that silently drops logs when queue is full.
    """
    def enqueue(self, record: logging.LogRecord) -> None:
        """
        Enqueue record if queue not full, otherwise drop silently.
        """
        try:
            self.queue.put_nowait(record)
        except queue.Full:
            pass


def setup_async_logging(config: RawConfigParser, level: int, format: str, is_foreground: bool = False) -> None:
    """
    Configure async logging with bounded QueueHandler.
    """
    global _queue_listener

    if _queue_listener is not None:
        print('WARNING: Async logging already initialized. Call shutdown_async_logging() first if reinitialization is needed.', file=sys.stderr)
        return

    queue_size = int(config.get('global', 'async_log_queue_size'))
    if queue_size <= 0:
        print(f'WARNING: Invalid async_log_queue_size: {queue_size}, using default 5000', file=sys.stderr)
        queue_size = 5000
    elif queue_size > 100000:
        print(f'WARNING: async_log_queue_size very large: {queue_size}, may cause memory issues', file=sys.stderr)

    log_queue = queue.Queue(maxsize=queue_size)  # type: ignore
    queue_handler = _SilentDropQueueHandler(log_queue)

    if is_foreground:
        stream_handler = logging.StreamHandler(sys.stdout)
        handlers = [stream_handler]
    else:
        log_file = config.get('global', 'log_file')
        file_handler = logging.FileHandler(log_file, 'a')
        handlers = [file_handler]  # type: ignore

    for handler in handlers:
        handler.setFormatter(logging.Formatter(format, style='{'))

    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.handlers.clear()
    root_logger.addHandler(queue_handler)

    _queue_listener = QueueListener(log_queue, *handlers, respect_handler_level=True)
    _queue_listener.start()
    atexit.register(shutdown_async_logging)


def shutdown_async_logging() -> None:
    """
    Shutdown async logging, draining remaining queue.
    """
    global _queue_listener

    if _queue_listener:
        try:
            _queue_listener.stop()
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            print(f'ERROR during async logging shutdown: {e}', file=sys.stderr)
        _queue_listener = None
