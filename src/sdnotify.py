import os
import socket


class Notifier:
    def __init__(self, debug=False):
        """Instantiate a new notifier object. This will initiate a connection
        to the systemd notification socket.
        Normally this method silently ignores exceptions (for example, if the
        systemd notification socket is not available) to allow applications to
        function on non-systemd based systems. However, setting debug=True will
        cause this method to raise any exceptions generated to the caller, to
        aid in debugging.
        """
        self.debug = debug
        try:
            self.socket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            address = os.getenv('NOTIFY_SOCKET')
            if address[0] == '@':
                address = '\0' + address[1:]
            self.socket.connect(address)
        except Exception:
            self.socket = None
            if self.debug:
                raise

    def _send(self, msg):
        """Send string `msg` as bytes on the notification socket"""
        if self.enabled():
            try:
                self.socket.sendall(msg.encode())
            except Exception:
                if self.debug:
                    raise

    def enabled(self):
        """Return a boolean stating whether watchdog is enabled"""
        return bool(self.socket)

    def ready(self):
        """Report ready service state, i.e. completed initialisation"""
        self._send("READY=1\n")

    def status(self, msg):
        """Set a service status message"""
        self._send("STATUS=%s\n" % (msg,))

    def notify(self):
        """Report a healthy service state"""
        self._send("WATCHDOG=1\n")

    def notify_error(self, msg=None):
        """
        Report a watchdog error. This program will likely be killed by the
        service manager.
        If `msg` is not None, it will be reported as an error message to the
        service manager.
        """
        if msg:
            self.status(msg)

        self._send("WATCHDOG=trigger\n")
