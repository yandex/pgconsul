"""
Version management for pgconsul.
"""

STATE_FILE = '/tmp/pgconsul.state'


def init_version():
    """Initialize version: read from package.release file, save to state file, and return it"""
    version = 'dev'
    try:
        # Try to find package.release in the installation directory
        with open('/opt/yandex/pgconsul/package.release', 'r') as f:
            version = f.read().strip()
    except Exception:
        pass
    
    # Save version to state file
    try:
        with open(STATE_FILE, 'w') as f:
            f.write(version)
    except Exception:
        pass
    
    return version


def get_version_from_state():
    """Read version from state file"""
    try:
        with open(STATE_FILE, 'r') as f:
            return f.read().strip()
    except Exception:
        return 'unknown'


__version__ = get_version_from_state()
