from pathlib import Path
import sys


SCRIPTS = str(Path(__file__).resolve().parents[2])
if SCRIPTS not in sys.path:
    sys.path.insert(0, SCRIPTS)
