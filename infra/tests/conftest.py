import os
import sys
from pathlib import Path

os.environ.setdefault("JSII_SILENCE_WARNING_UNTESTED_NODE_VERSION", "1")
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
