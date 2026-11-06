"""Import path for P1 source modules.

The Spark session (`spark` fixture) lives in the repo-root conftest.py.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
