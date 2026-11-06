"""Import paths for the shared + case-study tests.

The Spark session (`spark`, `delta_spark` fixtures) lives in the repo-root conftest.py.
"""
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "shared"))
sys.path.insert(0, str(ROOT / "case-studies" / "01-contact-center-kpi-model"))
sys.path.insert(0, str(ROOT / "case-studies" / "03-tableau-prep-migration"))
