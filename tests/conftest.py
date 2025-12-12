# tests/conftest.py
import os
import sys

# Ensure the project root (one level up from tests/) is on sys.path
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)