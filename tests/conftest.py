import sys
from pathlib import Path

# Let tests `import bls_ingest`, `import common`, etc. as if running inside
# Databricks, where the ingestion/ directory is on sys.path automatically.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "ingestion"))
