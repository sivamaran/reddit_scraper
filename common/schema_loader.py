import json
import os

def load_schema():
    """Load schema_template.json from the common folder"""
    base_dir = os.path.dirname(__file__)
    schema_path = os.path.join(base_dir, "schema_template.json")
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)
