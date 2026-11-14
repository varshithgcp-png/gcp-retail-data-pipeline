from pathlib import Path
import yaml

def load_entities(path: str):
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return payload["entities"]

def load_rules(path: str):
    payload = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    return payload["rules"]
