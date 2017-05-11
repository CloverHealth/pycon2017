import json


def load_json_file(pathname:str):
    with open(pathname, 'r') as f:
        return json.load(f)
