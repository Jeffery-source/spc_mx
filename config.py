import yaml
import os


BASE_DIR = os.path.dirname(
    os.path.abspath(__file__)
)


def load_config():

    config_path = os.path.join(
        BASE_DIR,
        "config.yaml"
    )

    with open(
        config_path,
        "r",
        encoding="utf-8"
    ) as f:

        return yaml.safe_load(f)