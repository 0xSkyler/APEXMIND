from dataclasses import replace
import numpy as np
import pytest
from apexmind.config import Config
from apexmind.dataset import build_dataset, load_dataset
from apexmind.demo import generate
from apexmind.latency import Latency
from apexmind.store import Store


@pytest.fixture
def config():
    c = Config()
    return replace(
        c, research=replace(c.research, min_train_rows=40, bootstrap_samples=30, folds=2)
    )


@pytest.fixture
def latency():
    return Latency(
        tuple(np.linspace(20, 80, 30)),
        tuple(np.linspace(20, 80, 30)),
        "SYNTHETIC_TEST_FIXTURE",
        "synthetic",
        0,
        True,
    )


@pytest.fixture
def dataset(tmp_path, config, latency):
    store = Store(tmp_path / "market.sqlite")
    generate(store, seconds=45)
    path = tmp_path / "dataset.npz"
    build_dataset(store, config, latency, str(path))
    store.close()
    data, metadata = load_dataset(path)
    return path, data, metadata
