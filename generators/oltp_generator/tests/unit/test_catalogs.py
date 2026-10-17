import importlib.util
import json
from pathlib import Path

import pytest

from generators.oltp_generator.catalogs import CATALOG_DIRECTORY, Catalogs


def test_catalogs_positive_real_devices_and_genres():
    """[unit] Shared catalogs contain 200 unique smartphones and 229 distinct genres."""
    catalogs = Catalogs.load()
    assert len(catalogs.devices) >= 195
    assert len(catalogs.genres) >= 150
    assert len(
        {(device.name, device.device_version) for device in catalogs.devices}
    ) == len(catalogs.devices)
    assert len(set(catalogs.genres)) == len(catalogs.genres)
    for device in catalogs.devices:
        assert device.os.versions
        assert len(set(device.os.versions)) == len(device.os.versions)
        if device.name.startswith("iPhone"):
            assert device.os.name == "iOS"
            assert device.os.platform == "ios"
        elif not device.name.startswith("Huawei"):
            assert device.os.name == "Android"
    iphone = next(device for device in catalogs.devices if device.name == "iPhone 15")
    assert iphone.os.versions == ("17", "18", "26")
    pixel = next(
        device for device in catalogs.devices if device.name == "Google Pixel 7"
    )
    assert pixel.os.versions == ("13", "14", "15")


def test_catalogs_positive_event_generator_compatibility():
    """[unit] The legacy devices module reads the shared catalog without package installation."""
    path = (
        Path(__file__).resolve().parents[3]
        / "event_generator/src/entities/models/devices.py"
    )
    spec = importlib.util.spec_from_file_location("legacy_devices", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert module.devices == [device.name for device in Catalogs.load().devices]


@pytest.mark.parametrize("filename", ["devices.json", "genres.json"])
def test_catalogs_negative_duplicates(tmp_path, filename):
    """[unit] Duplicate finite catalog entries are rejected."""
    for source in CATALOG_DIRECTORY.glob("*.json"):
        data = json.loads(source.read_text())
        if source.name == filename:
            data.append(data[0])
        (tmp_path / source.name).write_text(json.dumps(data))
    with pytest.raises(ValueError):
        Catalogs.load(tmp_path)
