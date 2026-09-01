from dataclasses import dataclass

@dataclass
class DeviceInfo:
    device_name: str
    device_version: str

    device_os: str
    device_os_version: str
