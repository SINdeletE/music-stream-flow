from dataclasses import dataclass
from uuid import UUID

@dataclass
class DeviceInfo:
    device_installation_id: UUID
    device_name: str
    device_version: str

    device_os: str
    device_os_version: str
