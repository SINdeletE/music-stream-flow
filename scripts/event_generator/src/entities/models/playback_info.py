from dataclasses import dataclass
from uuid import UUID
from enum import Enum

class AudioQuality(Enum):
    low      = "low"
    medium   = "medium"
    high     = "high"
    lossless = "lossless"

@dataclass
class PlaybackInfo:
    playback_position_ms: int
    playback_volume: int

    playback_shuffle_enabled: bool = False
    playback_repeat_mode: str = "off"
    playback_speed: float = 1.0
    playback_is_offline: bool = False
    audio_quality: AudioQuality = AudioQuality.high
