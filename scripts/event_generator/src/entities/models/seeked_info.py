from dataclasses import dataclass

@dataclass
class SeekedInfo:
    seeked_from_ms: int
    seeked_to_ms: int
    is_user_initiated: bool = True