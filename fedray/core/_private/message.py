from dataclasses import field, dataclass
import datetime

from typing import Dict, Literal

@dataclass
class Message:
    type: Literal['model', 'logic'] = None
    sender_id: str = None
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    body: Dict = field(default_factory=dict)