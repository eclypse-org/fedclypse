import datetime
from dataclasses import field, dataclass

from typing import Dict


@dataclass
class Message:
    """
    A message is a simple data structure that is used to communicate between
    nodes. It contains a header, a sender ID, a timestamp, and a body. The body
    is a dictionary that can contain any data that is needed to be communicated
    between nodes.
    """

    header: str = None
    sender_id: str = None
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    body: Dict = field(default_factory=dict)
