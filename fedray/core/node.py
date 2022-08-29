import ray
import torch
from queue import Queue

from typing import Dict, List, Literal, Optional, Union

from .broker import FedRayBroker, Message
from functools import cached_property


#@ray.remote
class FedRayNode(object):

    def __init__(self, id: str):
        self._id: str = id
        self._broker: FedRayBroker = None
        self._message_queue: Queue = None

        self.model: torch.nn.Module = None

    def run(self):
        raise NotImplementedError
    
    def send(self, msg_type: Literal['model', 'logic'], body: Dict, ids: Optional[Union[str, List[str]]]):
        msg = Message(type=msg_type, sender_id=self._id, body=body)
        msg_ref = ray.put(msg)
        return ray.get(self._broker.publish.remote(self.id, [msg_ref]), ids)
    
    def message_endpoint(self, msg: ray.ObjectRef):
        self._message_queue.put(msg)
    
    def get_message(self, block: bool = True, timeout: Optional[float] = None) -> Message:
        self._message_queue.get(block=block, timeout=timeout)
    
    @property
    def id(self) -> str:
        return self._id

    @cached_property
    def neighbors(self) -> List[str]:
        return ray.get(self._broker.get_neighbors.remote(self.id))
    
    def invalidate_neighbors(self):
        del self.neighbors