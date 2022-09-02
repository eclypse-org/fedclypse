import ray

from ._private.broker import FedRayBroker, Message
from ._private.queue import Queue

from functools import cached_property
from typing import Dict, List, Literal, Optional, Union


class FedRayNode(object):

    def __init__(self, 
                 node_id: str,
                 mode: Literal['sync', 'async'] = 'sync', 
                 timeout: Optional[float] = None, 
                 **kwargs):
        self._id: str = node_id
        self._broker: FedRayBroker = ray.get_actor('broker')
        self._message_queue: Queue = Queue()

        self._mode: Literal['sync', 'async'] = mode
        if mode == 'async' and (timeout is None or timeout <= 0):
            raise ValueError("Timeout must be a value > 0 for async nodes.")
        self._timeout: float = timeout

        self.build(**kwargs)
    
    def send(self, msg_type: str, body: Dict, ids: Optional[Union[str, List[str]]] = None):
        msg = Message(type=msg_type, sender_id=self._id, body=body)
        ray.get([self._broker.publish.remote(self.id, msg, ids)])
    
    def get_message(self, block: Optional[bool] = None, timeout: Optional[float] = None) -> Message:
        if block is None:
            block = self.mode == 'sync'
        if not block and timeout is None:
            timeout = self._timeout
        msg = self._message_queue.get(block=block, timeout=timeout)
        return msg.type, msg.timestamp, msg.body
    
    def build(self, **kwargs):
        raise NotImplementedError
    
    def _endpoint(self, msg: ray.ObjectRef):
        self._message_queue.put(msg)
        return True
    
    def _invalidate_neighbors(self):
        del self.neighbors
    
    @property
    def mode(self) -> str:
        return self._mode
    
    @property
    def id(self) -> str:
        return self._id

    @cached_property
    def neighbors(self) -> List[str]:
        return ray.get(self._broker.get_neighbors.remote(self.id))
