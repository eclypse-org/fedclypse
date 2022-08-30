import ray
from fedray.core._broker import FedRayBroker, Message

from queue import Queue
from functools import cached_property

from typing import Dict, List, Literal, Optional, Union


def node(mode: Literal['sync', 'async'], timeout: Optional[float] = None):

    def _node(cls):

        @ray.remote
        class FedRayNode(cls):

            def __init__(self, node_id: str, *args, **kwargs):
                self._id: str = node_id
                self._broker: FedRayBroker = ray.get_actor('broker')
                self._message_queue: Queue = Queue()

                self._mode: Literal['sync', 'async'] = mode
                if mode == 'async' and (timeout is None or timeout <= 0):
                    raise ValueError("Timeout must be a value > 0 for async nodes.")
                self._timeout: float = timeout

                super(FedRayNode, self).__init__(self, *args, **kwargs)
            
            def send(self, msg_type: str, body: Dict, ids: Optional[Union[str, List[str]]]):
                msg = Message(type=msg_type, sender_id=self._id, body=body)
                msg_ref = ray.put(msg)
                return ray.get(self._broker.publish.remote(self.id, [msg_ref]), ids)
            
            def endpoint(self, msg: ray.ObjectRef):
                self._message_queue.put(msg)
            
            def get_message(self, block: Optional[bool] = None, timeout: Optional[float] = None) -> Message:
                if block is None:
                    block = self.mode == 'sync'
                if not block and timeout is None:
                    timeout = self._timeout
                self._message_queue.get(block=block, timeout=timeout)
            
            def invalidate_neighbors(self):
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
        
        return FedRayNode
    
    return _node
