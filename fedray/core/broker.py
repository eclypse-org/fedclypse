import ray
import networkx as nx
import numpy as np
import datetime

from dataclasses import field, dataclass

from typing import Dict, List, Literal, Optional, Union

from fedray.core.node import FedRayNode


@ray.remote
class FedRayBroker:

    def __init__(self,
                 node_ids: List[str],
                 topology: Union[str, np.ndarray]) -> None:
        if len(node_ids) < 2:
            raise ValueError("At least 2 nodes are required to setup the topology.")

        self._topology = topology
        self._graph: nx.Graph = None

        self._node_ids: List[str] = node_ids
        self._nodes: Dict[str, FedRayNode] = None
        
    def publish(self, sender_id: str, msg: ray.ObjectRef, ids: Optional[Union[str, List[str]]] = None):
        if ids is None:
            ids = self.get_neighbors(sender_id)
        else:
            neighbors = self.get_neighbors(sender_id)
            if not all([curr_id in neighbors for curr_id in ids]):
                raise ValueError("")

        ray.get(
            [self._nodes[neigh].message_endpoint.remote(msg) for neigh in self._graph.neighbors(sender_id)]
        )

    def get_neighbors(self, node_id: str):
        return [neigh for neigh in self._graph.neighbors(node_id)]
    
    def build(self):
        if isinstance(self._topology, str):
            if self._topology == 'client-server':
                self._graph = nx.star_graph(self._node_ids)
        elif isinstance(self._topology, np.ndarray):
            raise NotImplementedError
        
        self._nodes = {node_id: ray.get_actor(node_id) for node_id in self._node_ids}



@dataclass
class Message:
    type: Literal['model', 'logic'] = None
    sender_id: str = None
    timestamp: datetime.datetime = field(default_factory=datetime.datetime.now)
    body: Dict = field(default_factory=dict)
