import ray
from .. import remote as fedray
import networkx as nx
import numpy as np

from .message import Message


from typing import Dict, List, Optional, Union

BROKER_CPU_RESOURCES = 0.05


@fedray.remote(num_cpus=BROKER_CPU_RESOURCES)
class FedRayBroker:

    def __init__(self,
                 node_ids: List[str],
                 topology: Union[str, np.ndarray]) -> None:

        if len(node_ids) < 2:
            raise ValueError("At least 2 nodes are required to setup the topology.")

        self._topology = topology

        self._node_ids: List[str] = node_ids
        self._nodes: Dict[str] = None
        
        if isinstance(self._topology, str):
            if self._topology == 'star':
                self._graph = nx.star_graph(self._node_ids)
        elif isinstance(self._topology, np.ndarray):
            raise NotImplementedError
        
    def publish(self, sender_id: str, msg: Message, ids: Optional[Union[str, List[str]]] = None):
        if ids is None:
            ids = self.get_neighbors(sender_id)
        else:
            neighbors = self.get_neighbors(sender_id)
            if not all([curr_id in neighbors for curr_id in ids]):
                raise ValueError("")
        msg_ref = ray.put(msg)
        return ray.get(
            [self._nodes[neigh]._endpoint.remote(msg_ref) for neigh in self._graph.neighbors(sender_id)]
        )

    def get_neighbors(self, node_id: str):
        return [neigh for neigh in self._graph.neighbors(node_id)]

    def link_nodes(self):
        self._nodes = {node_id: ray.get_actor(node_id) for node_id in self._node_ids}
