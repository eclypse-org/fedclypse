import ray, fedray
import networkx as nx
import numpy as np

from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
from ..message import Message

from typing import Dict, List, Optional, Union


TP_MANAGER_CPU_RESOURCES = 0.05


@fedray.remote(num_cpus=TP_MANAGER_CPU_RESOURCES)
class TopologyManager:
    """
    The TopologyManager is responsible for managing the network topology of a
    federation. It is responsible for creating the network topology, and for
    forwarding messages to the appropriate nodes through the neighborhood relationship.

    The actual definition of the network happens within the `build_network` method,
    which is called by the Federation object. Given a list of node IDs, and a topology,
    the TopologyManager creates a networkx graph, and stores the node IDs and
    the graph. The graph is used to determine the neighborhood relationship between
    nodes, and the node IDs are used to retrieve the nodes from the federation's
    node registry.
    """

    def __init__(self, federation_id: str) -> None:
        """
        Creates a new TopologyManager.

        Args:
            federation_id (str): The ID of the federation to which this topology
                manager belongs.
        """
        self._fed_id = federation_id

        self._node_ids: List[str] = []
        self._nodes: Dict[str] = None
        self._topology = None
        self._graph: nx.Graph = None

    def forward(self, msg: Message, to: Optional[Union[str, List[str]]] = None):
        """
        Forwards a message to the appropriate nodes.

        Args:
            msg (Message): The message to forward.
            to (Optional[Union[str, List[str]]], optional): The nodes to which the
                message should be forwarded. If None, the message is forwarded to
                all neighbors of the sender. Defaults to None.

        Raises:
            ValueError: If the `to` argument is not None, and any of the node IDs
                in the list are not neighbors of the sender.

        Returns:
            List[ObjectRef]: A list of ObjectRefs to the messages that were sent.
        """

        if to is None:
            to = self.get_neighbors(msg.sender_id)
        else:
            neighbors = self.get_neighbors(msg.sender_id)
            for curr_id in to:
                if not all([curr_id in neighbors for curr_id in neighbors]):
                    raise ValueError(f"{curr_id} is not a neighbor of {msg.sender_id}")
        msg_ref = ray.put(msg)
        return ray.get([self._nodes[neigh].enqueue.remote(msg_ref) for neigh in to])

    def get_neighbors(self, node_id: str):
        """
        Returns the neighbors of a node. This function is implicitly called by the
        `neighbors` property of a FedRayNode.

        Args:
            node_id (str): The ID of the node for which to retrieve the neighbors.

        Returns:
            List[str]: A list of node IDs.
        """

        return [neigh for neigh in self._graph.neighbors(node_id)]

    def build_network(self, node_ids: List[str], topology: Union[str, np.ndarray]):
        """Builds the network topology.

        Args:
            node_ids (List[str]): A list of node IDs.
            topology (Union[str, np.ndarray]): The topology to use. If a string, it
                must be one of the following: "star". If a numpy array, it must be
                a square matrix of shape (N, N), where N is the number of nodes.
                The matrix must be symmetric, and the diagonal must be all zeros.
                The matrix must be binary, and the matrix must be symmetric.

        Raises:
            ValueError: If the number of nodes is less than 2.
            NotImplementedError: If the topology is a numpy array.
        """

        if len(node_ids) < 2:
            raise ValueError("At least 2 nodes are required to setup the network.")
        self._node_ids = node_ids
        self._nodes = {
            node_id: ray.get_actor("/".join([self._fed_id, node_id]))
            for node_id in self._node_ids
        }

        self._topology = topology
        if isinstance(self._topology, str):
            if self._topology == "star":
                self._graph = nx.star_graph(self._node_ids)
        elif isinstance(self._topology, np.ndarray):
            raise NotImplementedError


def _get_or_create_topology_manager(
    placement_group, federation_id: str, bundle_offset: int
) -> TopologyManager:
    """
    Returns the TopologyManager for the given federation ID. If the TopologyManager
    does not exist, it is created.

    Args:
        placement_group (PlacementGroup): The placement group to use.
        federation_id (str): The ID of the federation.
        bundle_offset (int): The bundle offset.

    Returns:
        TopologyManager: The TopologyManager.
    """

    return TopologyManager.options(
        name=federation_id + "/topology_manager",
        num_cpus=TP_MANAGER_CPU_RESOURCES,
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group, placement_group_bundle_index=0 + bundle_offset
        ),
    ).remote(federation_id=federation_id)
