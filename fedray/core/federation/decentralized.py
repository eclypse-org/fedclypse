import ray, threading
import numpy as np

from ray.util.placement_group import PlacementGroup

from fedray.core.communication.topology.manager import _get_or_create_topology_manager
from fedray.core.node import FedRayNode, VirtualNode
from fedray.core.federation import Federation


from typing import Dict, List, Literal, Type, Union


class DecentralizedFederation(Federation):
    """
    A DecentralizedFederation is a special type of Federation that implements a
    decentralized federated learning scheme. It consists of multiple nodes, each of
    which has a different role. In this scheme, the nodes are connected with a
    user-defined topology.
    """

    def __init__(
        self,
        node_template: Type[FedRayNode],
        n_nodes_or_ids: Union[int, List[str]],
        roles: List[str],
        topology: Union[str, np.ndarray],
        node_config: Union[Dict, List[Dict]],
        resources: Union[str, PlacementGroup] = "uniform",
        federation_id: str = "",
        is_tune: bool = True,
        bundle_offset: int = 0,
    ) -> None:
        """Creates a new DecentralizedFederation object.

        Args:
            node_template (Type[FedRayNode]): The template for the nodes.
            n_nodes_or_ids (Union[int, List[str]]): The number of nodes, or a list of
                node IDs.
            roles (List[str]): A list of roles for the nodes. The length of this list
                must be equal to the number of nodes.
            topology (Union[str, np.ndarray]): The topology to be used for the
                federation. This can either be a string, or a numpy array. If it is a
                string, it must be one of the following: "fully_connected", "ring",
                "star", "line", "mesh", "grid", "tree", "bipartite", "custom". If it is
                a numpy array, it must be a square binary matrix of shape (N, N), where
                N is the number of nodes. The matrix must be symmetric, and the diagonal
                must be all zeros.
            node_config (Union[Dict, List[Dict]]): The configuration for the nodes.
                This can either be a dictionary, or a list of dictionaries. If it is a
                dictionary, the same configuration will be used for all nodes. If it is
                a list of dictionaries, the length of the list must be equal to the
                number of nodes.
            resources (Union[str, PlacementGroup], optional): The resources to be used
                for the federation. This can either be a string, or a PlacementGroup.
                If it is a string, it must be one of the following: "uniform", "random".
                Defaults to "uniform".
            federation_id (str, optional): The ID of the federation. Defaults to "".
            is_tune (bool, optional): Whether the federation is used for a Ray Tune
                experiment. Defaults to False.
            bundle_offset (int, optional): The offset for the bundle IDs. Defaults to 0.

        Raises:
            ValueError: If the number of nodes does not match the number of roles.
        """
        if isinstance(n_nodes_or_ids, int):
            node_ids = [f"node_{i}" for i in range(n_nodes_or_ids)]
        else:
            node_ids = n_nodes_or_ids

        nodes = [
            VirtualNode(
                node_template,
                node_id,
                federation_id,
                role,
                node_config[i] if isinstance(node_config, list) else node_config,
            )
            for i, (node_id, role) in enumerate(zip(node_ids, roles))
        ]

        super(DecentralizedFederation, self).__init__(
            nodes, topology, resources, federation_id, is_tune, bundle_offset
        )

    def train(self, train_args: Union[Dict, List[Dict]], blocking: bool = False):
        """Performs a training session in the federation. This method calls the train
        method of each node in the federation.

        Args:
            train_args (Union[Dict, List[Dict]]): The arguments for the train method.
                This can either be a dictionary, or a list of dictionaries. If it is a
                dictionary, the same arguments will be used for all nodes. If it is a
                list of dictionaries, the length of the list must be equal to the
                number of nodes.
            blocking (bool, optional): Whether the method should block until the
                training session is finished. Defaults to False.

        Raises:
            ValueError: If the number of nodes does not match the number of train_args.
        """
        if self._tp_manager is None:
            self._tp_manager = _get_or_create_topology_manager(
                self._pg, self._fed_id, self._bundle_offset
            )
        train_nodes = []
        for i, node in enumerate(self._nodes, start=1 + self._bundle_offset):
            if "train" in node.role:
                if not node.built:
                    node.build(i, self._pg)
                train_nodes.append(node)

        ray.get(
            self._tp_manager.build_network.remote(
                [node.id for node in train_nodes], self._topology
            )
        )
        ray.get([node.handle._setup_train.remote() for node in train_nodes])
        train_args = [
            (train_args[i] if isinstance(train_args, List) else train_args)
            for i, _ in enumerate(train_nodes)
        ]
        self._runtime = threading.Thread(
            target=ray.get,
            args=[[node.handle._train.remote(**train_args[i]) for node in train_nodes]],
            daemon=True,
        )
        self._runtime.start()
        if blocking:
            self._runtime.join()

    def test(
        self, phase: Literal["train", "eval", "test"], aggregate: bool = True, **kwargs
    ) -> Union[List[float], float]:
        """
        Performs a test session in the federation.

        Args:
            phase (Literal["train", "eval", "test"]): the role of the nodes on which
                the test should be performed.
            aggregate (bool, optional): Whether to aggregate the results weighted by the
                number of samples of the local datasets. If False, the results of each
                node are returned in a list. Defaults to True.
            **kwargs: The arguments to be passed to the test function of the nodes.

        Returns:
            Union[List[float], float]: The results of the test session. If aggregate is
                True, the results are averaged.
        """
        test_nodes = []
        for i, node in enumerate(self._nodes, start=1 + self._bundle_offset):
            if node.role is not None and phase in node.role:
                test_nodes.append(node)
                if node.handle is None:
                    node.build(i, self._pg)
        remotes = [node.handle.test.remote(phase, **kwargs) for node in test_nodes]
        results = ray.get(remotes)
        if not aggregate:
            return results

        values, weights = zip(*results)
        return np.average(values, weights=weights, axis=0)
