import ray, threading
import numpy as np
import ray

from ray.util.placement_group import PlacementGroup
from fedray.core.communication.topology.manager import _get_or_create_topology_manager
from fedray.core.node import FedRayNode, VirtualNode
from fedray.core.federation import Federation

from typing import Dict, List, Literal, Optional, Type, Union


class ClientServerFederation(Federation):
    """
    A ClientServerFederation is a special type of Federation that implements a federated
    client-server scheme. It consists of a server node, and multiple client nodes. In
    this scheme, the nodes are connected in a star topology, where the server node is
    the center of the star, and the client nodes are the leaves of the star.
    """

    def __init__(
        self,
        server_template: Type[FedRayNode],
        client_template: Type[FedRayNode],
        n_clients_or_ids: Union[int, List[str]],
        roles: List[str],
        server_config: Dict = {},
        client_config: Dict = {},
        server_id: str = "server",
        resources: Union[str, PlacementGroup] = "uniform",
        federation_id: str = "",
        is_tune: bool = False,
        bundle_offset: int = 0,
    ) -> None:
        """Creates a new ClientServerFederation object.

        Args:
            server_template (Type[FedRayNode]): The template for the server node.
            client_template (Type[FedRayNode]): The template for the client nodes.
            n_clients_or_ids (Union[int, List[str]]): The number of clients, or a list
                of client IDs.
            roles (List[str]): A list of roles for the client nodes. The length of this
                list must be equal to the number of clients.
            server_config (Dict, optional): The configuration to be passed to the build
                method of the server node. Defaults to {}.
            client_config (Dict, optional): The configuration to be passed to the build
                method of the client nodes. Defaults to {}.
            server_id (str, optional): The ID of the server node. Defaults to "server".
            resources (Union[str, PlacementGroup], optional): The resources to be used
                for the nodes. Defaults to "uniform".
            federation_id (str, optional): The ID of the federation. Defaults to "".
            is_tune (bool, optional): Whether the federation is used for a Ray Tune
                experiment. Defaults to False.
            bundle_offset (int, optional): The offset to be used for the bundle IDs.
                This is useful whenever we are allocating multiple federations in the
                same PlacementGroup. Defaults to 0.

        Raises:
            ValueError: If the number of clients does not match the number of roles.
        """
        if isinstance(n_clients_or_ids, int):
            c_ids = [f"client_{i}" for i in range(n_clients_or_ids)]
        else:
            c_ids = n_clients_or_ids

        nodes = [
            VirtualNode(
                server_template, server_id, federation_id, "train", server_config
            )
        ]
        for c_id, role in zip(c_ids, roles):
            nodes.append(
                VirtualNode(client_template, c_id, federation_id, role, client_config)
            )

        super(ClientServerFederation, self).__init__(
            nodes, "star", resources, federation_id, is_tune, bundle_offset
        )

    def train(
        self, server_args: Dict, client_args: Dict, blocking: bool = False
    ) -> None:
        """
        Performs a training session in the federation. Before calling the train method
        of the nodes, the method instantiates the training nodes in the federation by
        calling the .build


        Args:
            server_args (Dict): The arguments to be passed to the train function of the
                server node.
            client_args (Dict): The arguments to be passed to the train function of the
                client nodes.
            blocking (bool, optional): Whether to block the current thread until the
                training session is finished. Defaults to False.
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

        server_args = [server_args]
        client_args = [
            client_args[i] if isinstance(client_args, List) else client_args
            for i, _ in enumerate(train_nodes[1:])
        ]
        train_args = server_args + client_args

        self._runtime_remotes = [
            node.handle._train.remote(**train_args[i])
            for i, node in enumerate(train_nodes)
        ]
        self._runtime = threading.Thread(
            target=ray.get, args=[self._runtime_remotes], daemon=True
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
        for i, node in enumerate(self._nodes[1:], start=2 + self._bundle_offset):
            if phase in node.role:
                test_nodes.append(node)
                if node.handle is None:
                    node.build(i, self._pg)
        remotes = [node.handle.test.remote(phase, **kwargs) for node in test_nodes]

        results = ray.get(remotes)
        if not aggregate:
            return results

        values, weights = zip(*results)
        return np.average(values, weights=weights, axis=0)

    def pull_version(
        self,
        node_ids: Union[str, List[str]] = "server",
        timeout: Optional[float] = None,
    ) -> Dict:
        """
        Pulls the latest version of a model from in a federation. The default
        behavior is to pull the version from the server node.

        Args:
            node_ids (Union[str, List[str]], optional): The ID of the node(s) from which
                to pull the version. Defaults to "server".
            timeout (Optional[float], optional): The timeout for the pull operation.
                Defaults to None.

        Returns:
            Dict: The latest version of the model.
        """
        return super().pull_version(node_ids, timeout)

    @property
    def server(self):
        """Returns the handle of the server node."""
        return self._nodes[0].handle
