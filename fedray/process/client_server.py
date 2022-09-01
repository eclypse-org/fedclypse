from .fedprocess import FederatedProcess
from ray.util.placement_group import PlacementGroup

from typing import Any, Dict


class ClientServerProcess(FederatedProcess):

    def __init__(self,
                 server_template: Any,
                 client_template: Any,
                 n_clients: int,
                 placement_group: PlacementGroup,
                 server_config: Dict,
                 client_config: Dict) -> None:
        super().__init__()
        self._node_ids = ['server'] + [f'client_{i}' for i in range(n_clients)]
        self._pg = placement_group
        self._topology = 'star'
        self._broker = self._build_broker()

        self._nodes['server'] = self._build_node('server', server_template, placement_group.bundle_specs[1], **server_config)
        for i, c_id in enumerate(self._node_ids[1:]):
            self._nodes[c_id] = self._build_node(c_id, client_template, placement_group.bundle_specs[i+2], **client_config)