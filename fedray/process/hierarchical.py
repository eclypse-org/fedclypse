import numpy as np

from .fedprocess import FederatedProcess
from ray.util.placement_group import PlacementGroup

from typing import Any, Dict, List, Union


class HierarchicalProcess(FederatedProcess):

    def __init__(self,
                 level_template: List[Any],
                 nodes_per_level: List[int],
                 placement_group: PlacementGroup,
                 topology: Union[str, np.ndarray],
                 level_config: List[Dict]) -> None:
        super().__init__()
        return
        self._node_ids = [f'node_{i}' for i in range(num_nodes)]
        self._pg = placement_group
        self._topology = topology
        self._broker = self._build_broker()

        for i, node_id in enumerate(self._node_ids):
            self._nodes[node_id] = self._build_node(node_id, node_template, placement_group.bundle_specs[i+2], **node_config)