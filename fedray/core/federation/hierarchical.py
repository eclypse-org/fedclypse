import numpy as np

from fedray.core.node.fedray_node import FedRayNode
from fedray.core.federation import Federation
from ray.util.placement_group import PlacementGroup

from typing import Type, Dict, List, Union


class HierarchicalFederation(Federation):
    def __init__(
        self,
        level_templates: List[Type[FedRayNode]],
        nodes_per_level: Union[List[int], List[List[str]]],
        roles: List[str],
        topology: Union[str, np.ndarray],
        level_config: List[Dict],
        resources: Union[str, PlacementGroup] = "uniform",
    ) -> None:
        raise NotImplementedError
