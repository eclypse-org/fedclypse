# -*- coding: utf-8 -*-
from typing import Dict
from typing import List
from typing import Type
from typing import Union

import numpy as np
from ray.util.placement_group import PlacementGroup

from fedray.core.federation import Federation
from fedray.core.node.fedray_node import FedRayNode


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
