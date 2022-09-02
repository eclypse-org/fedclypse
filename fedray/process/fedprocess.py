import ray
import numpy as np

from ..core._private.broker import FedRayBroker, BROKER_CPU_RESOURCES
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from typing import Any, Dict, List, Union


class FederatedProcess(object):

    def __init__(self) -> None:
        self._node_ids: List[str]
        self._nodes: Dict[str, Any] = {}
        self._broker: FedRayBroker
        self._topology: Union[str, np.ndarray]
        self._pg: PlacementGroup
    
    def run(self) -> None:
        ray.get(self._broker.link_nodes.remote())
        ray.get([self._nodes[k]['handle'].run.remote() for k in self._nodes])

    def shutdown(self) -> None:
        raise NotImplementedError

    def _build_node(self, node_id: str, template: Any, resources: Dict[str, Union[int, float]], **kwargs) -> None:
        return {
            'template': template,
            'resources': resources,
            'handle': template
                .options(
                    name=node_id,
                    num_cpus=resources['CPU'],
                    num_gpus=resources['GPU'] if 'GPU' in resources else 0,
                    scheduling_strategy=PlacementGroupSchedulingStrategy(self._pg)
                )
                .remote(node_id=node_id, **kwargs)
        }
    
    def _build_broker(self) -> FedRayBroker:
        return FedRayBroker.options(
            name='broker',
            num_cpus=BROKER_CPU_RESOURCES,
            scheduling_strategy=PlacementGroupSchedulingStrategy(self._pg)
        ).remote(
            node_ids=self.node_ids,
            topology=self._topology
        )
    
    @property
    def num_nodes(self) -> int:
        return len(self._nodes)
    
    @property
    def node_ids(self) -> List[str]:
        return self._node_ids
    
    @property
    def resources(self) -> Dict[str, Dict[str, Union[int, float]]]:
        return {node['resources'] for node in self._nodes}
