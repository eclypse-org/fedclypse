import ray
import numpy as np

from fedray._private.broker import FedRayBroker
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from typing import Any, Dict, List, Union
from collections import OrderedDict

from fedray.util.resources import BROKER_CPU_RESOURCES


class FederatedProcess(object):

    def __init__(self) -> None:
        self._nodes: OrderedDict[str, Any] = OrderedDict()
        self._broker: FedRayBroker
        self._topology: Union[str, np.ndarray]
        self._pg: PlacementGroup
    
    def run(self) -> None:
        ray.get([self._nodes[k]['handle'].run.remote() for k in self._nodes])

    def shutdown(self) -> None:
        raise NotImplementedError

    def _build_node(self, node_id: str, **kwargs) -> None:
        node = self._nodes[node_id]
        node['handle'] = node['template'].options(
            name=node_id,
            num_cpus=node['resources']['CPU'],
            num_gpus=node['resources']['GPU'] if 'GPU' in node['resources'] else 0,
            scheduling_strategy=PlacementGroupSchedulingStrategy(self._pg)
        ).remote(node_id=node_id, **kwargs)
        return node
    
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
        return self._nodes.keys()
    
    @property
    def resources(self) -> Dict[str, Dict[str, Union[int, float]]]:
        return {node['resources'] for node in self._nodes}


class ClientServerProcess(FederatedProcess):

    def __init__(self,
                 server_template: Any,
                 client_template: Any,
                 n_clients: int,
                 placement_group: PlacementGroup,
                 server_config: Dict,
                 client_config: Dict,
                 **kwargs) -> None:
        super().__init__()

        self._pg = placement_group
        self._topology = 'star'
        self._nodes['server'] = {
            'template': server_template,
            'resources': placement_group.bundle_specs[1]
        }

        for i in range(n_clients):
            self._nodes[f'client_{i}'] = {
                'template': client_template,
                'resources': placement_group.bundle_specs[i+2]
            }
        
        self._broker = self._build_broker()
        for k in self._nodes:
            node_args = server_config if k == 'server' else client_config
            self._nodes[k] = self._build_node(node_id=k, **node_args)
        ray.get(self._broker.link_nodes.remote())
