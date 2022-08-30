from re import template
import ray
import numpy as np

from fedray.core._broker import FedRayBroker
from ray.util.placement_group import PlacementGroup
from typing import Any, Dict, List, Literal, Tuple, Union


__all__ = ['client_server_process', 'hierarchical_process', 'decentralized_process']


def client_server_process(server_template,
                          client_template,
                          server_id: str,
                          client_ids: List[str],
                          placement_group: PlacementGroup):
    nodes = [{
        'id': server_id,
        'template': server_template,
        'resources': placement_group.bundle_specs[1]
    }]

    for i in range(2, len(client_ids) + 2, 1):
        nodes.append([{
            'id': client_ids[i],
            'template': client_template,
            'resources': placement_group.bundle_specs[i]
        }])
    
    return FedProcess(
        mode='client-server',
        nodes=nodes,
        placement_group=placement_group
    )


def hierarchical_process(levels_templates: List[Tuple],
                         nodes_per_level: int,
                         ):
    pass


def decentralized_process(node_template):
    pass


class FedProcess(object):

    def __init__(self,
                 mode: Literal['client-server', 'hierarchical', 'decentralized'],
                 nodes: List[Dict[str, Any]],
                 placement_group: PlacementGroup,
                 **kwargs) -> None:
        
        self._mode = mode
        self._pg = placement_group
        self._nodes = nodes
        
        if self.mode == 'client-server':
            self._topology = 'star'
        elif self.mode == 'hierarchical':
            pass
        elif self.mode == 'decentralized':
            pass

        self._broker: FedRayBroker = None

        self._config: Dict = None

    def run(self) -> None:
        pass

    def shutdown(self) -> None:
        pass
    
    def build(self, config):
        self._broker = FedRayBroker.remote()

    @property
    def node_ids(self) -> List[str]:
        return self._nodes.keys()
    
    @property
    def resources(self) -> Dict[str, Dict[str, Union[int, float]]]:
        return {self._nodes[k]['resources'] for k in self.node_ids}
    
    @property
    def mode(self):
        return self.mode
