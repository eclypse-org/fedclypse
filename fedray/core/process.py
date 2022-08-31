import ray

from fedray.core._broker import FedRayBroker
from ray.util.placement_group import PlacementGroup
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy

from typing import Any, Dict, List, Literal, Union

from fedray.util.resources import BROKER_CPU_RESOURCES


__all__ = ['client_server_process', 'hierarchical_process', 'decentralized_process']


def client_server_process(server_template: Any,
                          client_template: Any,
                          server_id: str,
                          client_ids: List[str],
                          placement_group: PlacementGroup,
                          **kwargs):
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
        placement_group=placement_group,
        **kwargs
    )


def hierarchical_process(levels_templates: List[Any],
                         nodes_per_level: int,
                         ):
    pass


def decentralized_process(node_template: Any):
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
        
        self._build(**kwargs)
        

    def run(self) -> None:
        pass

    def shutdown(self) -> None:
        pass

    def _build(self, **kwargs) -> None:
        for node_id in self.node_ids:
            curr_node = self._nodes[node_id]
            curr_node['handle'] = curr_node['template'].options(
                name=node_id,
                num_cpus=curr_node['resources']['CPU'],
                num_gpus=curr_node['resources']['GPU'],
                scheduling_strategy=PlacementGroupSchedulingStrategy(self._pg)
            ).remote(**kwargs)
        
        self._broker: FedRayBroker = FedRayBroker.options(
            name='broker',
            num_cpus=BROKER_CPU_RESOURCES,
            scheduling_strategy=PlacementGroupSchedulingStrategy(self._pg)
        ).remote(
            node_ids=self.node_ids,
            topology=self._topology
        )
    
    @property
    def node_ids(self) -> List[str]:
        return self._nodes.keys()
    
    @property
    def resources(self) -> Dict[str, Dict[str, Union[int, float]]]:
        return {self._nodes[k]['resources'] for k in self.node_ids}
    
    @property
    def mode(self):
        return self.mode
