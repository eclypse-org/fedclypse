import ray
from ray import tune
import random

from ray.util.placement_group import placement_group, PlacementGroup

from typing import Dict, List, Literal, Union

BROKER_CPU_RESOURCES = 0.05
SAFETY_EPSILON = 0.01


def get_federated_process_resources(num_nodes_or_resources: Union[int, List[Dict[str, Union[int, float]]]],
                                    split_size: float = 1.,
                                    split_strategy: Literal['random', 'uniform'] = 'uniform',
                                    placement_strategy: Literal['strict_pack', 'pack', 'strict_spread', 'spread'] = 'strict_pack',
                                    is_tune: bool = False) -> Union[PlacementGroup, tune.PlacementGroupFactory]:
    """_summary_

    Args:
        num_nodes_or_resources (Union[int, List[Dict[str, Union[int, float]]]]): _description_
        split_strategy (Literal[&#39;random&#39;, &#39;uniform&#39;]): _description_
        placement_strategy (Literal[&#39;strict_pack&#39;, &#39;pack&#39;, &#39;strict_spread&#39;, &#39;spread&#39;], optional): _description_. Defaults to 'strict_pack'.
        is_tune (bool, optional): _description_. Defaults to False.

    Returns:
        Union[PlacementGroup, tune.PlacementGroupFactory]: _description_
    """

    if isinstance(num_nodes_or_resources, int):
        resources = create_resources_split(num_nodes_or_resources, split_size=split_size, split_strategy=split_strategy)
    
    if not is_tune:
        return placement_group(bundles=resources, strategy=placement_strategy)
    else:
        return tune.PlacementGroupFactory(bundles=[{}] + resources, strategy=placement_strategy)


def create_resources_split(num_nodes: int,
                           split_size: float = 1.,
                           split_strategy: Literal['random', 'uniform'] = 'uniform'):
    available_resources = ray.available_resources()
    available_resources['CPU'] = (available_resources['CPU'] - SAFETY_EPSILON) * split_size - BROKER_CPU_RESOURCES
    if 'GPU' in available_resources:
        available_resources['GPU'] = (available_resources['GPU'] - SAFETY_EPSILON) * split_size


    if split_strategy == 'uniform':
        resources = {'CPU': available_resources['CPU']/num_nodes}
        if 'GPU' in available_resources:
            resources['GPU'] = {'GPU': available_resources['GPU']/num_nodes}
        resources = [resources for _ in range(num_nodes)]
    

    if split_strategy == 'random':
        split = [random.random() for _ in range(num_nodes)]
        total = sum(split)
        split = [s / total for s in split]

        resources = []
        for s in split:
            curr_resources = {'CPU': available_resources['CPU'] * s}
            if 'GPU' in available_resources:
                curr_resources['GPU'] = {'GPU': available_resources['GPU'] * s}
            resources.append(curr_resources)
    
    return [{'CPU': BROKER_CPU_RESOURCES}] + resources
