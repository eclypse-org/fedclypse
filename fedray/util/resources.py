import ray, random, logging

from typing import Literal


def get_resources_split(
    num_nodes: int,
    num_cpus: int = None,
    num_gpus: int = None,
    split_strategy: Literal["random", "uniform"] = "uniform",
    placement_strategy: Literal[
        "STRICT_PACK", "PACK", "STRICT_SPREAD", "SPREAD"
    ] = "PACK",
    is_tune: bool = False,
):
    """
    Provides the resources for a federation. The resources are provided as a
    PlacementGroup or a PlacementGroupFactory (if is_tune is True).

    Given the number of nodes, cpus, gpus and split strategy, this function creates a
    set of bundles (i.e., dictionaries containing splits of the resources, one split
    per node), and then creates a PlacementGroup or a PlacementGroupFactory from the
    bundles. The placement strategy is used to determine how Ray will allocate the nodes
    in the cluster.

    Args:
        num_nodes (int): The number of nodes in the federation.
        num_cpus (int, optional): The number of CPUs available in the cluster. If None,
            it is set to num_nodes. Defaults to None.
        num_gpus (int, optional): The number of GPUs available in the cluster. If None,
            it is set to the number of GPUs available in the cluster. Defaults to None.
        split_strategy (Literal["random", "uniform"], optional): The strategy to split
            the resources. Defaults to "uniform".
        placement_strategy (Literal["STRICT_PACK", "PACK", "STRICT_SPREAD", "SPREAD"],
            optional): The strategy to place the nodes in the cluster. Defaults to
            "PACK".
        is_tune (bool, optional): Whether the resources are used within a Ray Tune
            experiment.

    Returns:
        Union[PlacementGroup, PlacementGroupFactory]: The resources for the federation.

    Raises:
        RuntimeError: If the number of CPUs is less than 2.
        ValueError: If the split strategy is not "random" or "uniform".
    """
    SAFETY_EPSILON = 0.01
    available_resources = ray.available_resources()
    if available_resources["CPU"] < 2:
        raise RuntimeError(
            "At least 2 CPUs are required in the Ray cluster. Please increase the",
            "number of CPUs.",
        )

    if not is_tune:
        resources = [{"CPU": 1}]
    else:
        resources = [{"CPU": 0.5}, {"CPU": 0.5}]

    if num_cpus is None:
        num_cpus = num_nodes
    if num_cpus > available_resources["CPU"]:
        num_cpus = available_resources["CPU"]
        logging.warn(
            "The available CPUs are less than the declared parameter num_cpus.",
            f"Parameter num_cpus set to {num_cpus}.",
        )

    if num_gpus is not None:
        if "GPU" not in available_resources and num_gpus is not None:
            logging.warn(
                "GPUs not available in this Ray cluster. Parameter num_gpus set to None."
            )
            num_gpus = 0
        elif num_gpus > available_resources["GPU"]:
            num_gpus = available_resources["GPU"]
            logging.warn(
                f"The available GPUs are less than the declared parameter num_gpus.",
                f"Parameter num_gpus set to {num_gpus}.",
            )
    else:
        num_gpus = available_resources["GPU"] if "GPU" in available_resources else 0

    fix_size = lambda x: x if x < 1 else int(x)
    if split_strategy == "uniform":
        alloc_fn = (
            lambda i, num: num_nodes // num + 1
            if i < num_nodes % num
            else num_nodes // num
        )
        cpu_alloc = [alloc_fn(i, num_cpus) for i in range(num_cpus)]
        gpu_alloc = [alloc_fn(i, num_gpus) for i in range(int(num_gpus))]
        cpu_i, gpu_i, b_cpu_i, b_gpu_i = 0, 0, 0, 0
        for i in range(num_nodes):
            resources_i = {}
            cpu_alloc_i = (1 - SAFETY_EPSILON) / cpu_alloc[cpu_i]
            resources_i["CPU"] = fix_size(cpu_alloc_i)
            b_cpu_i = b_cpu_i + 1
            if b_cpu_i == cpu_alloc[cpu_i]:
                cpu_i = cpu_i + 1
                b_cpu_i = 0

            if num_gpus > 0:
                gpu_alloc_i = (1 - SAFETY_EPSILON) / gpu_alloc[gpu_i]
                resources_i["GPU"] = fix_size(gpu_alloc_i)
                b_gpu_i = b_gpu_i + 1
                if b_gpu_i == gpu_alloc[gpu_i]:
                    gpu_i = gpu_i + 1
                    b_gpu_i = 0

            resources.append(resources_i)

    elif split_strategy == "random":
        perc = [random.random() for _ in range(num_nodes)]
        total = sum(perc)
        perc = [s / total for s in perc]

    else:
        raise ValueError(f"Unknown split strategy: {split_strategy}.")

    if not is_tune:
        from ray.util.placement_group import placement_group

        return placement_group(bundles=resources, strategy=placement_strategy)
    else:
        from ray import tune

        return tune.PlacementGroupFactory(
            bundles=resources, strategy=placement_strategy
        )
