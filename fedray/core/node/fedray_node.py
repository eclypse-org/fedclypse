# -*- coding: utf-8 -*-
import copy
import copyreg
import queue
import threading
import time
from functools import cached_property
from queue import Full
from queue import Queue as _Queue
from typing import Any
from typing import Dict
from typing import List
from typing import Literal
from typing import Optional
from typing import Tuple
from typing import Union

import ray

from fedray.core.communication.topology.manager import Message
from fedray.core.communication.topology.manager import TopologyManager
from fedray.util.exceptions import EndProcessException


class FedRayNode(object):
    """Base class for a node in a federation.

    It provides all the base functionalities to allow a node to interact with the
    federation. It is not meant to be used directly, but rather to be subclassed
    by the user to define a custom node.

    While subclassing, the user can implement the build, train and test methods function.


    The user must implement the train function, which is called when the federation
    starts the training process. The train function is responsible for the internal
    logic of the node (e.g., local training and aggregation for client and server nodes
    respectively).

    The user can also implement the test function, which is called to perform a round of
    evaluation on the federation.

    The communication between nodes happens only within the train function by using
    the `send` and `receive` methods. The send function is used to send a message to a
    specific node, while the receive function is used to receive a message from a
    specific node. The send and receive functions are blocking, meaning that the
    execution of the train function is paused until the message is received or sent.
    """

    def __init__(self, node_id: str, role: str, federation_id: str = "", **kwargs):
        """Creates a node in the federation. Must not be called directly or overridden.
        Args:
            node_id (str): The id of the node.
            role (str): The role of the node. It must be either a single role in
                {"train", "eval", "test"}, or a combination of them as a dash-separated
                string. For example, "train-eval" or "train-eval-test".
            federation_id (str): The id of the federation the node belongs to.
                Defaults to "".
            **kwargs: Additional arguments to be passed to the build function.
        """
        # Node hyperparameters
        self._fed_id: str = federation_id
        self._id: str = node_id
        self._role: str = role

        # Communication interface
        self._tp_manager: TopologyManager = None
        self._message_queue: Queue = None

        # Node's version
        self._version: int = 0
        self._version_buffer: Queue = None
        self._node_metrics: Dict[str, Any] = {}

        # Buildup function
        self._node_config = kwargs
        self.build(**kwargs)

    def build(self, **kwargs):
        """
        Performs the setup of the node's environment when the node is added to a
        federation. The build function and has a twofold purpose:
            1. Define here the attributes that are independent from whether the node is
            executing the training function or the test function (e.g., choosing the
            optimizer, the loss function, etc.);
            2. Perform all the resource-intensive operations in advance (e.g.,
            downloading the data from an external source, or instantiating a model with
            computationally-intensive techniques) to avoid bottlenecks within the
            training and test processes.
        Since it is called within the __init__ function, the user can define additional
        class attributes.

        An example of build function can be the following:

        .. code-block:: python

            def build(self, dataset_name: str):
                self._dataset_name = dataset_name
                self._dataset = load_dataset(self._dataset_name)
        """
        pass

    def _setup_train(self):
        """Prepares the node's environment for the training process."""
        if self._tp_manager is None:
            self._tp_manager = ray.get_actor(
                "/".join([self._fed_id, "topology_manager"])
            )
        self._message_queue = Queue()
        self._version = 0
        self._version_buffer = Queue()
        return True

    def _train(self, **train_args):
        """Wrapper for the training function"""
        try:
            self.train(**train_args)
        except EndProcessException:
            print(f"Node {self.id} is exiting.")

        return self._node_metrics

    def train(self, **train_args) -> Dict:
        """Implements the core logic of a node within a training process. It is
        called by the federation when the training process starts.

        An example can be the client in the Federated Averaging algorithm:

        .. code-block:: python

            def train(self, **train_args):
                while True:
                    # Get the model
                    model = self.receive().body["model"]

                    # Get the data
                    data_fn = self.get_data()

                    # Train the model

                    model.train(self.dataset, self.optimizer, self.loss, self.metrics)

                    # Send the model to the server
                    self.send("model", model)
        """
        raise NotImplementedError

    def test(
        self, phase: Literal["train", "eval", "test"], **kwargs
    ) -> Tuple[float, int]:
        """Implements the core logic of a node within a test process. It is
        called by the federation when the test session starts.

        Args:
            phase (Literal["train", "eval", "test"]): The phase of the test process.
                It can be either "train", "eval" or "test".
            **kwargs: Additional arguments to be passed to the test function.

        Returns:
            Tuple(float, int): A tuple containing the average loss and the number of
                samples used for the test.
        """
        raise NotImplementedError

    def send(self, header: str, body: Dict, to: Optional[Union[str, List[str]]] = None):
        """
        Sends a message to a specific node or to the neighbor nodes in the federation.

        Args:
            header (str): The header of the message.
            body (Dict): The body of the message.
            to (Optional[Union[str, List[str]]], optional): The id of the node to which
                the message is sent. If None, the message is sent to the neighbor nodes.
                Defaults to None.
        """
        if isinstance(to, str):
            to = [to]

        msg = Message(header=header, sender_id=self._id, body=body)
        ray.get([self._tp_manager.forward.remote(msg, to)])

    def receive(self, timeout: Optional[float] = None) -> Message:
        """
        Receives a message from the message queue. If the timeout value is defined, it
        waits for a message for the specified amount of time. If no message is received
        within the timeout, it returns None. This allows to implement a node with an
        asynchronous behavior.

        Args:
            timeout (Optional[float], optional): The timeout value. Defaults to None.

        Returns:
            Message: The received message.

        Raises:
            EndProcessException: If the message received is a "STOP" message, it raises
                an EndProcessException to stop the process. This is handled under the
                hood by the training function.
        """
        try:
            msg = self._message_queue.get(timeout=timeout)
        except Queue.Empty:
            msg = None

        if msg is not None and msg.header == "STOP":
            raise EndProcessException
        return msg

    def update_version(self, **kwargs):
        """
        Updates the node's version. Whenever this function is called, the version is
        stored in an internal queue. The version is pulled from the queue whever the
        federation calls the `pull_version` method.
        """
        to_save = {k: copy.deepcopy(v) for k, v in kwargs.items()}
        version_dict = {
            "id": self.id,
            "n_version": self.version,
            "timestamp": time.time(),
            "model": to_save,
        }
        self._version_buffer.put(version_dict)
        self._version += 1

    def stop(self):
        """Stops the node's processes."""
        self._message_queue.put(Message("STOP"), index=0)

    def enqueue(self, msg: ray.ObjectRef):
        """Enqueues a message in the node's message queue. This method is called by the
        topology manager when a message is sent from a neighbor.

        Args:
            msg (ray.ObjectRef): The message to be enqueued.

        Returns:
            bool: True, a dummy value for the federation.
        """
        self._message_queue.put(msg)
        return True

    def _invalidate_neighbors(self):
        """
        Invalidates the node's neighbors. This method is called by the topology manager
        when the topology changes. In future versions, this will be used to implement
        dynamic topologies.
        """
        del self.neighbors

    def _pull_version(self):
        """
        Pulls the version from the version buffer. This method is called under the
        hood by the federation when the `pull_version` method is called.
        """
        return self._version_buffer.get(block=True)

    @property
    def id(self) -> str:
        """Returns the node's id."""
        return self._id

    @property
    def version(self) -> int:
        """Returns the node's current version."""
        return self._version

    @property
    def is_train_node(self) -> bool:
        """True if the node is a training node, False otherwise."""
        return "train" in self._role.split("-")

    @property
    def is_eval_node(self) -> bool:
        """True if the node is an evaluation node, False otherwise."""
        return "eval" in self._role.split("-")

    @property
    def is_test_node(self) -> bool:
        """True if the node is a test node, False otherwise."""
        return "test" in self._role.split("-")

    @cached_property
    def neighbors(self) -> List[str]:
        """Returns the list of the node's neighbor IDs."""
        return ray.get(self._tp_manager.get_neighbors.remote(self.id))


class Queue(_Queue, object):
    Empty = queue.Empty
    Full = queue.Full

    def put(self, item, block=True, timeout=None, index=None):
        """Put an item into the queue.
        If optional args 'block' is true and 'timeout' is None (the default),
        block if necessary until a free slot is available. If 'timeout' is
        a non-negative number, it blocks at most 'timeout' seconds and raises
        the Full exception if no free slot was available within that time.
        Otherwise ('block' is false), put an item on the queue if a free slot
        is immediately available, else raise the Full exception ('timeout'
        is ignored in that case).
        """
        with self.not_full:
            if self.maxsize > 0:
                if not block:
                    if self._qsize() >= self.maxsize:
                        raise Full
                elif timeout is None:
                    while self._qsize() >= self.maxsize:
                        self.not_full.wait()
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = time() + timeout
                    while self._qsize() >= self.maxsize:
                        remaining = endtime - time()
                        if remaining <= 0.0:
                            raise Full
                        self.not_full.wait(remaining)
            self._put(item, index)
            self.unfinished_tasks += 1
            self.not_empty.notify()

    def _put(self, item, index) -> None:
        if index is None:
            self.queue.append(item)
        else:
            self.queue.insert(index, item)


def pickle_queue(q):
    q_dct = q.__dict__.copy()
    del q_dct["mutex"]
    del q_dct["not_empty"]
    del q_dct["not_full"]
    del q_dct["all_tasks_done"]
    return Queue, (), q_dct


def unpickle_queue(state):
    q = state[0]()
    q.mutex = threading.Lock()
    q.not_empty = threading.Condition(q.mutex)
    q.not_full = threading.Condition(q.mutex)
    q.all_tasks_done = threading.Condition(q.mutex)
    q.__dict__ = state[2]
    return q


copyreg.pickle(Queue, pickle_queue, unpickle_queue)
