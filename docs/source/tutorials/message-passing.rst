Tutorial: Simple Message-Passing Federation
===========================================

In this tutorial we are going to show how to easily create a simple message-passing 
federation. The objective of this tutorial is to show how to use the ``FedRayNode``,
instantiate a ``ClientServerFederation`` and implement a training interface as a
simple message-passing federation.

We will use the following components:

* The ``FedRayNode`` abstract class for implementing the client and the server;
* The ``ClientServerFederation`` class for instantiating and running the federation.


First, we need to perform all the useful imports for this task:

.. code-block:: python3

    import time
    import ray
    import fedray
    
Step 1: Create the client
-------------------------

The client is a ``FedRayNode`` that sends a message to the server and waits for a reply.

.. code-block:: python3

    from fedray.core.node import FedRayNode

    @fedray.remote # This is a Ray actor
    class MessagingClient(FedRayNode):
        
        def train(self, out_msg: str) -> None:
            while True:
                # Send a message to the server (doesn't need to specify sender-id)
                self.send("exchange", {"msg": out_msg()})

                # Wait for a reply
                msg = self.receive()

                # Print the reply
                print(
                    f"{self.id} received {msg.body['msg']} from {msg.sender_id}",
                    msg.timestamp,
                )

                # Wait for 3 seconds
                time.sleep(3)


Here, we used exploited the communication interface easily by using the ``send`` and 
``receive`` methods. The ``send`` method takes the parameters header and body, while
the ``receive`` method returns a ``Message`` object. The ``Message`` object has the
following attributes:

* ``sender_id``: the id of the sender;
* ``body``: the body of the message;
* ``timestamp``: the timestamp of the message.


Step 2: Create the server
-------------------------

To create the server, we can employ the same approach as for the client:

.. code-block:: python3

    from fedray.core.node import FedRayNode

    @fedray.remote # This is a Ray actor
    class MessagingServer(FedRayNode):
        
        def train(self, out_msg: str) -> None:
            while True:
                # Wait for a message
                msg = self.receive()

                # Print the message
                print(
                    f"{self.id} received {msg.body['msg']} from {msg.sender_id}",
                    msg.timestamp,
                )

                # Send a reply to the client that sent the message
                self.send("exchange", {"msg": out_msg()}, msg.sender_id)

                # Wait for 3 seconds
                time.sleep(3)


Step 3: Create the federation
-----------------------------

Now that we have created the client and the server, we can instantiate and run the
federation:

.. code-block:: python3

    # Create the federation
    federation = ClientServerFederation(
        server_template=MessagingServer, # Specifies the server template
        client=MessagingClient, # Specifies the client template
        n_clients_or_ids=3, # Number of clients
        roles=["train" for _ in range(3)], # Roles of the nodes
    )

    # Run the federation
    federation.train(
        server_args = {"out_msg": lambda: "Hello from server"},
        client_args = {"out_msg": lambda: "Hello from client"},
    )

Note that the arguments of the training process are passed as keyword arguments to the
``train`` method. Thus, **the keys in the ``server_args`` and ``client_args`` dictionaries
must match the names of the arguments of the ``train`` method of the ``MessagingServer``
and ``MessagingClient`` classes**.

Remarks
-------
Regardless of the complexity of the training process, the logic is always the same:

* Implement the ``FedRayNode`` abstract class for both the client and the server;
* Instantiate the ``ClientServerFederation`` class with the appropriate arguments;

The ``ClientServerFederation`` class takes care of the rest, including the creation of
the Ray actors, the communication between the nodes, the synchronization of the nodes,
the termination of the federation, etc.

**Final note: the values of all the arguments of the FedRayNode need to be serializable.**

