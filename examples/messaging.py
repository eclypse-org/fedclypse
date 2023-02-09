import ray, time, fedray
from fedray.core.node import FedRayNode
from fedray.core.federation import ClientServerFederation


@fedray.remote
class MessagingServer(FedRayNode):
    def train(self, out_msg: str):
        n_exchanges = 0
        while True:
            msg = self.receive()
            print(
                f"{self.id} received {msg.body['msg']} from {msg.sender_id} at",
                {msg.timestamp},
            )
            self.send("exchange", {"msg": out_msg()}, to=msg.sender_id)
            self.update_version(n_exchanges=n_exchanges)


@fedray.remote
class MessagingClient(FedRayNode):
    def train(self, out_msg: str) -> None:
        while True:
            self.send("exchange", {"msg": out_msg()})
            msg = self.receive()
            print(
                f"{self.id} received {msg.body['msg']} from {msg.sender_id}",
                msg.timestamp,
            )
            time.sleep(3)


def main():
    ray.init()
    federation = ClientServerFederation(
        server_template=MessagingServer,
        client_template=MessagingClient,
        n_clients_or_ids=4,
        roles=["train" for _ in range(4)],
    )
    report = federation.train(
        server_args={"out_msg": lambda: "Hello from server!"},
        client_args={"out_msg": lambda: "Hello from client!"},
    )
    for _ in range(4):
        version = federation.pull_version()
        print(version)
    time.sleep(3)
    federation.stop()


if __name__ == "__main__":
    main()
