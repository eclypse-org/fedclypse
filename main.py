import ray
import fedray
import time
from fedray.core.node import FedRayNode
from fedray.process import ClientServerProcess
from fedray.util.resources import get_federated_process_resources

@fedray.remote
class DummyServer(FedRayNode):

    def build(self, msg: str) -> None:
        self.msg = msg
    
    def run(self):
        while True:
            self.send(msg_type='logic', body={'msg': f'{self.msg}{self.id}'})
            for _ in range(3):
                sender_id, timestamp, body = self.get_message()
                print(f"Server receives the message '{body['msg']}' from {sender_id}.")
            time.sleep(3)
            
          
@fedray.remote
class DummyClient(FedRayNode):

    def build(self, msg: str) -> None:
        self.msg = msg
    
    def run(self):
        while True:
            self.send(msg_type='logic', body={'msg': f'{self.msg}{self.id}'})
            sender_id, timestamp, body = self.get_message()
            print(f"Client {self.id} receives the message '{body['msg']}' from {sender_id}.")
            time.sleep(3)


def main():
    ray.init()
    process = ClientServerProcess(
        server_template=DummyServer,
        client_template=DummyClient,
        n_clients=3,
        placement_group=get_federated_process_resources(4),
        server_config={
            'msg': 'I am the server '
        },
        client_config={
            'msg': 'I am the client '
        }
    )
    process.run()

if __name__ == '__main__':
    main()