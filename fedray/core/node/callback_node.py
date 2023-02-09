from .fedray_node import FedRayNode


class CallbackFedRayNode(FedRayNode):
    def run(self):
        while True:
            in_msg = self.receive()
            fn = getattr(self, in_msg.header)
            args = in_msg.body
            args["sender_id"] = in_msg.sender_id
            args["timestamp"] = in_msg.timestamp

            response = fn(**args)
            if response is not None:
                self.send(**response)
