import fedray

from typing import Dict, List


class FedOptClient:
    def build(self, local_epochs: int) -> None:
        self.epochs = local_epochs

    def run(self):
        while True:
            msg = self.get_message(block=True)
            self.model.load_state_dict(msg.body["state"])

            for e in range(self.epochs):
                self.train_epoch()

            self.send(type="model", body=self.client_opt.get_params())

    def train_epoch(self):
        raise NotImplementedError


class ServerOpt:
    def __init__(self) -> None:
        self.client_ids: Dict[str, bool] = {}

    def set_iteration(self, client_ids: List[str], **kwargs) -> None:
        raise NotImplementedError

    def update(self):
        raise NotImplementedError

    def aggregate(self):
        raise NotImplementedError


class AveragingOpt(ServerOpt):
    def __init__(self) -> None:
        self.n_samples: int = 0
        self.state_dict: Dict = None

    def set_iteration(self, client_ids: List[str]) -> None:
        self.client_ids = {c_id: False for c_id in client_ids}
        self.n_samples = 0
        self.state_dict = None

    def update(self, client_id: str, client_dict: Dict):
        local_n_samples = client_dict.pop("n_samples")
        for k in client_dict["state"].keys():
            client_dict["state"][k] = client_dict["state"][k] * local_n_samples
        self.n_samples += client_dict.pop("n_samples")

        if self.state_dict is None:
            self.state_dict = client_dict

    def aggregate(self):
        n_samples = sum(self.client_dicts[k]["n_samples"] for k in self.client_ids)
        model_dict = {}

        for k in self.client_ids:
            pass

    @property
    def ready(self):
        return all([c_id[1] for c_id in self.client_ids])
