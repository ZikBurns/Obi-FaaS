from torch.jit import RecursiveScriptModule
import torch

class TorchscriptFork:
    def __init__(self, ens: RecursiveScriptModule):
        self.ens = ens

    def postprocessing(self, out):
        preds = out.argmax(dim=1)
        pred_list = []
        for i in range(len(preds)):
            pred_list.append(preds[i].item())
        return pred_list


    def predict(self, tensors):
        tensors = torch.stack(tensors)
        with torch.no_grad():
            out = self.ens(tensors)
        pred_list = self.postprocessing(out)
        return pred_list


