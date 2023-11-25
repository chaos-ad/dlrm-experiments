import logging
import torch
import torch.functional as F
from typing import Callable, List, Optional, Union

#############################################################################

logger = logging.getLogger(__name__)

#############################################################################

class Perceptron(torch.nn.Module):
    def __init__(
        self,
        in_size: int,
        out_size: int,
        bias: bool = True,
        activation: Callable[[torch.Tensor], torch.Tensor] = torch.relu,
        device: Optional[torch.device] = None,
        dtype: torch.dtype = torch.float32,
    ) -> None:
        super().__init__()
        self._out_size = out_size
        self._in_size = in_size
        self._linear: torch.nn.Linear = torch.nn.Linear(
            self._in_size,
            self._out_size,
            bias=bias,
            device=device,
            dtype=dtype,
        )
        self._activation_fn: Callable[[torch.Tensor], torch.Tensor] = activation

    def forward(self, input: torch.Tensor) -> torch.Tensor:
        return self._activation_fn(self._linear(input))


class MLP(torch.nn.Module):
    def __init__(
        self,
        in_size: int,
        layer_sizes: List[int],
        bias: bool = True,
        activation: Callable[[torch.Tensor], torch.Tensor] = torch.relu,
        device: Optional[torch.device] = None,
        dtype: torch.dtype = torch.float32,
    ) -> None:
        super().__init__()

        self._mlp: torch.nn.Module = torch.nn.Sequential(
            *[
                Perceptron(
                    layer_sizes[i - 1] if i > 0 else in_size,
                    layer_sizes[i],
                    bias=bias,
                    activation=activation,
                    device=device,
                    dtype=dtype,
                )
                for i in range(len(layer_sizes))
            ]
        )
 
    def forward(self, input: torch.Tensor) -> torch.Tensor:
        return self._mlp(input)

#############################################################################
