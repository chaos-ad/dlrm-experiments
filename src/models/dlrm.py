import logging
import math
import torch
import torch.nn as nn
import torch.functional as F
from typing import Dict, List, Optional, Tuple

import models.mlp

#############################################################################

logger = logging.getLogger(__name__)

#############################################################################

class SparseArch(nn.Module):
    def __init__(self, sparse_feature_sizes: List[int], embedding_dim: int) -> None:
        super().__init__()
        self.embeddings = [torch.nn.Embedding(num_embeddings=size, embedding_dim=embedding_dim) for size in sparse_feature_sizes]
        self.embedding_dim = embedding_dim
        self.embeddings_num = len(self.embeddings)

    def forward(self, features: torch.Tensor) -> torch.Tensor:
        sparse_values: List[torch.Tensor] = []
        for feature_idx, feature_values in enumerate(features.unbind(dim=-1)):
            sparse_values.append(self.embeddings[feature_idx](feature_values))
        return torch.cat(sparse_values, dim=1).reshape(-1, self.embeddings_num, self.embedding_dim)

##############################################################################

class DenseArch(nn.Module):
    def __init__(self, in_features: int, layer_sizes: List[int], device: Optional[torch.device] = None) -> None:
        super().__init__()
        self.model: nn.Module = models.mlp.MLP(in_features, layer_sizes, bias=True, device=device)

    def forward(self, features: torch.Tensor) -> torch.Tensor:
        return self.model(features)

##############################################################################

class InteractionArch(nn.Module):
    def __init__(self, num_sparse_features: int) -> None:
        super().__init__()
        self.num_sparse_features: int = num_sparse_features
        self.triu_indices: torch.Tensor = torch.triu_indices(num_sparse_features + 1, num_sparse_features + 1, offset=1)

    def forward(self, dense_features: torch.Tensor, sparse_features: torch.Tensor) -> torch.Tensor:
        combined_values = torch.cat((dense_features.unsqueeze(1), sparse_features), dim=1)
        # dense/sparse + sparse/sparse interaction
        # size B X (F + F choose 2)
        interactions = torch.bmm(combined_values, torch.transpose(combined_values, 1, 2))
        interactions_flat = interactions[:, self.triu_indices[0], self.triu_indices[1]]
        return torch.cat((dense_features, interactions_flat), dim=1)

##############################################################################

class FinalArch(nn.Module):
    def __init__(self, in_features: int, layer_sizes: List[int], device: Optional[torch.device] = None) -> None:
        super().__init__()
        if len(layer_sizes) <= 1:
            raise ValueError("OverArch must have multiple layers.")
        self.model: nn.Module = nn.Sequential(
            models.mlp.MLP(in_features, layer_sizes[:-1], bias=True, device=device),
            nn.Linear(layer_sizes[-2], layer_sizes[-1], bias=True, device=device),
        )

    def forward(self, features: torch.Tensor) -> torch.Tensor:
        return self.model(features)

##############################################################################

class DLRM(nn.Module):
    def __init__(
        self,
        sparse_feature_dim: int,
        sparse_feature_sizes: List[int],
        dense_in_features: int,
        dense_layer_sizes: List[int],
        final_layer_sizes: List[int],
        dense_device: torch.device = "cpu"
    ) -> None:
        super().__init__()

        self.dense_device = dense_device
        embedding_dim: int = sparse_feature_dim
        num_sparse_features: int = len(sparse_feature_sizes)
        final_in_features: int = (embedding_dim + math.comb(num_sparse_features, 2) + num_sparse_features)
        
        self.sparse_arch = SparseArch(sparse_feature_sizes, sparse_feature_dim)
        self.dense_arch  = DenseArch(in_features=dense_in_features, layer_sizes=dense_layer_sizes, device=self.dense_device)
        self.inter_arch  = InteractionArch(num_sparse_features=num_sparse_features)
        self.final_arch  = FinalArch(in_features=final_in_features, layer_sizes=final_layer_sizes, device=self.dense_device)

    def forward(self, dense_features: torch.Tensor, sparse_features: torch.Tensor) -> torch.Tensor:
        embedded_dense = self.dense_arch(dense_features.to(self.dense_device))
        embedded_sparse = self.sparse_arch(sparse_features)
        concatenated_dense = self.inter_arch(dense_features=embedded_dense.cpu(), sparse_features=embedded_sparse)
        logits = self.final_arch(concatenated_dense.to(self.dense_device))
        return logits

#############################################################################
