"""
models_ai/contracts/__init__.py
Exporta los contratos públicos de la capa de modelos.
    from models_ai.contracts import ModelTrainer, ModelPersistence, ScoreNormalizer, TrainedModel
"""
from models_ai.contracts.model_trainer     import ModelTrainer, TrainedModel
from models_ai.contracts.model_persistence import ModelPersistence
from models_ai.contracts.score_normalizer  import ScoreNormalizer

__all__ = [
    "ModelTrainer",
    "TrainedModel",
    "ModelPersistence",
    "ScoreNormalizer",
]