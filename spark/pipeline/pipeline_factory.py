"""
pipeline/pipeline_factory.py
"""
from __future__ import annotations

from config.settings import (
    FRAUD_THRESHOLD, MONGO_ENABLED,
    WARMUP_SIZE, RETRAIN_INTERVAL,
    KAFKA_BROKER, TOPIC_RAW, TOPIC_ALERTS,
    TOPIC_USUARIOS, TOPIC_TARJETAS,
    CHECKPOINT_PATH, CHECKPOINT_USERS, CHECKPOINT_CARDS,
    MAX_OFFSETS_BATCH, TRIGGER_INTERVAL,
    CONTAMINATION,
)

# ── Contratos ─────────────────────────────────────────────────
from infrastructure.contracts.alert_publisher     import AlertPublisher
from infrastructure.contracts.document_repository import DocumentRepository

# ── Implementaciones de modelos ───────────────────────────────
from models_ai.isolation_forest_trainer import IsolationForestTrainer
from models_ai.joblib_persistence       import JobLibModelPersistence
from models_ai.sigmoid_normalizer       import SigmoidScoreNormalizer
from models_ai.model_registry           import ModelRegistry

# ── Buffers ───────────────────────────────────────────────────
from models_ai.buffers.warmup_buffer         import WarmupBuffer
from models_ai.buffers.sliding_window_buffer import SlidingWindowBuffer

# ── Servicios ─────────────────────────────────────────────────
from services.warmup_orchestrator     import WarmupOrchestrator
from services.retraining_orchestrator import RetrainingOrchestrator
from services.alert_evaluator         import AlertEvaluator

# ── Infraestructura ───────────────────────────────────────────
from infrastructure.kafka_alert_publisher     import KafkaAlertPublisher
from infrastructure.mongo_document_repository import MongoDocumentRepository
from infrastructure.null_document_repository  import NullDocumentRepository
from infrastructure.spark_session_factory     import SparkSessionFactory

# ── Processor y Pipeline ──────────────────────────────────────
from processors.batch_processor  import BatchProcessor
from pipeline.streaming_pipeline import StreamingPipeline

from infrastructure.mongo_user_profile_repository import MongoUserProfileRepository
from infrastructure.null_user_profile_repository import NullUserProfileRepository
from utils.logger import get_logger

logger = get_logger("PipelineFactory")


class PipelineFactory:
    """
    Fábrica del pipeline completo.

    Uso desde main.py:
        pipeline = PipelineFactory.build()
        pipeline.start().await_termination()
    """

    @staticmethod
    def build() -> StreamingPipeline:
        logger.info("Construyendo grafo de dependencias del pipeline...")

        # ── 1. Implementaciones base del modelo ───────────
        trainer     = IsolationForestTrainer(
            contamination = CONTAMINATION,
            n_estimators  = 300,
        )
        persistence = JobLibModelPersistence()
        normalizer  = SigmoidScoreNormalizer(k=5.0)

        # ── 2. ModelRegistry ──────────────────────────────
        registry = ModelRegistry(
            trainer         = trainer,
            persistence     = persistence,
            normalizer      = normalizer,
            fraud_threshold = FRAUD_THRESHOLD,
        )

        # ── 3. Buffers ────────────────────────────────────
        warmup_buffer = WarmupBuffer(warmup_size=WARMUP_SIZE)
        window_buffer = SlidingWindowBuffer(min_samples=WARMUP_SIZE)

        # ── 4. Orquestadores ──────────────────────────────
        warmup_orchestrator = WarmupOrchestrator(
            warmup_buffer = warmup_buffer,
            registry      = registry,
        )
        retraining_orchestrator = RetrainingOrchestrator(
            window_buffer    = window_buffer,
            registry         = registry,
            retrain_interval = RETRAIN_INTERVAL,
        )

        # ── 5. AlertEvaluator ─────────────────────────────
        evaluator = AlertEvaluator(threshold=FRAUD_THRESHOLD)

        # ── 6. Infraestructura ────────────────────────────
        publisher: AlertPublisher = KafkaAlertPublisher(
            broker = KAFKA_BROKER,
            topic  = TOPIC_ALERTS,
        )

        repository: DocumentRepository = (
            MongoDocumentRepository()
            if MONGO_ENABLED
            else NullDocumentRepository()
        )

        user_repository = (
            MongoUserProfileRepository()
            if MONGO_ENABLED
            else NullUserProfileRepository()
        )

        logger.info(
            f"DocumentRepository → "
            f"{'MongoDocumentRepository' if MONGO_ENABLED else 'NullDocumentRepository'}"
        )

        # ── 7. BatchProcessor ─────────────────────────────
        processor = BatchProcessor(
            registry   = registry,
            warmup     = warmup_orchestrator,
            retraining = retraining_orchestrator,
            evaluator  = evaluator,
            publisher  = publisher,
            repository = repository,
            user_repository = user_repository,
        )

        # ── 8. SparkSession ───────────────────────────────
        spark = SparkSessionFactory.create()

        # ── 9. StreamingPipeline ──────────────────────────
        # repository también se pasa a StreamingPipeline porque
        # los streams de usuarios y tarjetas hacen upsert directo
        # en MongoDB — no pasan por BatchProcessor.
        pipeline = StreamingPipeline(
            spark            = spark,
            processor        = processor,
            repository       = repository,
            broker           = KAFKA_BROKER,
            topic_raw        = TOPIC_RAW,
            topic_usuarios   = TOPIC_USUARIOS,
            topic_tarjetas   = TOPIC_TARJETAS,
            checkpoint_path  = CHECKPOINT_PATH,
            checkpoint_users = CHECKPOINT_USERS,
            checkpoint_cards = CHECKPOINT_CARDS,
            max_offsets      = MAX_OFFSETS_BATCH,
            trigger_interval = TRIGGER_INTERVAL,
        )

        logger.info("Pipeline construido y listo para iniciar.")
        return pipeline