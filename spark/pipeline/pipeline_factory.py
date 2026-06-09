"""
pipeline/pipeline_factory.py
"""
from __future__ import annotations

from config.settings import (
    FRAUD_THRESHOLD, MONGO_ENABLED,
    WARMUP_SIZE, RETRAIN_INTERVAL,
    KAFKA_BROKER, TOPIC_RAW, TOPIC_ALERTS,
    CHECKPOINT_PATH, MAX_OFFSETS_BATCH, TRIGGER_INTERVAL,
    CONTAMINATION,
)

from infrastructure.contracts.alert_publisher     import AlertPublisher
from infrastructure.contracts.document_repository import DocumentRepository
from models_ai.isolation_forest_trainer import IsolationForestTrainer
from models_ai.joblib_persistence       import JobLibModelPersistence
from models_ai.sigmoid_normalizer       import SigmoidScoreNormalizer
from models_ai.model_registry           import ModelRegistry
from models_ai.buffers.warmup_buffer         import WarmupBuffer
from models_ai.buffers.sliding_window_buffer import SlidingWindowBuffer
from services.warmup_orchestrator     import WarmupOrchestrator
from services.retraining_orchestrator import RetrainingOrchestrator
from services.alert_evaluator         import AlertEvaluator
from infrastructure.kafka_alert_publisher     import KafkaAlertPublisher
from infrastructure.mongo_document_repository import MongoDocumentRepository
from infrastructure.null_document_repository  import NullDocumentRepository
from infrastructure.spark_session_factory     import SparkSessionFactory
from processors.batch_processor  import BatchProcessor
from pipeline.streaming_pipeline import StreamingPipeline
from utils.logger import get_logger

logger = get_logger("PipelineFactory")


class PipelineFactory:

    @staticmethod
    def build() -> StreamingPipeline:
        """
        Returns:
            StreamingPipeline listo para llamar a start().
        """
        logger.info("Construyendo grafo de dependencias del pipeline...")

        trainer     = IsolationForestTrainer(
            contamination = CONTAMINATION,
            n_estimators  = 100,
        )
        persistence = JobLibModelPersistence()
        normalizer  = SigmoidScoreNormalizer(k=5.0)

        registry = ModelRegistry(
            trainer          = trainer,
            persistence      = persistence,
            normalizer       = normalizer,
            fraud_threshold  = FRAUD_THRESHOLD,
        )

        warmup_buffer = WarmupBuffer(warmup_size=WARMUP_SIZE)
        window_buffer = SlidingWindowBuffer(min_samples=WARMUP_SIZE)

        warmup_orchestrator = WarmupOrchestrator(
            warmup_buffer = warmup_buffer,
            registry      = registry,
        )
        retraining_orchestrator = RetrainingOrchestrator(
            window_buffer    = window_buffer,
            registry         = registry,
            retrain_interval = RETRAIN_INTERVAL,
        )

        evaluator = AlertEvaluator(threshold=FRAUD_THRESHOLD)

        publisher: AlertPublisher = KafkaAlertPublisher(
            broker = KAFKA_BROKER,
            topic  = TOPIC_ALERTS,
        )

        repository: DocumentRepository = (
            MongoDocumentRepository()
            if MONGO_ENABLED
            else NullDocumentRepository()
        )

        mongo_status = "MongoDocumentRepository" if MONGO_ENABLED else "NullDocumentRepository (MONGO_ENABLED=false)"
        logger.info(f"DocumentRepository → {mongo_status}")

        processor = BatchProcessor(
            registry   = registry,
            warmup     = warmup_orchestrator,
            retraining = retraining_orchestrator,
            evaluator  = evaluator,
            publisher  = publisher,
            repository = repository,
        )

        spark = SparkSessionFactory.create()

        pipeline = StreamingPipeline(
            spark            = spark,
            processor        = processor,
            broker           = KAFKA_BROKER,
            topic_raw        = TOPIC_RAW,
            checkpoint_path  = CHECKPOINT_PATH,
            max_offsets      = MAX_OFFSETS_BATCH,
            trigger_interval = TRIGGER_INTERVAL,
        )

        logger.info("Pipeline construido y listo para iniciar.")
        return pipeline