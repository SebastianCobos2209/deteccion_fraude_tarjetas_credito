"""
pipeline/streaming_pipeline.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType

from config.settings import (
    KAFKA_BROKER,
    TOPIC_RAW, TOPIC_USUARIOS, TOPIC_TARJETAS,
    CHECKPOINT_PATH, CHECKPOINT_USERS, CHECKPOINT_CARDS,
    MAX_OFFSETS_BATCH, TRIGGER_INTERVAL,
    COL_USERS, COL_CARDS, MONGO_ENABLED,
)
from infrastructure.contracts.document_repository import DocumentRepository
from processors.batch_processor import BatchProcessor
from schemas.transaction_schema import TX_SCHEMA
from schemas.usuario_schema     import USUARIO_SCHEMA
from schemas.tarjeta_schema     import TARJETA_SCHEMA
from utils.logger import get_logger

logger = get_logger("StreamingPipeline")


class StreamingPipeline:
    """
    Encapsula el ciclo de vida de los tres streams.

    Ciclo de vida:
      1. __init__            — recibe dependencias, no lanza nada
      2. start()             — lanza los tres streams, retorna self
      3. await_termination() — bloquea hasta que cualquiera termine

    Uso desde main.py:
        pipeline = StreamingPipeline(...)
        pipeline.start().await_termination()
    """

    def __init__(
        self,
        spark:            SparkSession,
        processor:        BatchProcessor,
        repository:       DocumentRepository,
        broker:           str       = KAFKA_BROKER,
        topic_raw:        str       = TOPIC_RAW,
        topic_usuarios:   str       = TOPIC_USUARIOS,
        topic_tarjetas:   str       = TOPIC_TARJETAS,
        checkpoint_path:  str       = CHECKPOINT_PATH,
        checkpoint_users: str       = CHECKPOINT_USERS,
        checkpoint_cards: str       = CHECKPOINT_CARDS,
        max_offsets:      int       = MAX_OFFSETS_BATCH,
        trigger_interval: str       = TRIGGER_INTERVAL,
    ) -> None:
        self._spark            = spark
        self._processor        = processor
        self._repository       = repository
        self._broker           = broker
        self._topic_raw        = topic_raw
        self._topic_usuarios   = topic_usuarios
        self._topic_tarjetas   = topic_tarjetas
        self._checkpoint_path  = checkpoint_path
        self._checkpoint_users = checkpoint_users
        self._checkpoint_cards = checkpoint_cards
        self._max_offsets      = max_offsets
        self._trigger_interval = trigger_interval

        self._query_txs:      StreamingQuery | None = None
        self._query_usuarios: StreamingQuery | None = None
        self._query_tarjetas: StreamingQuery | None = None

    # ── Ciclo de vida público ─────────────────────────────

    def start(self) -> "StreamingPipeline":
        """
        Lanza los tres streams en paralelo.
        Spark Structured Streaming soporta múltiples queries
        activas en la misma SparkSession.

        Returns:
            self — permite encadenar start().await_termination()
        """
        self._query_txs      = self._start_txs_stream()
        self._query_usuarios = self._start_usuarios_stream()
        self._query_tarjetas = self._start_tarjetas_stream()

        logger.info(
            "Tres streams activos | "
            f"txs='{self._topic_raw}' | "
            f"usuarios='{self._topic_usuarios}' | "
            f"tarjetas='{self._topic_tarjetas}'"
        )
        return self

    def await_termination(self) -> None:
        """
        Bloquea hasta que cualquiera de los tres streams termine o falle.
        awaitAnyTermination() es la forma correcta cuando hay múltiples
        queries — awaitTermination() en una sola query dejaría las otras
        sin supervisión.

        Raises:
            RuntimeError: si se llama antes de start().
        """
        if self._query_txs is None:
            raise RuntimeError(
                "Los streams no han sido iniciados. Llama a start() primero."
            )
        self._spark.streams.awaitAnyTermination()

    def stop(self) -> None:
        """Detiene los tres streams de forma ordenada."""
        for query in [self._query_txs, self._query_usuarios, self._query_tarjetas]:
            if query is not None:
                query.stop()
        logger.info("Todos los streams detenidos.")

    # ── Stream 1: transactions.raw → enrichment + scoring ─

    def _start_txs_stream(self) -> StreamingQuery:
        """
        Consume transactions.raw, enriquece con el modelo ML
        y persiste en transactions_enriched y fraud_alerts.
        """
        logger.info(f"[Stream TXS] Conectando a '{self._topic_raw}'")

        parsed = self._build_parsed_stream(self._topic_raw, TX_SCHEMA)
        parsed = parsed.filter(col("transaccionID").isNotNull())

        query = (
            parsed.writeStream
            .foreachBatch(self._processor.process)
            .option("checkpointLocation", self._checkpoint_path)
            .trigger(processingTime=self._trigger_interval)
            .start()
        )
        logger.info("[Stream TXS] Iniciado.")
        return query

    # ── Stream 2: vertexon.usuarios → user_profiles ───────

    def _start_usuarios_stream(self) -> StreamingQuery:
        """
        Consume vertexon.usuarios y persiste directamente
        en user_profiles usando upsert por usuarioID.
        """
        logger.info(f"[Stream USR] Conectando a '{self._topic_usuarios}'")

        parsed = self._build_parsed_stream(self._topic_usuarios, USUARIO_SCHEMA)
        parsed = parsed.filter(col("usuarioID").isNotNull())

        def procesar_usuarios(df: DataFrame, epoch_id: int) -> None:
            if df.rdd.isEmpty():
                return
            docs = [row.asDict() for row in df.collect()]
            if MONGO_ENABLED:
                self._upsert(COL_USERS, "usuarioID", docs)
            logger.info(
                f"[Stream USR][BATCH {epoch_id:04d}] "
                f"{len(docs)} usuarios procesados"
            )

        query = (
            parsed.writeStream
            .foreachBatch(procesar_usuarios)
            .option("checkpointLocation", self._checkpoint_users)
            .trigger(processingTime=self._trigger_interval)
            .start()
        )
        logger.info("[Stream USR] Iniciado.")
        return query

    # ── Stream 3: vertexon.tarjetas → cards ───────────────

    def _start_tarjetas_stream(self) -> StreamingQuery:
        """
        Consume vertexon.tarjetas y persiste directamente
        en cards usando upsert por tarjetaID.
        """
        logger.info(f"[Stream TAR] Conectando a '{self._topic_tarjetas}'")

        parsed = self._build_parsed_stream(self._topic_tarjetas, TARJETA_SCHEMA)
        parsed = parsed.filter(col("tarjetaID").isNotNull())

        def procesar_tarjetas(df: DataFrame, epoch_id: int) -> None:
            if df.rdd.isEmpty():
                return
            docs = [row.asDict() for row in df.collect()]
            if MONGO_ENABLED:
                self._upsert(COL_CARDS, "tarjetaID", docs)
            logger.info(
                f"[Stream TAR][BATCH {epoch_id:04d}] "
                f"{len(docs)} tarjetas procesadas"
            )

        query = (
            parsed.writeStream
            .foreachBatch(procesar_tarjetas)
            .option("checkpointLocation", self._checkpoint_cards)
            .trigger(processingTime=self._trigger_interval)
            .start()
        )
        logger.info("[Stream TAR] Iniciado.")
        return query

    # ── Helpers privados ──────────────────────────────────

    def _build_parsed_stream(
        self,
        topic:  str,
        schema: StructType,
    ) -> DataFrame:
        """
        Construye el DataFrame de streaming parseado para un topic.
        Reutilizado por los tres streams para evitar duplicación.
        """
        return (
            self._spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self._broker)
            .option("subscribe",               topic)
            .option("startingOffsets",         "latest")
            .option("failOnDataLoss",          "false")
            .option("maxOffsetsPerTrigger",    str(self._max_offsets))
            .load()
            .select(from_json(col("value").cast("string"), schema).alias("data"))
            .select("data.*")
        )

    def _upsert(self, collection: str, id_field: str, docs: list) -> None:
        """
        Upsert de documentos en MongoDB por el campo id_field.
        Upsert en lugar de insert_many para evitar duplicados:
        el producer puede reiniciarse y re-enviar los mismos
        usuarios/tarjetas con UUIDs distintos por ciclo.

        Args:
            collection: nombre de la colección destino.
            id_field:   campo que actúa como clave única (usuarioID / tarjetaID).
            docs:       lista de documentos a insertar o actualizar.
        """
        try:
            from pymongo import MongoClient
            from config.settings import MONGO_URI, MONGO_DB
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
            col    = client[MONGO_DB][collection]
            for doc in docs:
                col.update_one(
                    {id_field: doc[id_field]},
                    {"$set": doc},
                    upsert=True,
                )
            client.close()
        except Exception as e:
            logger.error(f"Error upsert en '{collection}': {e}")