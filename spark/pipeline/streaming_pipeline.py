"""
pipeline/streaming_pipeline.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.streaming import StreamingQuery

from config.settings import (
    KAFKA_BROKER, TOPIC_RAW,
    CHECKPOINT_PATH, MAX_OFFSETS_BATCH, TRIGGER_INTERVAL,
)
from processors.batch_processor  import BatchProcessor
from schemas.transaction_schema  import TX_SCHEMA
from utils.logger import get_logger

logger = get_logger("StreamingPipeline")


class StreamingPipeline:
    def __init__(
        self,
        spark:     SparkSession,
        processor: BatchProcessor,
        broker:           str = KAFKA_BROKER,
        topic_raw:        str = TOPIC_RAW,
        checkpoint_path:  str = CHECKPOINT_PATH,
        max_offsets:      int = MAX_OFFSETS_BATCH,
        trigger_interval: str = TRIGGER_INTERVAL,
    ) -> None:
        self._spark            = spark
        self._processor        = processor
        self._broker           = broker
        self._topic_raw        = topic_raw
        self._checkpoint_path  = checkpoint_path
        self._max_offsets      = max_offsets
        self._trigger_interval = trigger_interval
        self._query: StreamingQuery | None = None

    def start(self) -> "StreamingPipeline":
        """
        Returns:
            self — permite encadenar start().await_termination()
        """
        logger.info(f"Conectando al topic '{self._topic_raw}' en {self._broker}")

        raw_stream = (
            self._spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", self._broker)
            .option("subscribe",               self._topic_raw)
            .option("startingOffsets",         "latest")
            .option("failOnDataLoss",          "false")
            .option("maxOffsetsPerTrigger",    str(self._max_offsets))
            .load()
        )

        parsed = (
            raw_stream
            .select(
                from_json(col("value").cast("string"), TX_SCHEMA).alias("tx")
            )
            .select("tx.*")
            .filter(col("transaccionID").isNotNull())
        )

        self._query = (
            parsed.writeStream
            .foreachBatch(self._processor.process)
            .option("checkpointLocation", self._checkpoint_path)
            .trigger(processingTime=self._trigger_interval)
            .start()
        )

        logger.info(
            f"Stream iniciado | "
            f"topic={self._topic_raw} | "
            f"max_offsets={self._max_offsets} | "
            f"trigger={self._trigger_interval} | "
            f"checkpoint={self._checkpoint_path}"
        )
        return self

    def await_termination(self) -> None:
        """
        Raises:
            RuntimeError: si se llama antes de start()
        """
        if self._query is None:
            raise RuntimeError(
                "El stream no ha sido iniciado. Llama a start() primero."
            )
        self._query.awaitTermination()

    def stop(self) -> None:
        
        if self._query is not None:
            self._query.stop()
            logger.info("Stream detenido.")