from pipeline import PipelineFactory


def main() -> None:
    PipelineFactory.build().start().await_termination()

"""
main.py — Entry point del pipeline
Ejecución:
  spark-submit --master spark://spark-master:7077 \\
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \\
    main.py
"""

if __name__ == "__main__":
    main()