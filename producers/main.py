"""
main.py — Entry point del producer Vertexon → Kafka
Uso:
    python main.py
    python main.py --intervalo 2 --variacion 5
"""
import argparse

from producer_factory import ProducerFactory


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Vertexon Mock → Kafka Producer (IEEE-CIS v2)"
    )
    parser.add_argument(
        "--intervalo",
        type=float,
        default=2.0,
        help="Segundos entre ciclos (default: 2.0)",
    )
    parser.add_argument(
        "--variacion",
        type=int,
        default=5,
        help="Usuarios generados por ciclo (default: 5)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ProducerFactory.build(args).run()


if __name__ == "__main__":
    main()