from pipeline import PipelineFactory


def main() -> None:
    PipelineFactory.build().start().await_termination()

if __name__ == "__main__":
    main()