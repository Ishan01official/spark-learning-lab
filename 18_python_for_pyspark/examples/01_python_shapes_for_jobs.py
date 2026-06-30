from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class JobConfig:
    app_name: str
    input_path: Path
    output_path: Path
    target_partitions: int = 4


def build_config(environment: str) -> JobConfig:
    base = Path("data") / environment
    return JobConfig(
        app_name=f"orders-etl-{environment}",
        input_path=base / "raw" / "orders",
        output_path=base / "curated" / "orders",
    )


def describe_config(config: JobConfig) -> str:
    return (
        f"{config.app_name}: read {config.input_path}, "
        f"write {config.output_path}, partitions={config.target_partitions}"
    )


def main() -> None:
    config = build_config("dev")
    print(describe_config(config))


if __name__ == "__main__":
    main()
