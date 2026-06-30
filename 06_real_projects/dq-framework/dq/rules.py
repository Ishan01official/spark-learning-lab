"""
Reusable DQ framework.
"""

import json
from dataclasses import dataclass, asdict, field
from typing import Callable, List, Literal

from pyspark.sql import DataFrame, Column, functions as F


Severity = Literal["error", "warn"]


@dataclass
class DQRule:
    name: str
    predicate: Callable[[DataFrame], Column]   # passes when predicate is True
    severity: Severity = "error"
    threshold: float = 0.0                     # max violation rate before failing
    description: str = ""


@dataclass
class DQResult:
    name: str
    severity: Severity
    passed: bool
    violations: int
    rate: float
    threshold: float


@dataclass
class DQReport:
    total_rows: int
    results: List[DQResult] = field(default_factory=list)

    def has_errors(self) -> bool:
        return any(r.severity == "error" and not r.passed for r in self.results)

    def has_warnings(self) -> bool:
        return any(r.severity == "warn" and not r.passed for r in self.results)

    def as_dict(self) -> dict:
        d = asdict(self)
        d["has_errors"] = self.has_errors()
        d["has_warnings"] = self.has_warnings()
        return d

    def as_json(self) -> str:
        return json.dumps(self.as_dict(), indent=2, default=str)


class DQException(Exception):
    pass


class DQRunner:
    def __init__(self, rules: List[DQRule]):
        self.rules = rules

    def run(self, df: DataFrame) -> DQReport:
        df = df.cache()  # avoid recomputing per rule
        try:
            total = df.count()
            results = []
            for rule in self.rules:
                violations = df.filter(~rule.predicate(df)).count()
                rate = violations / total if total else 0.0
                passed = rate <= rule.threshold
                results.append(DQResult(
                    name=rule.name,
                    severity=rule.severity,
                    passed=passed,
                    violations=violations,
                    rate=rate,
                    threshold=rule.threshold,
                ))
            return DQReport(total_rows=total, results=results)
        finally:
            df.unpersist()
