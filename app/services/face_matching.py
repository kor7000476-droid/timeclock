from __future__ import annotations

import json
import math
from typing import Iterable, List, Optional, Tuple


def parse_embedding(raw: str) -> List[float]:
    values = json.loads(raw)
    return [float(v) for v in values]


def euclidean_distance(v1: List[float], v2: List[float]) -> float:
    if len(v1) != len(v2):
        raise ValueError("embedding dimension mismatch")
    return math.sqrt(sum((a - b) ** 2 for a, b in zip(v1, v2)))


def best_match(
    embedding: List[float],
    candidates: Iterable[Tuple[str, str, List[float]]],
    threshold: float,
) -> Tuple[bool, Optional[str], Optional[str], float, Optional[float]]:
    best_distance = None
    best_employee_code = None
    best_employee_name = None

    for employee_code, employee_name, candidate in candidates:
        try:
            distance = euclidean_distance(embedding, candidate)
        except ValueError:
            continue

        if best_distance is None or distance < best_distance:
            best_distance = distance
            best_employee_code = employee_code
            best_employee_name = employee_name

    if best_distance is None or best_distance > threshold:
        conf = 0.0 if best_distance is None else max(0.0, 1.0 - best_distance)
        return False, None, None, round(conf, 4), None if best_distance is None else round(best_distance, 4)

    confidence = max(0.0, 1.0 - best_distance)
    return True, best_employee_code, best_employee_name, round(confidence, 4), round(best_distance, 4)


def top2_matches(
    embedding: List[float],
    candidates: Iterable[Tuple[str, str, List[float]]],
) -> Tuple[
    Optional[Tuple[str, str, float]],
    Optional[Tuple[str, str, float]],
]:
    """
    Returns (best, second) where each is (employee_code, employee_name, distance).
    """
    best = None
    second = None

    for employee_code, employee_name, candidate in candidates:
        try:
            distance = euclidean_distance(embedding, candidate)
        except ValueError:
            continue
        item = (employee_code, employee_name, float(distance))
        if best is None or item[2] < best[2]:
            second = best
            best = item
        elif second is None or item[2] < second[2]:
            second = item

    return best, second
