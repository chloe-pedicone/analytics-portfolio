"""Build a Fabric `notebookutils.notebook.runMultiple` DAG from a short spec.

Pattern from CS2: one shared "spine" notebook, then several fact notebooks
that depend on it and run in parallel.

Usage in a Fabric notebook:
    from fabric_utils.run_dag import build_dag
    dag = build_dag(
        [
            ("NB_02_CaseSpine", []),
            ("NB_03A_FactA", ["NB_02_CaseSpine"]),
            ("NB_03B_FactB", ["NB_02_CaseSpine"]),
            ("NB_03C_FactC", ["NB_02_CaseSpine"]),
        ],
        args={"load_mode": "incremental"},
    )
    notebookutils.notebook.runMultiple(dag)
"""
from __future__ import annotations

from typing import Dict, List, Optional, Sequence, Tuple


def build_dag(
    steps: Sequence[Tuple[str, List[str]]],
    args: Optional[Dict[str, object]] = None,
    timeout_per_cell_s: int = 1800,
    total_timeout_s: int = 7200,
    concurrency: int = 4,
) -> dict:
    names = [n for n, _ in steps]
    if len(names) != len(set(names)):
        raise ValueError("Duplicate notebook names in steps")
    known = set(names)
    for name, deps in steps:
        unknown = set(deps) - known
        if unknown:
            raise ValueError(f"{name} depends on unknown step(s): {sorted(unknown)}")
    _check_acyclic(steps)

    return {
        "activities": [
            {
                "name": name,
                "path": name,
                "timeoutPerCellInSeconds": timeout_per_cell_s,
                "args": dict(args or {}),
                "dependencies": list(deps),
            }
            for name, deps in steps
        ],
        "timeoutInSeconds": total_timeout_s,
        "concurrency": concurrency,
    }


def _check_acyclic(steps: Sequence[Tuple[str, List[str]]]) -> None:
    graph = {n: list(d) for n, d in steps}
    state: Dict[str, int] = {}  # 1 = visiting, 2 = done

    def visit(n: str, path: List[str]) -> None:
        if state.get(n) == 2:
            return
        if state.get(n) == 1:
            raise ValueError("Cycle in DAG: " + " -> ".join(path + [n]))
        state[n] = 1
        for d in graph[n]:
            visit(d, path + [n])
        state[n] = 2

    for n in graph:
        visit(n, [])
