"""Topology as data. Adding a queue/consumer = one plugin file + entries here."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from petbowl.console.nodes.base import NodePlugin


@dataclass(frozen=True)
class Edge:
    src: str
    dst: str
    label: str = ""
    planned: bool = False  # drawn dashed: a hop that is designed but not built yet


def describe(nodes: list[NodePlugin], edges: list[Edge]) -> dict[str, Any]:
    ids = {n.id for n in nodes}
    for e in edges:
        if e.src not in ids or e.dst not in ids:
            raise ValueError(f"edge {e.src}->{e.dst} references unknown node")
    return {
        "nodes": [
            {
                "id": n.id,
                "type": n.type,
                "label": n.label,
                "snapshot": n.snapshot(),
                "actions": sorted(n.actions()),
            }
            for n in nodes
        ],
        "edges": [
            {"src": e.src, "dst": e.dst, "label": e.label, "planned": e.planned} for e in edges
        ],
    }
