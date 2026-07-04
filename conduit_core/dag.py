from __future__ import annotations
from collections import deque
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set
import structlog

from conduit_core.models import TaskDefinition, DAGDefinition

logger = structlog.get_logger(__name__)


class CycleDetectedError(Exception):
    pass


class DAGValidationError(Exception):
    pass


@dataclass
class DAGNode:
    task: TaskDefinition
    in_degree: int = 0
    downstream: List[str] = field(default_factory=list)


class DAG:
    """
    Directed Acyclic Graph representation of a pipeline.

    Implements Kahn's algorithm for topological sort:
    1. Compute in_degree for each node
    2. Start with all nodes having in_degree == 0
    3. Process nodes level by level
    4. If unprocessed nodes remain after traversal: cycle detected

    Parallel execution: all nodes in the same "level"
    (same topological depth) can run simultaneously.
    """

    def __init__(self, definition: DAGDefinition):
        self.definition = definition
        self.name = definition.name
        self._nodes: Dict[str, DAGNode] = {}
        self._build(definition.tasks)
        self._validate()

    def _build(self, tasks: List[TaskDefinition]) -> None:
        task_names = {t.name for t in tasks}

        # Build nodes
        for task in tasks:
            self._nodes[task.name] = DAGNode(task=task)

        # Build edges and in-degrees
        for task in tasks:
            for dep in task.depends_on:
                if dep not in task_names:
                    raise DAGValidationError(
                        f"Task '{task.name}' depends on '{dep}' "
                        f"which is not defined in this DAG"
                    )
                # dep -> task (dep must complete before task)
                self._nodes[dep].downstream.append(task.name)
                self._nodes[task.name].in_degree += 1

    def _validate(self) -> None:
        order = self._topological_sort()
        if len(order) != len(self._nodes):
            raise CycleDetectedError(
                f"DAG '{self.name}' contains a cycle. "
                f"Processed {len(order)}/{len(self._nodes)} nodes."
            )
        logger.info(
            "dag.validated",
            dag=self.name,
            tasks=len(self._nodes),
            execution_order=[t.name for t in order],
        )

    def _topological_sort(self) -> List[TaskDefinition]:
        """
        Kahn's algorithm.
        Returns tasks in valid execution order.
        Raises CycleDetectedError if a cycle exists.
        """
        in_degree: Dict[str, int] = {
            name: node.in_degree for name, node in self._nodes.items()
        }
        queue: deque = deque(
            name for name, deg in in_degree.items() if deg == 0
        )
        order: List[TaskDefinition] = []

        while queue:
            name = queue.popleft()
            order.append(self._nodes[name].task)
            for downstream_name in self._nodes[name].downstream:
                in_degree[downstream_name] -= 1
                if in_degree[downstream_name] == 0:
                    queue.append(downstream_name)

        return order

    def execution_levels(self) -> List[List[TaskDefinition]]:
        """
        Return tasks grouped by execution level.
        Tasks in the same level can run in parallel.

        Level 0 = no dependencies (run first)
        Level 1 = depends only on level 0 tasks
        Level N = depends only on tasks in levels 0..N-1
        """
        levels: List[List[TaskDefinition]] = []
        in_degree: Dict[str, int] = {
            name: node.in_degree for name, node in self._nodes.items()
        }
        remaining: Set[str] = set(self._nodes.keys())

        while remaining:
            # Current level: all remaining tasks with in_degree == 0
            current_level = [
                self._nodes[name].task
                for name in remaining
                if in_degree[name] == 0
            ]
            if not current_level:
                raise CycleDetectedError(
                    f"DAG '{self.name}' has unresolvable dependencies"
                )
            levels.append(current_level)

            # Remove current level, decrement in-degrees
            for task in current_level:
                remaining.discard(task.name)
                for downstream_name in self._nodes[task.name].downstream:
                    in_degree[downstream_name] -= 1

        return levels

    def get_ready_tasks(
        self, completed: Set[str], failed: Set[str]
    ) -> List[TaskDefinition]:
        """
        Return tasks whose dependencies are all satisfied.
        Used for event-driven dispatch: call after each task completes.
        """
        ready = []
        for name, node in self._nodes.items():
            if name in completed or name in failed:
                continue
            deps_met = all(
                dep in completed for dep in node.task.depends_on
            )
            if deps_met:
                ready.append(node.task)
        return ready

    def get_downstream_tasks(self, task_name: str) -> List[str]:
        """All tasks that directly or transitively depend on task_name."""
        result: Set[str] = set()
        queue = deque(self._nodes[task_name].downstream)
        while queue:
            name = queue.popleft()
            if name not in result:
                result.add(name)
                queue.extend(self._nodes[name].downstream)
        return list(result)

    def task_names(self) -> List[str]:
        return list(self._nodes.keys())

    def get_task(self, name: str) -> Optional[TaskDefinition]:
        node = self._nodes.get(name)
        return node.task if node else None

    def __len__(self) -> int:
        return len(self._nodes)
