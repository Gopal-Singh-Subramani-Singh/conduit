from __future__ import annotations

import os
import time
from typing import Optional
import typer
from rich.console import Console
from rich.table import Table
from rich import box
import httpx

app = typer.Typer(
    name="conduit",
    help="Event-driven ML pipeline orchestrator",
    add_completion=False,
)
console = Console()

# Configurable via CONDUIT_URL env var or --url option on each command
_DEFAULT_URL = os.environ.get("CONDUIT_URL", "http://localhost:8004")


def _url(base: Optional[str]) -> str:
    return (base or _DEFAULT_URL).rstrip("/")


def _headers() -> dict:
    """Include API key if set in the environment."""
    api_key = os.environ.get("CONDUIT_API_KEY", "")
    return {"X-API-Key": api_key} if api_key else {}


# ── Commands ───────────────────────────────────────────────────────────────────

@app.command("run")
def cmd_run(
    dag_name: str = typer.Argument(..., help="DAG name to trigger"),
    wait: bool = typer.Option(False, "--wait", "-w", help="Wait for completion"),
    base_url: Optional[str] = typer.Option(None, "--url", help="Conduit base URL"),
):
    """Trigger a pipeline run."""
    url = _url(base_url)
    try:
        with httpx.Client(timeout=10, headers=_headers()) as client:
            resp = client.post(
                f"{url}/runs",
                json={"dag_name": dag_name, "input_data": {}},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        console.print(f"[red]✗ Server error {e.response.status_code}:[/red] {e.response.text}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗ Failed:[/red] {e}")
        raise typer.Exit(1)

    console.print(
        f"[green]✓[/green] Run [bold]{data['run_id']}[/bold] queued for "
        f"[cyan]{dag_name}[/cyan]"
    )
    if wait:
        console.print("Waiting for completion...")
        _wait_for_run(data["run_id"], url)


def _wait_for_run(run_id: str, base_url: str, poll_interval: float = 2.0) -> None:
    terminal = {"success", "failed", "partial", "cancelled"}
    max_failures = 10
    failures = 0
    while True:
        try:
            with httpx.Client(timeout=5, headers=_headers()) as client:
                resp = client.get(f"{base_url}/runs/{run_id}")
                resp.raise_for_status()
                data = resp.json()
            failures = 0
            state = data["state"]
            if state in terminal:
                color = "green" if state == "success" else "red"
                console.print(f"[{color}]{state.upper()}[/{color}]")
                return
        except Exception as exc:
            failures += 1
            console.print(f"[dim]Poll failed ({failures}/{max_failures}): {exc}[/dim]")
            if failures >= max_failures:
                console.print("[red]✗ Too many poll failures — giving up[/red]")
                raise typer.Exit(1)
        time.sleep(poll_interval)


@app.command("status")
def cmd_status(
    run_id: str = typer.Argument(..., help="Run ID"),
    base_url: Optional[str] = typer.Option(None, "--url"),
):
    """Get status of a pipeline run."""
    url = _url(base_url)
    try:
        with httpx.Client(timeout=5, headers=_headers()) as client:
            resp = client.get(f"{url}/runs/{run_id}")
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as e:
        console.print(f"[red]✗ {e.response.status_code}:[/red] {e.response.text}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗ Failed:[/red] {e}")
        raise typer.Exit(1)

    state = data["state"]
    color = (
        "green"
        if state == "success"
        else "red"
        if state in ("failed", "partial")
        else "yellow"
    )
    console.print(
        f"\nRun [bold]{run_id}[/bold] — [{color}]{state.upper()}[/{color}]"
        f" — {data['dag_name']}"
    )

    t = Table(box=box.SIMPLE)
    t.add_column("Task", style="cyan")
    t.add_column("State")
    t.add_column("Attempt", justify="right")
    t.add_column("Error")
    for task_name, tr in data.get("task_runs", {}).items():
        s = tr["state"]
        sc = "green" if s == "success" else "red" if s in ("failed", "dlq") else "yellow"
        t.add_row(
            task_name,
            f"[{sc}]{s}[/{sc}]",
            str(tr["attempt"]),
            (tr.get("error") or "")[:60],
        )
    console.print(t)


@app.command("list")
def cmd_list(
    dag_name: Optional[str] = typer.Option(None, "--dag", "-d"),
    limit: int = typer.Option(20, "--limit", "-n"),
    base_url: Optional[str] = typer.Option(None, "--url"),
):
    """List recent pipeline runs."""
    url = _url(base_url)
    try:
        params: dict = {"limit": limit}
        if dag_name:
            params["dag_name"] = dag_name
        with httpx.Client(timeout=5, headers=_headers()) as client:
            resp = client.get(f"{url}/runs", params=params)
            resp.raise_for_status()
            runs = resp.json()
    except httpx.HTTPStatusError as e:
        console.print(f"[red]✗ {e.response.status_code}:[/red] {e.response.text}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗ Failed:[/red] {e}")
        raise typer.Exit(1)

    t = Table(title="Pipeline Runs", box=box.SIMPLE)
    t.add_column("Run ID", style="cyan")
    t.add_column("DAG")
    t.add_column("State")
    t.add_column("Trigger")
    t.add_column("Started")
    for r in runs:
        s = r["state"]
        sc = "green" if s == "success" else "red" if s in ("failed",) else "yellow"
        t.add_row(
            r["run_id"],
            r["dag_name"],
            f"[{sc}]{s}[/{sc}]",
            r.get("trigger", "manual"),
            str(r["started_at"])[:16],
        )
    console.print(t)


@app.command("dags")
def cmd_dags(base_url: Optional[str] = typer.Option(None, "--url")):
    """List registered DAGs."""
    url = _url(base_url)
    try:
        with httpx.Client(timeout=5, headers=_headers()) as client:
            resp = client.get(f"{url}/dags")
            resp.raise_for_status()
            dags = resp.json()
    except httpx.HTTPStatusError as e:
        console.print(f"[red]✗ {e.response.status_code}:[/red] {e.response.text}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗ Failed:[/red] {e}")
        raise typer.Exit(1)

    t = Table(title="Registered DAGs", box=box.SIMPLE)
    t.add_column("Name", style="cyan")
    t.add_column("Tasks", justify="right")
    t.add_column("Schedule")
    t.add_column("Description")
    for d in dags:
        t.add_row(
            d["name"],
            str(d["task_count"]),
            d.get("schedule") or "—",
            d.get("description", "")[:50],
        )
    console.print(t)


@app.command("dlq")
def cmd_dlq(
    limit: int = typer.Option(20, "--limit", "-n"),
    base_url: Optional[str] = typer.Option(None, "--url"),
):
    """List dead letter queue entries."""
    url = _url(base_url)
    try:
        with httpx.Client(timeout=5, headers=_headers()) as client:
            resp = client.get(f"{url}/dlq", params={"limit": limit})
            resp.raise_for_status()
            entries = resp.json()
    except httpx.HTTPStatusError as e:
        console.print(f"[red]✗ {e.response.status_code}:[/red] {e.response.text}")
        raise typer.Exit(1)
    except Exception as e:
        console.print(f"[red]✗[/red] {e}")
        raise typer.Exit(1)

    if not entries:
        console.print("[dim]Dead letter queue is empty[/dim]")
        return

    t = Table(title="Dead Letter Queue", box=box.SIMPLE)
    t.add_column("Task Run ID", style="cyan")
    t.add_column("DAG")
    t.add_column("Task")
    t.add_column("Attempts", justify="right")
    t.add_column("Error")
    for e in entries:
        t.add_row(
            e["task_run_id"][:16],
            e["dag_name"],
            e["task_name"],
            str(e["attempt"]),
            e["error"][:60],
        )
    console.print(t)


if __name__ == "__main__":
    app()
