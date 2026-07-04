"""
Conduit ML training pipeline demo.
Demonstrates a realistic ML pipeline with parallel tasks,
retries, resource declarations, and cron scheduling.

Usage:
  python demo/ml_training_pipeline.py
  Requires: Conduit running at http://localhost:8004
"""
from __future__ import annotations
import time
import random
import httpx

import sdk.client as conduit


# ── Task definitions ───────────────────────────────────────────────────────────

@conduit.task(
    name="fetch_data",
    retries=2,
    cpu_cores=1.0,
    memory_gb=2.0,
    timeout_seconds=300,
)
def fetch_data(**kwargs) -> dict:
    print("  [fetch_data] Downloading training data...")
    time.sleep(0.5)
    return {
        "rows": 100_000,
        "path": "/data/raw.parquet",
        "schema": ["user_id", "feature_a", "feature_b", "label"],
    }


@conduit.task(
    name="validate_data",
    depends_on=["fetch_data"],
    retries=1,
    cpu_cores=1.0,
    memory_gb=1.0,
)
def validate_data(fetch_data_result: dict, **kwargs) -> dict:
    print(f"  [validate_data] Validating {fetch_data_result['rows']} rows...")
    time.sleep(0.3)
    null_pct = random.uniform(0, 0.02)
    if null_pct > 0.05:
        raise ValueError(f"Too many null values: {null_pct:.1%}")
    return {"valid": True, "null_pct": null_pct, "path": fetch_data_result["path"]}


@conduit.task(
    name="compute_features",
    depends_on=["validate_data"],
    retries=1,
    cpu_cores=2.0,
    memory_gb=4.0,
)
def compute_features(validate_data_result: dict, **kwargs) -> dict:
    print("  [compute_features] Running feature engineering...")
    time.sleep(0.8)
    return {
        "feature_count": 47,
        "path": "/data/features.parquet",
        "rows": 95_000,
    }


# These two can run in PARALLEL after compute_features
@conduit.task(
    name="train_model",
    depends_on=["compute_features"],
    retries=0,
    cpu_cores=4.0,
    memory_gb=8.0,
    timeout_seconds=1800,
)
def train_model(compute_features_result: dict, **kwargs) -> dict:
    print("  [train_model] Training model (this is the slow step)...")
    time.sleep(1.5)
    return {
        "model_path": "/models/fraud_v3.pt",
        "accuracy": 0.943,
        "auc": 0.981,
        "feature_count": compute_features_result["feature_count"],
    }


@conduit.task(
    name="compute_baseline",
    depends_on=["compute_features"],
    retries=1,
    cpu_cores=1.0,
    memory_gb=2.0,
)
def compute_baseline(compute_features_result: dict, **kwargs) -> dict:
    print("  [compute_baseline] Computing baseline metrics...")
    time.sleep(0.4)
    return {"baseline_accuracy": 0.871, "baseline_auc": 0.924}


@conduit.task(
    name="evaluate_model",
    depends_on=["train_model", "compute_baseline"],
    retries=0,
    cpu_cores=1.0,
    memory_gb=2.0,
)
def evaluate_model(
    train_model_result: dict,
    compute_baseline_result: dict,
    **kwargs,
) -> dict:
    print("  [evaluate_model] Comparing model vs baseline...")
    acc_lift = (
        train_model_result["accuracy"]
        - compute_baseline_result["baseline_accuracy"]
    )
    if acc_lift < 0:
        raise ValueError(
            f"Model regression: {acc_lift:.3f} accuracy drop vs baseline"
        )
    return {
        "passed": True,
        "accuracy_lift": round(acc_lift, 4),
        "model_path": train_model_result["model_path"],
    }


@conduit.task(
    name="deploy_model",
    depends_on=["evaluate_model"],
    retries=2,
    cpu_cores=1.0,
    memory_gb=1.0,
)
def deploy_model(evaluate_model_result: dict, **kwargs) -> dict:
    print(
        f"  [deploy_model] Deploying model "
        f"(accuracy lift: +{evaluate_model_result['accuracy_lift']:.1%})..."
    )
    time.sleep(0.3)
    return {
        "deployed": True,
        "endpoint": "http://hermes:8000/v1/chat/completions",
        "version": "fraud_v3",
    }


# ── DAG definition ─────────────────────────────────────────────────────────────

@conduit.dag(
    name="fraud_training_pipeline",
    schedule="0 2 * * *",  # daily at 2am
    description="Train and deploy fraud detection model",
)
def fraud_training_pipeline():
    return [
        fetch_data,
        validate_data,
        compute_features,
        train_model,        # parallel with compute_baseline
        compute_baseline,   # parallel with train_model
        evaluate_model,
        deploy_model,
    ]


# ── Demo runner ────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("CONDUIT ML PIPELINE DEMO")
    print("=" * 60)
    print()
    print("Pipeline: fraud_training_pipeline")
    print("Stages:")
    print("  1. fetch_data")
    print("  2. validate_data")
    print("  3. compute_features")
    print("  4. train_model ─┬─ (parallel)")
    print("     compute_baseline ┘")
    print("  5. evaluate_model")
    print("  6. deploy_model")
    print()

    try:
        with httpx.Client(timeout=3) as client:
            client.get("http://localhost:8004/health")
    except Exception:
        print("Conduit not running. Start with:")
        print("  uvicorn conduit_api.main:app --port 8004")
        print()
        print("Or run tasks directly (no server needed):")
        _run_local()
        return

    print("Triggering pipeline via REST API...")
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(
                "http://localhost:8004/runs",
                json={
                    "dag_name": "fraud_training_pipeline",
                    "input_data": {"dataset_version": "2024-q1"},
                },
            )
            resp.raise_for_status()
            result = resp.json()
            run_id = result["run_id"]
            print(f"Run ID: {run_id}")
            print()
            print("Monitor with:")
            print(f"  conduit status {run_id}")
            print(f"  curl http://localhost:8004/runs/{run_id}")
            print(f"  http://localhost:8004/docs")
    except Exception as e:
        print(f"API trigger failed: {e}")
        _run_local()


def _run_local():
    """Run pipeline tasks directly without the server."""
    print("Running pipeline locally...")
    try:
        r1 = fetch_data()
        print(f"  fetch_data: {r1['rows']} rows")
        r2 = validate_data(fetch_data_result=r1)
        r3 = compute_features(validate_data_result=r2)
        r4 = train_model(compute_features_result=r3)
        r5 = compute_baseline(compute_features_result=r3)
        r6 = evaluate_model(
            train_model_result=r4,
            compute_baseline_result=r5,
        )
        r7 = deploy_model(evaluate_model_result=r6)
        print()
        print("Pipeline complete!")
        print(f"  Model: {r7['version']}")
        print(f"  Endpoint: {r7['endpoint']}")
        print(f"  Accuracy lift: +{r6['accuracy_lift']:.1%}")
    except Exception as e:
        print(f"  Failed: {e}")


if __name__ == "__main__":
    main()
