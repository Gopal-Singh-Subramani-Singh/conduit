.PHONY: install infra-up infra-down start test demo clean

install:
	pip install -r requirements.txt

infra-up:
	docker compose up redis prometheus grafana -d

infra-down:
	docker compose down

start:
	uvicorn conduit_api.main:app --port 8004 --reload

start-prod:
	uvicorn conduit_api.main:app --port 8004 --workers 4

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=conduit_core --cov-report=term-missing

demo:
	python demo/ml_training_pipeline.py

dlq:
	conduit dlq

dags:
	conduit dags

clean:
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null; true
	find . -name "*.pyc" -delete 2>/dev/null; true
	rm -f conduit.db
