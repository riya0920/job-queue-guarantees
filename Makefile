.PHONY: test storm storm-mp scaling cli
test:
	pytest
storm:
	PYTHONPATH=src python -m jobq.storm all --jobs 1500 --kill-probability 0.25
storm-mp:
	PYTHONPATH=src python -m jobq.bench storm --jobs 1200 --workers 4
scaling:
	PYTHONPATH=src python -u -m jobq.bench scaling --jobs 1500 --worker-counts 1 2 4 8
cli:
	PYTHONPATH=src python -m jobq.cli --db data/bench/storm.db stats
