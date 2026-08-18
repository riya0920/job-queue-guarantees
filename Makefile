.PHONY: test storm
test:
	pytest
storm:
	PYTHONPATH=src python -m jobq.storm all --jobs 1500 --kill-probability 0.25
