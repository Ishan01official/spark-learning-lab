.PHONY: help setup validate smoke clean

PYTHON ?= python

help:
	@echo "Commands:"
	@echo "  make setup     Create .venv and install dependencies"
	@echo "  make validate  Run repository structure checks"
	@echo "  make smoke     Run the smallest PySpark example"
	@echo "  make clean     Remove local Spark artifacts"

setup:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && python -m pip install --upgrade pip
	. .venv/bin/activate && pip install -r requirements.txt

validate:
	$(PYTHON) scripts/validate_repo.py

smoke:
	$(PYTHON) 00_setup/examples/01_hello_spark.py

clean:
	rm -rf metastore_db spark-warehouse output tmp derby.log
