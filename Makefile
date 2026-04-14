PYTHON ?= python3
VENV ?= .venv
UVICORN ?= uvicorn
APP ?= powerbuddy.main:app

.PHONY: venv install run format lint build

venv:
	$(PYTHON) -m venv $(VENV)

install: venv
	. $(VENV)/bin/activate && pip install -U pip && pip install -e .

run:
	. $(VENV)/bin/activate && $(UVICORN) $(APP) --host 0.0.0.0 --port 8000

build:
	. $(VENV)/bin/activate && python -m build
