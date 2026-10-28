PY ?= python3

.PHONY: help lint unit integration test system-test reliability test-all demo stress clean

help:
	@echo "targets: lint | test | system-test | reliability | test-all | demo | stress | clean"

lint:
	$(PY) -m compileall -q libs services tests scripts
	$(PY) -c "import libs.mesh.httpkit, services.kpilot.app, services.api_gateway.app; print('imports ok')"

unit:
	$(PY) -m scripts.run_tests --label unit --start-dir tests/unit

integration:
	$(PY) -m scripts.run_tests --label integration --start-dir tests/integration

test: unit integration

system-test:
	$(PY) -m scripts.run_tests --label system --start-dir tests/system

reliability:
	$(PY) -m scripts.run_tests --label reliability --start-dir tests/reliability

test-all:
	$(PY) -m scripts.run_tests --label all --start-dir tests/unit --start-dir tests/integration --start-dir tests/system --start-dir tests/reliability

demo:
	$(PY) -m scripts.demo

stress:
	$(PY) -m scripts.stress

clean:
	rm -rf runtime
