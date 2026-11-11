.PHONY: install lint fmt types test ci sim baseline

install:  ; uv sync
lint:     ; uv run ruff check . && uv run ruff format --check .
fmt:      ; uv run ruff check --fix . && uv run ruff format .
types:    ; uv run mypy src
test:     ; uv run pytest
ci: lint types test
sim:      ; uv run python -m petbowl.simulator --devices 100 --hours 24 --seed 42 --sink jsonl:out.jsonl
baseline: ; uv run python -m petbowl.simulator --devices 1000 --hours 24 --seed 42 --sink null