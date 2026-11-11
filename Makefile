install: ; uv sync
lint:    ; uv run ruff check . && uv run ruff format --check .
fmt:     ; uv run ruff check --fix . && uv run ruff format .
types:   ; uv run mypy src
test:    ; uv run pytest
ci: lint types test
sim:     ; uv run python -m petbowl.simulator --devices 100 --speedup 3600 --hours 24 --seed 42 --sink jsonl:out.jsonl