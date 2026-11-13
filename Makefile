.PHONY: install lint fmt types test ci sim baseline

install:  ; uv sync
lint:     ; uv run ruff check . && uv run ruff format --check .
fmt:      ; uv run ruff check --fix . && uv run ruff format .
types:    ; uv run mypy src
test:     ; uv run pytest
ci: lint types test
sim:      ; uv run python -m petbowl.simulator --devices 100 --hours 24 --seed 42 --sink jsonl:out.jsonl
baseline: ; uv run python -m petbowl.simulator --devices 1000 --hours 24 --seed 42 --sink null
kv-bench: ; uv run python bench/kv_bench.py
db-up: ; docker compose up -d --wait
db-down: ; docker compose down
db-psql: ; docker compose exec db psql -U petbowl
db-schema: ; docker compose exec -T db psql -U petbowl -v ON_ERROR_STOP=1 < src/petbowl/ingest/schema.sql
db-reset: ; docker compose down -v && docker compose up -d --wait