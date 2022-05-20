build:
	go build -v ./...

check:
	pre-commit run --all-files --show-diff-on-failure

dev:
	docker compose up

init:
	pip install pre-commit
	pre-commit install --install-hooks --overwrite

test:
	go test -v -race ./...

.PHONY: build check dev init test
