build:
	go build ./...

check:
	pre-commit run --all-files --show-diff-on-failure

init:
	direnv allow
	pip install pre-commit
	pre-commit install --install-hooks --overwrite

.PHONY: build check init
