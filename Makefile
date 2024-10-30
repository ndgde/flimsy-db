BINARY=bin/flimsy-db
MAIN=cmd/main.go
TESTS=./tests


BLUE    := \033[0;34m
GREEN   := \033[0;32m
RED     := \033[0;31m
YELLOW  := \033[0;33m
RESET   := \033[0m


define print_header
	@printf "$(BLUE)▶ $(1)$(RESET)\n"
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
endef

define print_task
	@printf "$(BLUE)┌─ $(1)\n"
	@printf "│$(RESET)\n"
endef

define print_success
	@printf "$(GREEN)✓ $(1)$(RESET)\n"
endef

define print_error
	@printf "$(RED)✗ $(1)$(RESET)\n"
endef

.PHONY: all build run test clean

build:
	$(call print_header,Building application)
	$(call print_task,Compiling sources)
	@go build -o $(BINARY) $(MAIN) || ($(call print_error,Build failed) && exit 1)
	$(call print_success,Build completed successfully)

run:
	@if [ ! -f $(BINARY) ]; then \
		echo "⚠️  Binary not found"; \
		make -s build; \
	fi
	@echo -e "Running the project...\n"
	@./$(BINARY)

rerun: build run

clean:
	$(call print_header,Cleaning workspace)
	$(call print_task,Removing build artifacts)
	@rm -f $(BINARY)
	@rm -rf bin/
	@rm -rf dist/
	$(call print_success,Cleanup completed)

test:
	$(call print_header,Running tests)
	@echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
	@start_time=$$(date +%s.%N); \
	go test -v $(TESTS) 2>&1 | \
		awk -v green="$(GREEN)" -v red="$(RED)" -v yellow="$(YELLOW)" -v blue="$(BLUE)" -v reset="$(RESET)" ' \
		BEGIN { \
			passed = 0; \
			failed = 0; \
			skipped = 0; \
			printf "%s┌─ Running Tests ─────────────────────────────\n", blue, reset; \
		} \
		/=== RUN/ { \
			printf "│  %-50s\n", $$3; \
			next; \
		} \
		/--- PASS/ { \
			passed++; \
			printf "│  %s✓%s %-43s%s[%.2fs]%s\n", green, reset, $$3, green, $$5, reset; \
			next; \
		} \
		/--- FAIL/ { \
			failed++; \
			printf "│  %s✗%s %-43s%s[%.2fs]%s\n", red, reset, $$3, red, $$5, reset; \
			next; \
		} \
		/--- SKIP/ { \
			skipped++; \
			printf "│  %s-%s %-45s%sSKIP%s\n", yellow, reset, $$3, yellow, reset; \
			next; \
		} \
		END { \
			printf "%s└───────────────────────────────────────────────\n\n", blue, reset; \
			printf "%s┌─ Test Results ────────────────────────────────\n", blue, reset; \
			printf "│  ✓ Passed:  %-37s\n", sprintf("%s%d%s", green, passed, reset); \
			if (failed > 0) printf "│  ✗ Failed:  %-37s\n", sprintf("%s%d%s", red, failed, reset); \
			if (skipped > 0) printf "│  - Skipped: %-37s\n", sprintf("%s%d%s", yellow, skipped, reset); \
			printf "%s└───────────────────────────────────────────────\n", blue, reset; \
			exit (failed > 0 ? 1 : 0); \
		} \
		'; \
	test_result=$$?; \
	end_time=$$(date +%s.%N); \
	duration=$$(echo "$$end_time - $$start_time" | bc); \
	printf "⏱  Total time: %.2f sec\n" "$$duration"; \
	exit $$test_result
