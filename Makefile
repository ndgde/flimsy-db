BINARY=bin/flimsy-db
MAIN=cmd/main.go
TESTS=./tests

.PHONY: all build run test clean

build:
	@echo "⚪ Building the project..."
	@{ time go build -o $(BINARY) $(MAIN); } 2>&1 | \
		grep real | awk '{print "\t• Build time: " $$2}'
	@echo -e "\t⚫Build completed: $(BINARY)"

run:
	@if [ ! -f $(BINARY) ]; then \
		echo "⚠️  Binary not found"; \
		make -s build; \
	fi
	@echo -e "Running the project...\n"
	@./$(BINARY)

rerun: build run

test:
	@echo "⚪ Running tests..."
	@start_time=$$(date +%s); \
	go test -v $(TESTS) | \
		awk -v green="\033[0;32m" -v red="\033[0;31m" -v yellow="\033[0;33m" -v reset="\033[0m" ' \
		BEGIN { run_count = 0; pass_count = 0; fail_count = 0; } \
		/=== RUN/ { \
			test_start_time = systime(); \
			printf "%s%-30s%s", yellow, substr($$0, index($$0, " ") + 1), reset; \
			run_count++; \
			next; \
		} \
		/PASS/ { \
			test_end_time = systime(); \
			printf " %s%-5s%s (%.2fs)\n", green, "PASS", reset, test_end_time - test_start_time; \
			pass_count++; \
			next; \
		} \
		/FAIL/ { \
			test_end_time = systime(); \
			printf " %s%-5s%s (%.2fs)\n", red, "FAIL", reset, test_end_time - test_start_time; \
			fail_count++; \
			next; \
		} \
		{ print $$0; } \
		END { total = run_count + pass_count + fail_count; printf("\nTotal Tests: %d, Passed: %d, Failed: %d\n", total, pass_count, fail_count); }'; \
	end_time=$$(date +%s); \
	printf "Total Time: $$((end_time - start_time)) seconds\n"; \
	printf "\t⚫ All tests completed\n"

clean:
	@echo "⚪ Cleaning up..."
	@rm -f $(BINARY)
	@echo -e "\t⚫Clean completed"
