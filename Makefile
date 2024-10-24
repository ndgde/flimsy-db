BINARY=bin/flimsy-db
MAIN=cmd/main.go
TESTS=./...

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
	@go test -v $(TESTS)
	@echo -e "\t⚫All tests completed"

clean:
	@echo "⚪ Cleaning up..."
	@rm -f $(BINARY)
	@echo -e "\t⚫Clean completed"
