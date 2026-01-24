CC = gcc
CFLAGS = -Wall -Wextra -Werror -Wpedantic -std=c11 -g -O2
SRCDIR = src
INCDIR = include
BUILDDIR = build
BINDIR = bin

SRC_SOURCES = $(wildcard $(SRCDIR)/*.c)
EXECUTOR_SOURCES = $(wildcard $(SRCDIR)/executor/*.c)
TEST_SOURCES = $(wildcard $(SRCDIR)/tests/*.c)
OBJECTS = $(SRC_SOURCES:$(SRCDIR)/%.c=$(BUILDDIR)/%.o)
OBJECTS += $(EXECUTOR_SOURCES:$(SRCDIR)/executor/%.c=$(BUILDDIR)/executor/%.o)
TEST_OBJECTS = $(TEST_SOURCES:$(SRCDIR)/tests/%.c=$(BUILDDIR)/tests/%.o)
TARGET = $(BINDIR)/db
TEST_TARGET = $(BINDIR)/test_db

.PHONY: all clean debug run test format format-check lint check

all: $(TARGET)

$(TARGET): $(filter-out $(BUILDDIR)/tests.o,$(OBJECTS)) | $(BINDIR)
	$(CC) $(filter-out $(BUILDDIR)/tests.o,$(OBJECTS)) -o $@ -lm

$(BUILDDIR)/%.o: $(SRCDIR)/%.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(BUILDDIR)/executor/%.o: $(SRCDIR)/executor/%.c | $(BUILDDIR)/executor
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(BUILDDIR)/%.o: $(INCDIR)/%.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(BUILDDIR)/tests/%.o: $(SRCDIR)/tests/%.c | $(BUILDDIR)/tests
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(BUILDDIR)/tests:
	mkdir -p $(BUILDDIR)/tests

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(BUILDDIR)/executor:
	mkdir -p $(BUILDDIR)/executor

$(BINDIR):
	mkdir -p $(BINDIR)

$(TEST_TARGET): $(filter-out $(BUILDDIR)/main.o $(BUILDDIR)/tests.o,$(OBJECTS)) $(TEST_OBJECTS) | $(BINDIR)
	$(CC) $(filter-out $(BUILDDIR)/main.o $(BUILDDIR)/tests.o,$(OBJECTS)) $(TEST_OBJECTS) -o $@ -lm

debug: CFLAGS += -DDEBUG -g3
debug: $(TARGET)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf $(BUILDDIR) $(BINDIR)

test: $(TEST_TARGET)
	@echo "Running tests..."
	./$(TEST_TARGET) --all

format:
	@echo "Formatting C source files..."
	@find $(SRCDIR) $(INCDIR) -name '*.c' -o -name '*.h' | xargs clang-format -i
	@echo "Formatting complete!"

format-check:
	@echo "Checking code formatting..."
	@find $(SRCDIR) $(INCDIR) -name '*.c' -o -name '*.h' | xargs clang-format --dry-run --Werror

lint:
	@echo "Running clang-tidy..."
	@find $(SRCDIR) -name '*.c' -exec clang-tidy {} -- -I$(INCDIR) \;

check: format-check
	@echo "Running cppcheck..."
	@cppcheck --enable=all --suppress=missingIncludeSystem --force --quiet $(SRCDIR)/ $(INCDIR)/ 2>&1 	
	@echo ""
	@echo "Running clang-tidy..."
	@$(MAKE) lint
	@echo ""
	@echo "Static analysis complete!"

help:
	@echo "Available targets:"
	@echo "  all          - Build the database system (default)"
	@echo "  debug        - Build with debug flags"
	@echo "  run          - Build and run the database system"
	@echo "  test         - Run unit tests"
	@echo "  format       - Format all C source files with clang-format"
	@echo "  format-check - Check code formatting without modifying files"
	@echo "  lint         - Run clang-tidy linter on source files"
	@echo "  check        - Run all static analysis (format-check + cppcheck + clang-tidy)"
	@echo "  clean        - Remove all build artifacts"
	@echo "  help         - Show this help message"
