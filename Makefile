CC = gcc
CFLAGS = -Wall -Wextra -Werror -Wpedantic -std=c11 -g -O2 -Iinclude
SRCDIR = src
INCDIR = include
BUILDDIR = build
BINDIR = bin

SRC_SOURCES = $(wildcard $(SRCDIR)/*.c)
OBJECTS = $(SRC_SOURCES:$(SRCDIR)/%.c=$(BUILDDIR)/%.o)
TARGET = $(BINDIR)/db
TEST_TARGET = $(BINDIR)/test_db

.PHONY: all clean debug run test test-integration test-all

all: $(TARGET)

$(TARGET): $(filter-out $(BUILDDIR)/tests.o,$(OBJECTS)) | $(BINDIR)
	$(CC) $(filter-out $(BUILDDIR)/tests.o,$(OBJECTS)) -o $@ -lm

$(BUILDDIR)/%.o: $(SRCDIR)/%.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(BUILDDIR)/%.o: $(INCDIR)/%.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(BUILDDIR)/main.o: $(SRCDIR)/main.c | $(BUILDDIR)
	$(CC) $(CFLAGS) -I$(INCDIR) -c $< -o $@

$(BUILDDIR):
	mkdir -p $(BUILDDIR)

$(BINDIR):
	mkdir -p $(BINDIR)

$(TEST_TARGET): $(filter-out $(BUILDDIR)/main.o $(BUILDDIR)/tests.o,$(OBJECTS)) $(BUILDDIR)/tests.o | $(BINDIR)
	$(CC) $(filter-out $(BUILDDIR)/main.o $(BUILDDIR)/tests.o,$(OBJECTS)) $(BUILDDIR)/tests.o -o $@ -lm

debug: CFLAGS += -DDEBUG -g3
debug: $(TARGET)

run: $(TARGET)
	./$(TARGET)

clean:
	rm -rf $(BUILDDIR) $(BINDIR)

test: $(TEST_TARGET)
	@echo "Running tests..."
	./$(TEST_TARGET) --all


help:
	@echo "Available targets:"
	@echo "  all     - Build the database system (default)"
	@echo "  debug   - Build with debug flags"
	@echo "  run     - Build and run the database system"
	@echo "  test    - Run unit tests"
	@echo "  test-integration - Run integration tests"
	@echo "  test-all - Run both unit and integration tests"
	@echo "  clean   - Remove all build artifacts"
	@echo "  help    - Show this help message"
