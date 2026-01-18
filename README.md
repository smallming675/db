# Simple Database System

A lightweight SQL database implemented in C.

## Features

### Data Types
- `INT` / `INTEGER` - 64-bit signed integers
- `STRING` / `TEXT` - Variable-length strings (up to 255 chars)
- `FLOAT` / `DOUBLE` - Double-precision floating point
- `BOOLEAN` / `BOOL` - True/false values
- `DECIMAL` / `NUMERIC` - Fixed-point decimal
- `BLOB` - Binary data (hex literals: `X'0ABC123'`)
- `DATE` - Date values (YYYY-MM-DD format)
- `TIME` - Time values (HH:MM:SS format)

### SQL Commands

## Building

```bash
make          # Build the database
make clean    # Clean build artifacts
make debug    # Build with debug flags
make run      # Build and run the database CLI
make test     # Run all unit tests
```

## Running

```bash
./bin/db                    # Start interactive CLI
./bin/db --show-logs        # Start with debug logging enabled

-- In the CLI:
.db> SELECT * FROM users;
.db> .LIST                  -- List all tables
.db> .EXIT;                 -- Exit
```

## Running Tests

```bash
make test                   # Run all tests
./bin/test_db --all         # Alternative
./bin/test_db --verbose     # Verbose output
```

## TODO 

- persisting data written to disk
- transactions
- PRIMARY KEY indexing
- FOREIGN KEY constraint 
- Multiple users
