#!/bin/bash

# Test the pgmg test command with the sample test files
echo "Testing pgmg test command..."
echo "This should:"
echo "1. Run plpgsql_check first"
echo "2. Run test_check_function.sql successfully"
echo "3. Run test_failures.test.sql and handle the SQL error gracefully"
echo "4. Continue to run any other test files"
echo

# Run the test command
cargo run -- test sql/api --connection-string "$DATABASE_URL"