#!/bin/bash

# Function to compare the output and display the result
compare_output() {
    local test_case=$1
    local output_file="$EXPECTED_OUTPUT_DIR/output$test_case"
    local temp_file="output.tmp"
    
    # Check if the expected output file exists
    if [ ! -f "$output_file" ]; then
        echo -e "\e[31mError: Expected output file '$output_file' not found.\e[0m"
        return 1
    fi
    
    if diff -q "$temp_file" "$output_file" >/dev/null; then
        echo -e "\e[32mTest case $test_case passed!\e[0m"
        rm "$temp_file"
        return 0
    else
        echo -e "\e[31mTest case $test_case failed. Here's the difference:\e[0m"
        diff -u "$temp_file" "$output_file"
        rm "$temp_file"
        return 1
    fi
}

# Function to run a test case
run_test_case() {
    local test_case=$1
    local command_file="$TEST_CASES_DIR/command$test_case.py"
    local temp_file="output.tmp"
    
    echo -e "\e[1mRunning test case $test_case...\e[0m"
    
    # Copy the command file for the current test case
    if cp "$command_file" "$COMMAND_FILE"; then
        # Run the MPI program with a timeout and capture the output
        timeout "${TIMEOUT}s" mpirun -np "$NUM_PROCESSES" --oversubscribe python3 "$DHT_FILE" > "$temp_file" 2>&1
        
        # Check if the MPI program timed out
        if [ $? -eq 124 ]; then
            echo -e "\e[31mError: Test case $test_case timed out after $TIMEOUT seconds.\e[0m"
            ((failed_tests++))
        else
            # Compare the output with the expected output
            if compare_output "$test_case"; then
                ((passed_tests++))
            else
                ((failed_tests++))
            fi
        fi
    else
        echo -e "\e[31mError: Failed to copy the command file for test case $test_case.\e[0m"
        ((failed_tests++))
    fi
    
    ((total_tests++))
    echo -e "\e[1m-----------------------------\e[0m"
}

# Function to display the test summary
display_summary() {
    echo -e "\e[1m----- Test Summary -----\e[0m"
    echo "Total test cases: $total_tests"
    
    if [ $passed_tests -eq $total_tests ]; then
        echo -e "Passed: \e[32m$passed_tests\e[0m"
    else
        echo "Passed: $passed_tests"
    fi
    
    if [ $failed_tests -eq 0 ]; then
        echo "Failed: $failed_tests"
    else
        echo -e "Failed: \e[31m$failed_tests\e[0m"
    fi
    
    if [ $failed_tests -eq 0 ]; then
        echo -e "\e[32mAll test cases passed!\e[0m"
    else
        echo -e "\e[31mSome test cases failed. Please check the output for details.\e[0m"
    fi
}

# Constants
TEST_CASES_DIR="PublicTestCases"         # Directory containing the test case files
EXPECTED_OUTPUT_DIR="PublicTestCases"    # Directory containing the expected output files
COMMAND_FILE="command.py"                # Name of the command file
COMMAND_BACKUP="command.py.bak"          # Name of the backup file for command.py
DHT_FILE="dht.py"                        # Name of the DHT file
NUM_PROCESSES=8                          # Number of processes to run with mpirun
TIMEOUT=5                                # Timeout value in seconds for each test case

# Initialize counters
total_tests=0
passed_tests=0
failed_tests=0

# Get a list of all command files in the test cases directory
command_files=("$TEST_CASES_DIR"/command*.py)

# Check if there are any command files
if [ ${#command_files[@]} -eq 0 ]; then
    echo "No test cases found in the $TEST_CASES_DIR directory."
    exit 1
fi

# Backup the existing command.py file if it exists
if [ -f "$COMMAND_FILE" ]; then
    mv "$COMMAND_FILE" "$COMMAND_BACKUP"
    echo -e "\e[33mExisting '$COMMAND_FILE' file backed up as '$COMMAND_BACKUP'.\e[0m"
fi

# Extract the test case numbers from the command file names and sort them numerically
test_cases=($(printf '%s\n' "${command_files[@]}" | awk -F'[^0-9]*' '{print $2}' | sort -n))

echo -e "\e[1m----- Running Test Cases -----\e[0m"

# Loop through each test case number and run the test case
for test_case in "${test_cases[@]}"; do
    run_test_case "$test_case"
done

# Display the test summary
display_summary

# Restore the original command.py file if it was backed up
if [ -f "$COMMAND_BACKUP" ]; then
    mv "$COMMAND_BACKUP" "$COMMAND_FILE"
    echo -e "\e[33mRestored '$COMMAND_BACKUP' to '$COMMAND_FILE'.\e[0m"
fi