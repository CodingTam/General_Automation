#!/bin/bash
# Run all tests with output sent to files only

# Variables
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_DIR="test_results"
RESULTS_FILE="$RESULTS_DIR/framework_tests_$TIMESTAMP.txt"
SUMMARY_FILE="$RESULTS_DIR/summary_$TIMESTAMP.txt"
HTML_SUMMARY="$RESULTS_DIR/summary_$TIMESTAMP.html"
LATEST_MARKER="latest_test_results.txt"

# Create results directory if it doesn't exist
mkdir -p "$RESULTS_DIR"

# Initialize summary file
echo "FRAMEWORK SELF-TESTS SUMMARY - $(date)" > "$SUMMARY_FILE"
echo "=================================================" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

# Initialize results file
echo "FRAMEWORK SELF-TESTS DETAILED RESULTS - $(date)" > "$RESULTS_FILE"
echo "=================================================" >> "$RESULTS_FILE"
echo "" >> "$RESULTS_FILE"

# Initialize HTML summary
cat > "$HTML_SUMMARY" << EOL
<!DOCTYPE html>
<html>
<head>
    <title>Framework Test Results</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h1, h2 { color: #333366; }
        table { border-collapse: collapse; width: 100%; margin-bottom: 20px; }
        th, td { padding: 8px; text-align: left; border: 1px solid #ddd; }
        th { background-color: #f2f2f2; }
        tr:nth-child(even) { background-color: #f9f9f9; }
        .pass { color: green; }
        .fail { color: red; }
        .summary { background-color: #f5f5f5; padding: 10px; margin-top: 20px; }
    </style>
</head>
<body>
    <h1>Framework Test Results</h1>
    <p>Generated on: $(date)</p>
    
    <h2>Test Case Summary</h2>
    <table>
        <tr>
            <th>Module Tested</th>
            <th>Test Case</th>
            <th>Status</th>
            <th>Error Message</th>
            <th>Details</th>
        </tr>
EOL

# Test files to run
TEST_FILES=(
  "tests/framework_self_tests/test_yaml_processor.py"
  "tests/framework_self_tests/test_logger.py"
  "tests/framework_self_tests/test_db_handler.py"
  "tests/framework_self_tests/test_cli.py"
)

# Initialize counters
TOTAL_TESTS=0
TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_ERROR=0
FILE_COUNT=0

# Create test case details array
declare -A MODULE_NAMES
MODULE_NAMES["test_yaml_processor.py"]="YAML Processor"
MODULE_NAMES["test_logger.py"]="Logger"
MODULE_NAMES["test_db_handler.py"]="DB Handler"
MODULE_NAMES["test_cli.py"]="CLI Interface"

# Check if console output is requested
CONSOLE_OUTPUT=false
for arg in "$@"; do
  if [ "$arg" == "--console" ]; then
    CONSOLE_OUTPUT=true
  fi
done

# Print header to console if console output is enabled
if [ "$CONSOLE_OUTPUT" = true ]; then
  echo ""
  echo "================================================================"
  echo "               FRAMEWORK SELF-TESTS EXECUTION                   "
  echo "================================================================"
  echo ""
  echo "Starting test execution at $(date)"
  echo ""
fi

# Process each test file
for TEST_FILE in "${TEST_FILES[@]}"; do
  if [ ! -f "$TEST_FILE" ]; then
    echo "Test file not found: $TEST_FILE" >> "$RESULTS_FILE"
    if [ "$CONSOLE_OUTPUT" = true ]; then
      echo "Test file not found: $TEST_FILE"
    fi
    echo "" >> "$RESULTS_FILE"
    continue
  fi
  
  # Get module name
  BASE_NAME=$(basename "$TEST_FILE")
  MODULE_NAME=${MODULE_NAMES[$BASE_NAME]:-"Unknown Module"}
  
  # Run test and capture output
  echo "Running tests from: $TEST_FILE" >> "$RESULTS_FILE"
  echo "------------------------------------------------------------" >> "$RESULTS_FILE"
  
  if [ "$CONSOLE_OUTPUT" = true ]; then
    echo "------------------------------------------------------------"
    echo "Running tests from: $TEST_FILE (Module: $MODULE_NAME)"
    echo "------------------------------------------------------------"
  fi
  
  TEMP_OUTPUT="/tmp/test_output_$TIMESTAMP.txt"
  
  # Run test with or without console output
  if [ "$CONSOLE_OUTPUT" = true ]; then
    # Run test and display output in real-time while capturing it
    python3 -m unittest "$TEST_FILE" | tee "$TEMP_OUTPUT"
  else
    # Run test and only capture output to file
    python3 -m unittest "$TEST_FILE" > "$TEMP_OUTPUT" 2>&1
  fi
  
  # Copy output to results file
  cat "$TEMP_OUTPUT" >> "$RESULTS_FILE"
  echo "" >> "$RESULTS_FILE"
  echo "" >> "$RESULTS_FILE"
  
  # Process results
  if grep -q "OK" "$TEMP_OUTPUT"; then
    # Extract test count
    TEST_COUNT=$(grep "Ran " "$TEMP_OUTPUT" | sed -E 's/.*Ran ([0-9]+) tests.*/\1/')
    TOTAL_TESTS=$((TOTAL_TESTS + TEST_COUNT))
    TOTAL_PASS=$((TOTAL_PASS + TEST_COUNT))
    FILE_COUNT=$((FILE_COUNT + 1))
    
    # Add to summary
    echo "PASS - $TEST_FILE" >> "$SUMMARY_FILE"
    echo "  Tests: $TEST_COUNT, Passed: $TEST_COUNT, Failed: 0, Errors: 0" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"
    
    # Print to console if enabled
    if [ "$CONSOLE_OUTPUT" = true ]; then
      echo ""
      echo "✅ PASS - Module: $MODULE_NAME ($TEST_COUNT tests)"
      echo ""
    fi
    
    # Extract individual test names
    TEST_NAMES=$(grep -o "test_[a-zA-Z0-9_]*" "$TEMP_OUTPUT" | sort | uniq)
    
    # Add to HTML summary
    for TEST_NAME in $TEST_NAMES; do
      cat >> "$HTML_SUMMARY" << EOL
        <tr>
            <td>${MODULE_NAME}</td>
            <td>${TEST_NAME}</td>
            <td class="pass">PASS</td>
            <td>N/A</td>
            <td>Test passed successfully</td>
        </tr>
EOL
    done
    
  elif grep -q "FAIL" "$TEMP_OUTPUT"; then
    # Extract test counts
    TEST_COUNT=$(grep "Ran " "$TEMP_OUTPUT" | sed -E 's/.*Ran ([0-9]+) tests.*/\1/')
    FAIL_LINE=$(grep "FAILED" "$TEMP_OUTPUT")
    FAILURES=$(echo "$FAIL_LINE" | grep -oE "failures=[0-9]+" | cut -d= -f2)
    ERRORS=$(echo "$FAIL_LINE" | grep -oE "errors=[0-9]+" | cut -d= -f2)
    
    # Default to 0 if not found
    FAILURES=${FAILURES:-0}
    ERRORS=${ERRORS:-0}
    
    PASS_COUNT=$((TEST_COUNT - FAILURES - ERRORS))
    
    # Update totals
    TOTAL_TESTS=$((TOTAL_TESTS + TEST_COUNT))
    TOTAL_PASS=$((TOTAL_PASS + PASS_COUNT))
    TOTAL_FAIL=$((TOTAL_FAIL + FAILURES))
    TOTAL_ERROR=$((TOTAL_ERROR + ERRORS))
    FILE_COUNT=$((FILE_COUNT + 1))
    
    # Add to summary
    echo "FAIL - $TEST_FILE" >> "$SUMMARY_FILE"
    echo "  Tests: $TEST_COUNT, Passed: $PASS_COUNT, Failed: $FAILURES, Errors: $ERRORS" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"
    
    # Print to console if enabled
    if [ "$CONSOLE_OUTPUT" = true ]; then
      echo ""
      echo "❌ FAIL - Module: $MODULE_NAME (Passed: $PASS_COUNT, Failed: $FAILURES, Errors: $ERRORS)"
      echo ""
    fi
    
    # Extract passing test names (estimation)
    ALL_TESTS=$(grep -o "test_[a-zA-Z0-9_]*" "$TEMP_OUTPUT" | sort | uniq)
    FAILED_TESTS=$(grep -A 1 "FAIL:" "$TEMP_OUTPUT" | grep -o "test_[a-zA-Z0-9_]*" | sort | uniq)
    ERROR_TESTS=$(grep -A 1 "ERROR:" "$TEMP_OUTPUT" | grep -o "test_[a-zA-Z0-9_]*" | sort | uniq)
    
    # Add passing tests to HTML
    for TEST_NAME in $ALL_TESTS; do
      if ! echo "$FAILED_TESTS" | grep -q "$TEST_NAME" && ! echo "$ERROR_TESTS" | grep -q "$TEST_NAME"; then
        cat >> "$HTML_SUMMARY" << EOL
        <tr>
            <td>${MODULE_NAME}</td>
            <td>${TEST_NAME}</td>
            <td class="pass">PASS</td>
            <td>N/A</td>
            <td>Test passed successfully</td>
        </tr>
EOL
      fi
    done
    
    # Print failed tests details to console if enabled
    if [ "$CONSOLE_OUTPUT" = true ]; then
      echo "Failed Tests:"
      echo "-------------"
    fi
    
    # Extract failed test details
    while read -r LINE; do
      if [[ $LINE =~ FAIL:\ (test_[a-zA-Z0-9_]*) ]]; then
        TEST_NAME="${BASH_REMATCH[1]}"
        TEST_DESC=$(echo "$LINE" | sed -E 's/FAIL: test_[a-zA-Z0-9_]* \((.*)\)/\1/' | tr -d '()')
        ERROR_MSG=$(grep -A 5 "FAIL: $TEST_NAME" "$TEMP_OUTPUT" | grep "AssertionError" | head -1 | sed 's/.*AssertionError: //')
        
        # Print to console if enabled
        if [ "$CONSOLE_OUTPUT" = true ]; then
          echo "❌ $TEST_NAME: $ERROR_MSG"
          echo "   Module: $MODULE_NAME"
          echo "   Description: $TEST_DESC"
          echo ""
        fi
        
        # Add to HTML summary
        cat >> "$HTML_SUMMARY" << EOL
        <tr>
            <td>${MODULE_NAME}</td>
            <td>${TEST_NAME}</td>
            <td class="fail">FAIL</td>
            <td>${ERROR_MSG}</td>
            <td>${TEST_DESC}</td>
        </tr>
EOL
      fi
    done < <(grep "FAIL:" "$TEMP_OUTPUT")
    
    # Print error tests details to console if enabled
    if [ "$CONSOLE_OUTPUT" = true ] && [ "$ERRORS" -gt 0 ]; then
      echo "Error Tests:"
      echo "------------"
    fi
    
    # Extract error test details
    while read -r LINE; do
      if [[ $LINE =~ ERROR:\ (test_[a-zA-Z0-9_]*) ]]; then
        TEST_NAME="${BASH_REMATCH[1]}"
        TEST_DESC=$(echo "$LINE" | sed -E 's/ERROR: test_[a-zA-Z0-9_]* \((.*)\)/\1/' | tr -d '()')
        ERROR_MSG=$(grep -A 5 "ERROR: $TEST_NAME" "$TEMP_OUTPUT" | grep "Error" | head -1 | sed 's/.*Error: //')
        
        # Print to console if enabled
        if [ "$CONSOLE_OUTPUT" = true ]; then
          echo "❌ $TEST_NAME: $ERROR_MSG"
          echo "   Module: $MODULE_NAME"
          echo "   Description: $TEST_DESC"
          echo ""
        fi
        
        # Add to HTML summary
        cat >> "$HTML_SUMMARY" << EOL
        <tr>
            <td>${MODULE_NAME}</td>
            <td>${TEST_NAME}</td>
            <td class="fail">ERROR</td>
            <td>${ERROR_MSG}</td>
            <td>${TEST_DESC}</td>
        </tr>
EOL
      fi
    done < <(grep "ERROR:" "$TEMP_OUTPUT")
  else
    echo "ERROR - $TEST_FILE (Couldn't parse results)" >> "$SUMMARY_FILE"
    echo "" >> "$SUMMARY_FILE"
    
    # Print to console if enabled
    if [ "$CONSOLE_OUTPUT" = true ]; then
      echo "❌ ERROR - $TEST_FILE (Couldn't parse results)"
    fi
    
    # Add to HTML summary
    cat >> "$HTML_SUMMARY" << EOL
        <tr>
            <td>${MODULE_NAME}</td>
            <td>Unknown</td>
            <td class="fail">ERROR</td>
            <td>Could not parse test results</td>
            <td>Unknown</td>
        </tr>
EOL
  fi
  
  # Clean up temp file
  rm "$TEMP_OUTPUT"
done

# Add overall summary
echo "" >> "$SUMMARY_FILE"
echo "Overall Summary:" >> "$SUMMARY_FILE"
echo "------------------------------------------------------------" >> "$SUMMARY_FILE"
echo "Total Test Files: $FILE_COUNT" >> "$SUMMARY_FILE"
echo "Total Tests Run: $TOTAL_TESTS" >> "$SUMMARY_FILE"
echo "Total Tests Passed: $TOTAL_PASS" >> "$SUMMARY_FILE"
echo "Total Tests Failed: $TOTAL_FAIL" >> "$SUMMARY_FILE"
echo "Total Errors: $TOTAL_ERROR" >> "$SUMMARY_FILE"
echo "" >> "$SUMMARY_FILE"

OVERALL_STATUS="PASS"
if [ $TOTAL_FAIL -gt 0 ] || [ $TOTAL_ERROR -gt 0 ]; then
  OVERALL_STATUS="FAIL"
fi

echo "OVERALL STATUS: $OVERALL_STATUS" >> "$SUMMARY_FILE"

# Complete HTML summary
cat >> "$HTML_SUMMARY" << EOL
    </table>
    
    <div class="summary">
        <h2>Summary Statistics</h2>
        <p>Total Test Files: $FILE_COUNT</p>
        <p>Total Tests Run: $TOTAL_TESTS</p>
        <p>Total Tests Passed: $TOTAL_PASS</p>
        <p>Total Tests Failed: $TOTAL_FAIL</p>
        <p>Total Errors: $TOTAL_ERROR</p>
        <p>Overall Status: <span class="${OVERALL_STATUS,,}">$OVERALL_STATUS</span></p>
    </div>
</body>
</html>
EOL

# Create marker files
echo "$RESULTS_FILE" > "$LATEST_MARKER"
echo "$SUMMARY_FILE" > "latest_summary.txt"
echo "$HTML_SUMMARY" > "latest_html_summary.txt"

# Show overall summary in console if enabled
if [ "$CONSOLE_OUTPUT" = true ]; then
  echo ""
  echo "================================================================"
  echo "                  FRAMEWORK TEST SUMMARY                        "
  echo "================================================================"
  echo ""
  echo "Total Test Files: $FILE_COUNT"
  echo "Total Tests Run: $TOTAL_TESTS"
  echo "Total Tests Passed: $TOTAL_PASS"
  echo "Total Tests Failed: $TOTAL_FAIL"
  echo "Total Errors: $TOTAL_ERROR"
  echo ""
  echo "OVERALL STATUS: $OVERALL_STATUS"
  echo ""
  
  if [ $TOTAL_FAIL -gt 0 ] || [ $TOTAL_ERROR -gt 0 ]; then
    echo "Issues detected in the following modules:"
    
    # List all failed modules
    if [ $TOTAL_FAIL -gt 0 ]; then
      grep "FAIL -" "$SUMMARY_FILE" | while read -r line; do
        TEST_FILE=$(echo "$line" | cut -d'-' -f2- | xargs)
        BASE_NAME=$(basename "$TEST_FILE")
        MODULE_NAME=${MODULE_NAMES[$BASE_NAME]:-"Unknown Module"}
        echo "  - $MODULE_NAME: $TEST_FILE"
      done
    fi
    
    echo ""
    echo "Fix these issues to ensure core framework functionality is working correctly."
  fi
  echo ""
fi

# Create a notification file that can be displayed separately
cat > "test_results_notification.txt" << EOL
Test results saved to:
- Summary: $SUMMARY_FILE
- HTML Summary: $HTML_SUMMARY
- Detailed results: $RESULTS_FILE

To view the summary:
cat $SUMMARY_FILE

To open the HTML summary in a browser:
open $HTML_SUMMARY
EOL

# Print notification to console
cat test_results_notification.txt

# Make script executable
chmod +x run_tests.sh 