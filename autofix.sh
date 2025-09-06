#!/bin/bash
MAX_COMPLEXITY=30
MAX_LINE_LENGTH=127
SKIP_FILE="pkbrokers/kite/__init__.py"
rm -rf build
rm -rf dist

# Run isort excluding the specific file
isort . --skip $SKIP_FILE

# Run ruff format excluding the specific file  
ruff format . --exclude $SKIP_FILE

# Get flake8 statistics and extract unique error codes
ERROR_CODES=$(flake8 . --count --exit-zero --max-complexity=$MAX_COMPLEXITY --exclude=$SKIP_FILE --max-line-length=$MAX_LINE_LENGTH --statistics | \
               grep -oE '[A-Z][0-9]+' | \
               sort -u)

# Convert error codes to autopep8 format (comma-separated list)
SELECT_CODES=$(echo "$ERROR_CODES" | tr '\n' ',' | sed 's/,$//')

if [ -z "$SELECT_CODES" ]; then
    echo "No flake8 errors found to fix."
    exit 0
fi

echo "Fixing the following flake8 error codes: $SELECT_CODES"

# Run autopep8 on all Python files for the specific error codes, excluding pkbrokers/kite/__init__.py
find . -name "*.py" -not -path "./$SKIP_FILE" -print0 | while IFS= read -r -d '' file; do
    autopep8 --in-place --select="$SELECT_CODES" --aggressive "$file"
done

# Also exclude the file from flake8 statistics if needed
flake8 . --count --exit-zero --max-complexity=$MAX_COMPLEXITY --max-line-length=$MAX_LINE_LENGTH --statistics --exclude=$SKIP_FILE

echo "Autopep8 fixes completed for codes: $SELECT_CODES"
