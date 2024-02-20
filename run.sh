#!/bin/bash

VENV_FOLDER=venv

# Check if venv folder exists
if [ ! -d "$VENV_FOLDER" ]; then
    echo "Creating virtual environment..."
    python -m venv $VENV_FOLDER
fi

# Activate virtual environment (for Linux/macOS)
source $VENV_FOLDER/bin/activate

# Install requirements
pip install -r requirements.txt

# Run the main.py script
python main.py

# Deactivate virtual environment
deactivate
