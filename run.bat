@echo off

set VENV_FOLDER=venv

REM Check if venv folder exists
if not exist %VENV_FOLDER% (
    echo Creating virtual environment...
    python -m venv %VENV_FOLDER%
)

REM Activate virtual environment (for Windows)
call %VENV_FOLDER%\Scripts\activate

REM Install requirements
pip install -r requirements.txt

REM Run the main.py script
python main.py
