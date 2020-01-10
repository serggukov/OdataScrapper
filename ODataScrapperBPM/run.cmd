@echo off
echo Creating virtual environment for project
call venv\Scripts\Activate.bat >> logenv.txt
pip install -r requirements.txt > logenv.txt
echo Ok. Running script...
python main.py
pause