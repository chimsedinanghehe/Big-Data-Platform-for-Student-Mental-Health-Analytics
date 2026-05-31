@echo off
echo ===============================================
echo Student Mental Health Analytics Dashboard
echo ===============================================
echo.

if exist ".venv\Scripts\python.exe" (
    ".venv\Scripts\python.exe" -m streamlit run app.py
) else if exist "..\venv\Scripts\python.exe" (
    "..\venv\Scripts\python.exe" -m streamlit run app.py
) else (
    echo Khong tim thay virtual environment.
    echo Hay tao moi truong bang lenh:
    echo python -m venv .venv
    echo .\.venv\Scripts\activate
    echo pip install -r requirements.txt
)

pause
