REM CD /d %~dp0
REM call C:\ProgramData\Anaconda3\Scripts\activate base
REM call C:\ProgramData\Anaconda3\python.exe daily_file_ingestion_timer_script.py
call project_env\Scripts\activate.bat
call python daily_file_ingestion_timer_script.py