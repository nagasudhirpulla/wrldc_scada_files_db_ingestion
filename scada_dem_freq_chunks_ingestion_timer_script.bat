REM CD /d %~dp0
REM call C:\ProgramData\Anaconda3\Scripts\activate base
REM call C:\ProgramData\Anaconda3\python.exe scadaChunksIngestionTimerScript.py --chunksFolder \\10.2.100.55\e$\scada_reports\EdnaDemFreqChunks
call project_env\Scripts\activate.bat
python scadaChunksIngestionTimerScript.py --chunksFolder \\10.2.100.55\e$\scada_reports\EdnaDemFreqChunks