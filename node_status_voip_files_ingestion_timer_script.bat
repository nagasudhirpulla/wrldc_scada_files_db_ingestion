REM CD /d %~dp0
REM call C:\ProgramData\Anaconda3\Scripts\activate base
REM call C:\ProgramData\Anaconda3\python.exe nodeStatusFilesIngestionTimerScript.py --filesFolder \\10.2.100.89\voip_ping_dumps
call project_env\Scripts\activate.bat
call python nodeStatusFilesIngestionTimerScript.py --filesFolder \\10.2.100.89\voip_ping_dumps