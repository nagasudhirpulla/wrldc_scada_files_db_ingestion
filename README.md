## mapping shared network folder to a mapped drive
```bat
net use s: \\hostip\scada\Reports /user:hostusername hostpassworrd /persistent:Yes
```

Using a persistent network drive is not advisable - https://stackoverflow.com/questions/24428597/task-scheduler-access-non-local-drives-while-running-task-not-logged-in

Hence on system log on, run a task as shown below
```bat
@echo off
echo Checking availability of server/computer XXX
%SystemRoot%\system32\ping.exe -n 1 XXX >nul
if errorlevel 1 goto EndBatch
echo Map shared folder XXX\share to drive letter s:
%SystemRoot%\system32\net.exe use s: \\XXX\share /persistent:no
:EndBatch
```

In this example the Reports folder in remote server that contains all the scada reports is mapped as drive named s