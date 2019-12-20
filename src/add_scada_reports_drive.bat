@echo off
echo Run this batch on system user log on
echo Checking availability of server/computer XXX
%SystemRoot%\system32\ping.exe -n 1 10.x.xxx.xx >nul
if errorlevel 1 goto EndBatch
echo Map shared folder 10.x.xxx.xx\scada\Reports to drive letter s:
%SystemRoot%\system32\net.exe use s: \\10.x.xxx.xx\scada\Reports /user:uname password /persistent:no
:EndBatch