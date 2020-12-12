call nssm.exe install nodes_ping_status_api "%cd%\run_server.bat"
call nssm.exe set nodes_ping_status_api AppStdout "%cd%\logs\nodes_ping_status_api.log"
call nssm.exe set nodes_ping_status_api AppStderr "%cd%\logs\nodes_ping_status_api.log"
call sc start nodes_ping_status_api