@echo off
set PATH=%PATH%;C:\Python34
title EMAIL BULK SENDER WITH SMTP AWS [PYTHON]
:runez

python aws_smtp_sender.py
pause
cls
goto runez