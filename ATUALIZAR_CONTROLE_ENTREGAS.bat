@echo off
setlocal EnableExtensions

if /I "%~1"=="__RUN__" goto :MAIN
start "Atualizar Controle Entregas" cmd /k ""%~f0" __RUN__"
exit /b 0

:MAIN

set "EXIT_CODE=0"
set "HAS_CHANGES=0"
set "PAGE_URL=https://luan9753.github.io/banco-aura-dashboard/CONTROLE_ENTREGAS_20D.html"
set "CYCLE=1"

cd /d "%~dp0"
if errorlevel 1 set "ERRMSG=Nao foi possivel acessar a pasta do script." & goto :FAIL

:LOOP
set "EXIT_CODE=0"
set "HAS_CHANGES=0"

echo ============================================================
echo  ATUALIZAR CONTROLE ENTREGAS - PUBLICAR NO GITHUB
echo ============================================================
echo Pasta atual: %CD%
echo Inicio: %date% %time%
echo Ciclo: %CYCLE%
echo.

echo [1/3] Gerando HTML e CSV atualizados...
py ".\gerar_html_controle_entregas.py"
if errorlevel 1 set "ERRMSG=Falha ao gerar o HTML (passo 1)." & goto :FAIL_CYCLE
echo [OK] Arquivos gerados com sucesso.
echo.

echo [2/3] Preparando commit no Git...
git add CONTROLE_ENTREGAS_20D.html CONTROLE_ENTREGAS_20D.csv gerar_html_controle_entregas.py ATUALIZAR_CONTROLE_ENTREGAS.bat
if errorlevel 1 set "ERRMSG=Falha no git add (passo 2)." & goto :FAIL_CYCLE

git diff --cached --quiet --exit-code
if errorlevel 1 goto :DO_COMMIT
echo [INFO] Nenhuma alteracao nova para commit.
goto :AFTER_COMMIT

:DO_COMMIT
set "HAS_CHANGES=1"
for /f "tokens=1-3 delims=/ " %%a in ("%date%") do set "TODAY=%%c-%%b-%%a"
git commit -m "Atualiza CONTROLE_ENTREGAS_20D.html - %TODAY% %time:~0,8%"
if errorlevel 1 set "ERRMSG=Falha no git commit (passo 2)." & goto :FAIL_CYCLE
echo [OK] Commit criado com sucesso.

:AFTER_COMMIT
echo.

echo [3/3] Enviando para GitHub...
set "ALL_PROXY="
set "HTTP_PROXY="
set "HTTPS_PROXY="

if "%HAS_CHANGES%"=="0" (
    echo [INFO] Push nao necessario ^(sem alteracoes novas^).
    goto :AFTER_PUSH
)

git fetch origin
if errorlevel 1 set "ERRMSG=Falha no git fetch antes do push (passo 3)." & goto :FAIL_CYCLE

git rebase --autostash origin/main
if errorlevel 1 set "ERRMSG=Falha no git rebase contra origin/main (passo 3)." & goto :FAIL_CYCLE

git push origin HEAD:main
if errorlevel 1 set "ERRMSG=Falha no git push (passo 3)." & goto :FAIL_CYCLE
echo [OK] Push concluido.

:AFTER_PUSH
echo.
echo ============================================================
echo  CONCLUIDO COM SUCESSO
echo  URL: %PAGE_URL%
echo  Fim: %date% %time%
echo ============================================================
goto :SLEEP

:FAIL_CYCLE
set "EXIT_CODE=1"
echo.
echo [ERRO] %ERRMSG%
echo Fim com erro: %date% %time%

:SLEEP
echo.
echo Proxima atualizacao em 40 minutos. Pressione Ctrl+C para encerrar.
timeout /t 2400 /nobreak >nul
set /a CYCLE+=1
goto :LOOP
