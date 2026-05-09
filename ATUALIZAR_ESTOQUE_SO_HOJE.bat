@echo off
setlocal EnableExtensions EnableDelayedExpansion

if /I "%~1"=="__RUN__" goto :MAIN
start "Atualizar Estoque Dataloggers - Hoje" cmd /k ""%~f0" __RUN__"
exit /b 0

:MAIN
cd /d "%~dp0"
if errorlevel 1 exit /b 1

set "PAGE_URL=https://luan9753.github.io/banco-aura-dashboard/ESTOQUE_DATALOGGERS.html"
set "INTERVAL_MIN=40"

:LOOP
set "EXIT_CODE=0"
set "HAS_CHANGES=0"
set "HAS_STASH=0"

echo ============================================================
echo  ATUALIZAR ESTOQUE DATALOGGERS - SOMENTE HOJE
echo ============================================================
echo Pasta atual: %CD%
echo Inicio do ciclo: %date% %time%
echo.

echo [1/3] Gerando HTML atualizado...
py ".\gerar_html_estoque.py"
if errorlevel 1 set "ERRMSG=Falha ao gerar o HTML (passo 1)." & goto :FAIL
echo [OK] HTML gerado com sucesso.
echo.

echo [2/3] Preparando commit no Git...
git add ESTOQUE_DATALOGGERS.html gerar_html_estoque.py ATUALIZAR_ESTOQUE_SO_HOJE.bat
if errorlevel 1 set "ERRMSG=Falha no git add (passo 2)." & goto :FAIL

git diff --cached --quiet --exit-code
if errorlevel 1 goto :DO_COMMIT
echo [INFO] Nenhuma alteracao nova para commit.
goto :AFTER_COMMIT

:DO_COMMIT
set "HAS_CHANGES=1"
git commit -m "Atualiza ESTOQUE_DATALOGGERS.html - hoje"
if errorlevel 1 set "ERRMSG=Falha no git commit (passo 2)." & goto :FAIL
echo [OK] Commit criado com sucesso.

:AFTER_COMMIT
echo.

echo [3/3] Enviando para GitHub...
set "ALL_PROXY="
set "HTTP_PROXY="
set "HTTPS_PROXY="
set "GIT_HTTP_PROXY="
set "GIT_HTTPS_PROXY="

if "%HAS_CHANGES%"=="1" goto :DO_PUSH
echo [INFO] Push nao necessario (sem alteracoes novas).
goto :AFTER_PUSH

:DO_PUSH
git status --porcelain | findstr /r "." >nul
if not errorlevel 1 (
    echo [INFO] Salvando alteracoes locais temporariamente...
    git stash push --include-untracked -m "auto-publish ESTOQUE_DATALOGGERS_HOJE" >nul
    if errorlevel 1 set "ERRMSG=Falha ao criar stash local." & goto :FAIL
    set "HAS_STASH=1"
)

echo [INFO] Sincronizando com origin/main antes do push...
git pull --rebase --autostash origin main
if errorlevel 1 set "ERRMSG=Falha ao sincronizar com o remoto (passo 3)." & goto :FAIL

git push origin HEAD:main
if errorlevel 1 set "ERRMSG=Falha no git push (passo 3)." & goto :FAIL
echo [OK] Push concluido com sucesso.

if "%HAS_STASH%"=="1" (
    echo [INFO] Restaurando alteracoes locais...
    git stash pop --index >nul
    if errorlevel 1 echo [WARN] Nao foi possivel restaurar o stash local automaticamente.
)

:AFTER_PUSH
echo.
echo [OK] Ciclo concluido com sucesso.
echo URL publicada: %PAGE_URL%
echo Fim do ciclo: %date% %time%
echo.
echo Proxima atualizacao em %INTERVAL_MIN% minutos. Pressione Ctrl+C para encerrar.
timeout /t %INTERVAL_MIN% /nobreak >nul
goto :LOOP

:FAIL
set "EXIT_CODE=1"
echo.
if "%HAS_STASH%"=="1" (
    echo [INFO] Restaurando alteracoes locais...
    git stash pop --index >nul
    if errorlevel 1 echo [WARN] Nao foi possivel restaurar o stash local automaticamente.
)
echo [ERRO] %ERRMSG%
echo Fim com erro: %date% %time%
echo.
echo Proxima tentativa em %INTERVAL_MIN% minutos. Pressione Ctrl+C para encerrar.
timeout /t %INTERVAL_MIN% /nobreak >nul
goto :LOOP
