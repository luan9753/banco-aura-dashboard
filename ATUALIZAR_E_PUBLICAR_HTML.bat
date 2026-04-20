@echo off
setlocal EnableExtensions

if /I not "%~1"=="__RUN__" (
  start "Atualizar e Publicar HTML Aura" cmd /k ""%~f0" __RUN__"
  exit /b 0
)
shift /1

set "EXIT_CODE=0"
set "HAS_CHANGES=0"
set "PAGE_URL=https://luan9753.github.io/banco-aura-dashboard/HTMLACOMPANHAMENTO.html"

cd /d "%~dp0"
if errorlevel 1 (
  set "ERRMSG=Nao foi possivel acessar a pasta do script."
  goto :FAIL
)

echo ============================================================
echo  ATUALIZAR E PUBLICAR - HTML ACOMPANHAMENTO
echo ============================================================
echo Pasta atual: %CD%
echo Inicio: %date% %time%
echo.

echo [1/4] Gerando HTML atualizado a partir do banco...
py ".\HTMLACOMPANHAMENTO.py"
if errorlevel 1 (
  set "ERRMSG=Falha ao gerar o HTML (passo 1)."
  goto :FAIL
)
echo [OK] HTML gerado com sucesso.
echo.

echo [2/4] Preparando commit no Git...
git add HTMLACOMPANHAMENTO.html HTMLACOMPANHAMENTO.py ATUALIZAR_E_PUBLICAR_HTML.bat
if errorlevel 1 (
  set "ERRMSG=Falha no git add (passo 2)."
  goto :FAIL
)

git diff --cached --quiet --exit-code
if errorlevel 1 (
  set "HAS_CHANGES=1"
  echo [INFO] Alteracoes detectadas. Criando commit...
  git commit -m "Atualiza HTMLACOMPANHAMENTO e script de publicacao"
  if errorlevel 1 (
    set "ERRMSG=Falha no git commit (passo 2)."
    goto :FAIL
  )
  echo [OK] Commit criado com sucesso.
) else (
  echo [INFO] Nenhuma alteracao nova para commit.
)
echo.

echo [3/4] Enviando para GitHub...
set "ALL_PROXY="
set "HTTP_PROXY="
set "HTTPS_PROXY="
set "GIT_HTTP_PROXY="
set "GIT_HTTPS_PROXY="

if "%HAS_CHANGES%"=="1" (
  git push origin main
  if errorlevel 1 (
    set "ERRMSG=Falha no git push (passo 3)."
    goto :FAIL
  )
  echo [OK] Push concluido com sucesso.
) else (
  echo [INFO] Push nao necessario (sem alteracoes novas).
)
echo.

echo [4/4] Processo concluido com sucesso.
echo URL publicada: %PAGE_URL%
echo Fim: %date% %time%
goto :END

:FAIL
set "EXIT_CODE=1"
echo.
echo [ERRO] %ERRMSG%
echo Fim com erro: %date% %time%

:END
echo.
echo Pressione qualquer tecla para fechar...
pause >nul
exit /b %EXIT_CODE%
