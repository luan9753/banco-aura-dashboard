@echo off
setlocal

cd /d "%~dp0"

echo [1/4] Gerando HTML atualizado...
py ".\HTMLACOMPANHAMENTO.py"
if errorlevel 1 (
  echo ERRO: falha ao gerar HTML.
  exit /b 1
)

echo [2/4] Preparando commit...
git add HTMLACOMPANHAMENTO.html HTMLACOMPANHAMENTO.py ATUALIZAR_E_PUBLICAR_HTML.bat
if errorlevel 1 (
  echo ERRO: falha no git add.
  exit /b 1
)

git diff --cached --quiet --exit-code
if %errorlevel% EQU 0 (
  echo Nenhuma alteracao para commit.
) else (
  git commit -m "Atualiza HTMLACOMPANHAMENTO e script de publicacao"
  if errorlevel 1 (
    echo ERRO: falha no git commit.
    exit /b 1
  )
)

echo [3/4] Enviando para GitHub...
set "ALL_PROXY="
set "HTTP_PROXY="
set "HTTPS_PROXY="
set "GIT_HTTP_PROXY="
set "GIT_HTTPS_PROXY="
git push origin main
if errorlevel 1 (
  echo ERRO: falha no git push.
  exit /b 1
)

echo [4/4] Concluido.
echo URL: https://luan9753.github.io/banco-aura-dashboard/HTMLACOMPANHAMENTO.html
exit /b 0

