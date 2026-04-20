@echo off
cd /d "C:\Users\luan.machado\Desktop\WorkSpace\Banco_Aura"

python HTMLACOMPANHAMENTO.py
if errorlevel 1 (
    echo ERRO: falha ao gerar o dashboard.
    exit /b 1
)

git add HTMLACOMPANHAMENTO.html
git diff --cached --quiet && exit /b 0

git commit -m "Dashboard atualizado automaticamente em %date% %time%"
git push origin main

echo Dashboard publicado com sucesso!
