Write-Host "=== Démarrage KPI API (port 8000) ===" -ForegroundColor Cyan
$api = Start-Process -NoNewWindow -FilePath "uvicorn" -ArgumentList "api.main:app --host 0.0.0.0 --port 8000 --reload" -PassThru

Write-Host ""
Write-Host "  Dashboard: http://localhost:8000" -ForegroundColor Green
Write-Host ""
Write-Host "Appuyez sur Entrée pour arrêter le serveur..."
Read-Host

Stop-Process -Id $api.Id -Force
Write-Host "Serveur arrêté." -ForegroundColor Yellow
