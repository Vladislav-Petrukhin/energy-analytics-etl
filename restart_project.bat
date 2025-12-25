@echo off
echo ============================================
echo Полный перезапуск проекта
echo ============================================
echo.

echo [1/4] Остановка всех контейнеров...
docker-compose down
echo.

echo [2/4] Ожидание 5 секунд...
timeout /t 5 /nobreak >nul
echo.

echo [3/4] Запуск контейнеров...
docker-compose up -d
echo.

echo [4/4] Ожидание инициализации (30 секунд)...
timeout /t 30 /nobreak >nul
echo.

echo ============================================
echo Проверка статуса контейнеров:
echo ============================================
docker-compose ps
echo.

echo ============================================
echo Готово!
echo ============================================
echo.
echo Airflow UI будет доступен через минуту:
echo   http://localhost:8081
echo.
echo Логины:
echo   Username: airflow
echo   Password: airflow
echo.
pause

