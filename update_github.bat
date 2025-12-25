@echo off
echo ========================================
echo Обновление проекта на GitHub
echo ========================================
echo.

echo [1/4] Проверка статуса Git...
git status
echo.

echo [2/4] Добавление всех изменений...
git add .
echo ✓ Файлы добавлены
echo.

echo [3/4] Создание коммита...
git commit -m "Clean up project: remove temporary files and update documentation"
if errorlevel 1 (
    echo ⚠ Нет изменений для коммита, или ошибка при создании коммита
    echo Продолжаем...
) else (
    echo ✓ Коммит создан
)
echo.

echo [4/4] Загрузка на GitHub...
git push origin main
if errorlevel 1 (
    echo ⚠ Возможно используется master вместо main
    git push origin master
)

echo.
echo ========================================
echo Готово! Проект обновлен на GitHub
echo ========================================
echo.
pause

