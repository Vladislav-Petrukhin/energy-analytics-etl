# 📊 Инструкция по загрузке датасета

## Важно: CSV файлы не включены в Git

Большие CSV файлы (9.6 GB) исключены из репозитория через `.gitignore` для экономии места.

---

## 🎯 Рекомендуемый датасет: Smart Meters in London

### Вариант 1: Kaggle (РЕКОМЕНДУЕТСЯ)

**Датасет:** [Smart meters in London](https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london)

**Размер:** ~9.6 GB (336 CSV файлов, ~150+ миллионов строк)

**Шаги:**

1. Зарегистрируйтесь на [Kaggle](https://www.kaggle.com/)
2. Скачайте датасет: https://www.kaggle.com/datasets/jeanmidev/smart-meters-in-london
3. Распакуйте архив
4. **СКОПИРУЙТЕ ПАПКИ** с данными в директорию `data/raw/` (см. варианты ниже)

**Структура в архиве:**

В архиве находится несколько папок. Обычно каждая папка содержит вложенную структуру с CSV файлами:

```
archive-2/
  ├── halfhourly_dataset/
  │   └── halfhourly_dataset/
  │       └── block_*.csv  (block_0.csv до block_111.csv = 112 файлов)
  ├── daily_dataset/
  │   └── daily_dataset/
  │       └── block_*.csv  (block_0.csv до block_111.csv = 112 файлов)
  └── hhblock_dataset/
      └── hhblock_dataset/
          └── block_*.csv  (block_0.csv до block_111.csv = 112 файлов)
```

**Для полного датасета (336 файлов) нужно скопировать эти три папки в `data/raw/`.**

**Важно:** 
- В архиве могут быть и другие папки (например, с метаданными или документацией), которые **не нужны**
- **Для полного датасета (336 файлов) требуется папка `halfhourly_dataset`** (112 файлов) + еще **две другие папки** с `block_*.csv` файлами (по 112 файлов каждая)
- Для минимального тестирования можно использовать только папку `halfhourly_dataset` (112 файлов), но рекомендуется использовать все 336 файлов

---

## ✅ ПРОСТОЙ СПОСОБ: Просто скопируйте папки (РЕКОМЕНДУЕТСЯ)

**Проект теперь поддерживает работу с подпапками!** Вам не нужно объединять файлы в одну папку.

**Просто скопируйте нужные папки из архива в `data/raw/`:**

**Windows PowerShell:**
```powershell
# Путь к распакованному архиву (замените на свой)
$archivePath = "C:\Users\Владислав\Desktop\123"

# Перейдите в папку проекта
cd C:\Users\Владислав\Desktop\TOBD_project\data\raw

# Скопируйте папки (они могут иметь вложенную структуру, это нормально)
Copy-Item "$archivePath\halfhourly_dataset" -Destination "." -Recurse
Copy-Item "$archivePath\daily_dataset" -Destination "." -Recurse
Copy-Item "$archivePath\hhblock_dataset" -Destination "." -Recurse

# Или используйте любые другие папки с block_*.csv файлами
```

**Результат:**
```
data/raw/
  ├── halfhourly_dataset/
  │   └── halfhourly_dataset/
  │       └── block_*.csv (112 файлов)
  ├── daily_dataset/
  │   └── daily_dataset/
  │       └── block_*.csv (112 файлов)
  └── hhblock_dataset/
      └── hhblock_dataset/
          └── block_*.csv (112 файлов)
```

**ETL процесс автоматически найдет все CSV файлы во всех подпапках!**

---

## 📦 Альтернативный способ: Объединение файлов в одну папку

Если вы хотите объединить все файлы в одну папку (старый способ):

**Вариант A: PowerShell скрипт (Windows) - РЕКОМЕНДУЕТСЯ**

Создайте скрипт `merge_blocks.ps1` в корне проекта:
```powershell
# Путь к распакованному архиву (замените на свой путь)
$archivePath = "C:\Users\Владислав\Desktop\123"

# Путь к целевой папке
$targetPath = "C:\Users\Владислав\Desktop\TOBD_project\data\raw"

# Убедитесь, что целевая папка существует
if (-not (Test-Path $targetPath)) {
    New-Item -ItemType Directory -Path $targetPath -Force
}

# Находим все папки с block_*.csv файлами (включая halfhourly_dataset)
$folders = Get-ChildItem -Path $archivePath -Directory | Where-Object {
    (Get-ChildItem -Path $_.FullName -Filter "block_*.csv" -ErrorAction SilentlyContinue).Count -gt 0
}

Write-Host "Найдено папок с CSV файлами: $($folders.Count)"
foreach ($folder in $folders) {
    $fileCount = (Get-ChildItem -Path $folder.FullName -Filter "block_*.csv").Count
    Write-Host "  - $($folder.Name): $fileCount файлов"
}

# Счетчик для уникальных имен
$counter = 0

foreach ($folder in $folders) {
    $files = Get-ChildItem -Path $folder.FullName -Filter "block_*.csv" | Sort-Object Name
    foreach ($file in $files) {
        $newName = "block_$counter.csv"
        Copy-Item -Path $file.FullName -Destination "$targetPath\$newName"
        Write-Host "Скопирован: $($folder.Name)\$($file.Name) -> $newName"
        $counter++
    }
}

Write-Host "`nВсего обработано файлов: $counter"
Write-Host "Файлы находятся в: $targetPath"
```

Запустите скрипт:
```powershell
.\merge_blocks.ps1
```

**Вариант B: Ручное переименование через PowerShell (с диагностикой)**

Выполните в PowerShell (замените пути на свои):
```powershell
# Путь к распакованному архиву
$archivePath = "C:\Users\Владислав\Desktop\123"

# Перейдите в целевую папку проекта
$targetPath = "C:\Users\Владислав\Desktop\TOBD_project\data\raw"
cd $targetPath

Write-Host "Начало обработки файлов..." -ForegroundColor Green
Write-Host "Путь к архиву: $archivePath" -ForegroundColor Yellow
Write-Host "Целевая папка: $targetPath" -ForegroundColor Yellow

# Проверка существования папок
$folders = @("halfhourly_dataset", "daily_dataset", "hhblock_dataset")
foreach ($folderName in $folders) {
    $fullPath = Join-Path $archivePath $folderName
    if (Test-Path $fullPath) {
        $fileCount = (Get-ChildItem -Path $fullPath -Filter "block_*.csv" -ErrorAction SilentlyContinue).Count
        Write-Host "  ✓ Папка '$folderName' найдена: $fileCount файлов" -ForegroundColor Green
    } else {
        Write-Host "  ✗ Папка '$folderName' НЕ найдена по пути: $fullPath" -ForegroundColor Red
    }
}

Write-Host "`nКопирование файлов..." -ForegroundColor Green

# Папка 1: halfhourly_dataset (block_0.csv до block_111.csv)
Write-Host "`nПапка 1: halfhourly_dataset" -ForegroundColor Cyan
$folder1 = Join-Path $archivePath "halfhourly_dataset"
if (Test-Path $folder1) {
    $files1 = Get-ChildItem "$folder1\block_*.csv" -ErrorAction SilentlyContinue
    Write-Host "  Найдено файлов: $($files1.Count)"
    Copy-Item "$folder1\block_*.csv" -Destination "." -Verbose
    Write-Host "  ✓ Скопировано файлов из halfhourly_dataset"
} else {
    Write-Host "  ✗ Папка не найдена!" -ForegroundColor Red
}

# Папка 2: daily_dataset (block_0.csv до block_111.csv)
# Станут block_112.csv до block_223.csv
Write-Host "`nПапка 2: daily_dataset" -ForegroundColor Cyan
$counter = 112
$folder2 = Join-Path $archivePath "daily_dataset"
if (Test-Path $folder2) {
    $files2 = Get-ChildItem "$folder2\block_*.csv" -ErrorAction SilentlyContinue | Sort-Object Name
    Write-Host "  Найдено файлов: $($files2.Count)"
    foreach ($file in $files2) {
        $newName = "block_$counter.csv"
        Copy-Item $file.FullName -Destination ".\$newName" -Verbose
        $counter++
    }
    Write-Host "  ✓ Скопировано файлов: $($counter - 112)"
} else {
    Write-Host "  ✗ Папка не найдена!" -ForegroundColor Red
}

# Папка 3: hhblock_dataset (block_0.csv до block_111.csv)
# Станут block_224.csv до block_335.csv
Write-Host "`nПапка 3: hhblock_dataset" -ForegroundColor Cyan
$startCounter = $counter
$folder3 = Join-Path $archivePath "hhblock_dataset"
if (Test-Path $folder3) {
    $files3 = Get-ChildItem "$folder3\block_*.csv" -ErrorAction SilentlyContinue | Sort-Object Name
    Write-Host "  Найдено файлов: $($files3.Count)"
    foreach ($file in $files3) {
        $newName = "block_$counter.csv"
        Copy-Item $file.FullName -Destination ".\$newName" -Verbose
        $counter++
    }
    Write-Host "  ✓ Скопировано файлов: $($counter - $startCounter)"
} else {
    Write-Host "  ✗ Папка не найдена!" -ForegroundColor Red
}

# Итоговая проверка
$totalFiles = (Get-ChildItem "$targetPath\block_*.csv" -ErrorAction SilentlyContinue).Count
Write-Host "`n" -NoNewline
Write-Host "═══════════════════════════════════════" -ForegroundColor Green
Write-Host "Готово! Всего файлов в $targetPath : $totalFiles" -ForegroundColor Green
Write-Host "═══════════════════════════════════════" -ForegroundColor Green
```

**Примечание:** Замените `другая_папка_1` и `другая_папка_2` на реальные названия папок в вашем архиве. Чтобы узнать названия всех папок с CSV файлами, выполните:
```powershell
Get-ChildItem "C:\path\to\extracted\archive-2" -Directory | Where-Object {
    (Get-ChildItem -Path $_.FullName -Filter "block_*.csv" -ErrorAction SilentlyContinue).Count -gt 0
} | Select-Object Name
```

**Вариант C: Linux/Mac/WSL**

```bash
# Путь к распакованному архиву
ARCHIVE_PATH="/path/to/extracted/archive-2"

# Перейдите в директорию проекта
cd /mnt/c/Users/Владислав/Desktop/TOBD_project/data/raw

# Папка 1: halfhourly_dataset (block_0.csv до block_111.csv)
cp "$ARCHIVE_PATH/halfhourly_dataset/block_"*.csv .

# Папка 2: вторая папка с CSV (переименование: +112)
counter=112
for file in "$ARCHIVE_PATH/другая_папка_1/block_"*.csv; do
    cp "$file" "block_$counter.csv"
    counter=$((counter+1))
done

# Папка 3: третья папка с CSV (переименование: +224)
counter=224
for file in "$ARCHIVE_PATH/другая_папка_2/block_"*.csv; do
    cp "$file" "block_$counter.csv"
    counter=$((counter+1))
done

echo "Готово! Всего файлов: $counter"
```

**Примечание:** Замените `другая_папка_1` и `другая_папка_2` на реальные названия папок. Чтобы найти все папки с CSV:
```bash
find /path/to/extracted/archive-2 -type d -exec sh -c 'test -n "$(find "$1" -maxdepth 1 -name "block_*.csv" 2>/dev/null)" && echo "$1"' _ {} \;
```

**Результат:** Все 336 файлов (3 папки × 112 файлов) будут в `data/raw/` с именами `block_0.csv` до `block_335.csv`

**Формат данных:**
- Колонки: `LCLid`, `tstp`, `energy(kWh/hh)`
- Период: 2011-2014
- Домохозяйства: 5,567
- Записей: ~150+ миллионов

---

### Вариант 2: Тестовый датасет (для быстрой проверки)

Если нужен небольшой датасет для тестирования, создайте файл `sample_energy_data.csv`:

```csv
LCLid,day,energy_kwh,timestamp
MAC000002,2024-01-01,0.125,2024-01-01 00:00:00
MAC000002,2024-01-01,0.089,2024-01-01 01:00:00
MAC000003,2024-01-01,0.098,2024-01-01 00:00:00
...
```

**Минимум:** 50-100 строк для проверки работы системы.

---

## ✅ Проверка наличия данных

После загрузки датасета проверьте:

**Windows PowerShell:**
```powershell
# Количество CSV файлов (рекурсивный поиск в подпапках)
(Get-ChildItem data/raw -Recurse -Filter "block_*.csv").Count
# Должно быть: 336 файлов для полного датасета

# Размер данных
"{0:N2} GB" -f ((Get-ChildItem data/raw -Recurse -Filter "block_*.csv" | Measure-Object -Property Length -Sum).Sum / 1GB)
# Должно быть: ~9.6 GB для полного датасета

# Показать структуру папок
Get-ChildItem data/raw -Directory | ForEach-Object {
    $count = (Get-ChildItem $_.FullName -Recurse -Filter "block_*.csv").Count
    Write-Host "$($_.Name): $count файлов"
}
```

**Linux/Mac/WSL:**
```bash
# Количество CSV файлов (рекурсивный поиск)
find data/raw -name "block_*.csv" | wc -l
# Должно быть: 336 файлов для полного датасета

# Размер данных
du -sh data/raw/
# Должно быть: ~9.6 GB для полного датасета

# Показать структуру папок
find data/raw -type d -maxdepth 2 | while read dir; do
    count=$(find "$dir" -name "block_*.csv" 2>/dev/null | wc -l)
    if [ $count -gt 0 ]; then
        echo "$(basename $dir): $count файлов"
    fi
done
```

---

## 🚀 После загрузки данных

1. Запустите Docker контейнеры:
   ```bash
   docker-compose up -d
   ```

2. Откройте Airflow: http://localhost:8081

3. Запустите DAG `energy_analytics_etl`

4. **Время обработки:**
   - Тестовый датасет (100 строк): ~20 секунд
   - Полный датасет (150+ млн строк): **30-60 минут**

---

## 📝 Примечания

- **CSV файлы не коммитятся в Git** - это правильно, они слишком большие
- **DAG автоматически адаптируется** под формат Kaggle датасета
- **Для защиты проекта** рекомендуется использовать полный датасет для демонстрации Big Data возможностей

---

**Вопросы?** Смотрите `README.md` в корне проекта.

