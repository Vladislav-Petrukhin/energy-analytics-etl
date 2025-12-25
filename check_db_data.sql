-- Скрипт для проверки данных в базе

-- Проверка сырых данных
SELECT COUNT(*) as raw_count, MIN(day) as min_date, MAX(day) as max_date 
FROM raw.energy_consumption;

-- Проверка аналитических данных
SELECT 'daily' as table_name, COUNT(*) as count, MIN(day) as min_date, MAX(day) as max_date 
FROM analytics.daily_consumption
UNION ALL
SELECT 'hourly' as table_name, COUNT(*) as count, MIN(DATE(hour)) as min_date, MAX(DATE(hour)) as max_date 
FROM analytics.hourly_consumption
UNION ALL
SELECT 'weekly' as table_name, COUNT(*) as count, MIN(week_start) as min_date, MAX(week_start) as max_date 
FROM analytics.weekly_consumption
UNION ALL
SELECT 'monthly' as table_name, COUNT(*) as count, MIN(month) as min_date, MAX(month) as max_date 
FROM analytics.monthly_consumption
UNION ALL
SELECT 'household_stats' as table_name, COUNT(*) as count, NULL as min_date, NULL as max_date 
FROM analytics.household_stats
UNION ALL
SELECT 'anomalies' as table_name, COUNT(*) as count, MIN(DATE(timestamp)) as min_date, MAX(DATE(timestamp)) as max_date 
FROM analytics.anomalies;

-- Пример данных из daily_consumption
SELECT * FROM analytics.daily_consumption ORDER BY day LIMIT 5;

