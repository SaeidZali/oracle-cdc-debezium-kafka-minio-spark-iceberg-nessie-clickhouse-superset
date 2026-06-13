-- ============================================
-- ksqlDB CDC Pipeline for Oracle Debezium
-- ============================================
-- Author: Real-Time Platform
-- Description: Process Oracle CDC data from Debezium and create materialized views
-- ============================================

-- ============================================
-- تنظیمات اولیه
-- ============================================
SET 'auto.offset.reset' = 'earliest';
SET 'ksql.streams.cache.max.bytes.buffering' = '10000000';
SET 'ksql.streams.commit.interval.ms' = '1000';
SET 'ksql.streams.auto.offset.reset' = 'earliest';

-- ============================================
-- حذف موارد قبلی (برای اجرای مجدد)
-- ============================================
DROP TABLE IF EXISTS customers_deletes;
DROP TABLE IF EXISTS customers_to_s3;
DROP TABLE IF EXISTS customers_current;
DROP STREAM IF EXISTS customers_clean;
DROP STREAM IF EXISTS customers_cdc_raw;

-- ============================================
-- 1. Stream خام از Debezium
-- ============================================
-- این Stream داده‌های خام را از تاپیک Debezium می‌خواند
CREATE STREAM IF NOT EXISTS customers_cdc_raw (
    before STRUCT<ID BIGINT, NAME STRING>,
    after STRUCT<ID BIGINT, NAME STRING>,
    op STRING,
    ts_ms BIGINT
) WITH (
    KAFKA_TOPIC = 'server1.C__DBZUSER.CUSTOMERS',
    VALUE_FORMAT = 'AVRO',
    TIMESTAMP = 'ts_ms'
);

-- ============================================
-- 2. Stream تمیز
-- ============================================
-- داده‌ها را پاکسازی کرده و مقادیر NULL را با COALESCE مدیریت می‌کند
CREATE STREAM IF NOT EXISTS customers_clean AS
SELECT 
    COALESCE(after->ID, before->ID) AS id,
    COALESCE(after->NAME, before->NAME) AS name,
    op,
    ts_ms
FROM customers_cdc_raw
WHERE COALESCE(after->ID, before->ID) IS NOT NULL
PARTITION BY COALESCE(after->ID, before->ID);

-- ============================================
-- 3. جدول وضعیت فعلی
-- ============================================
-- این جدول آخرین وضعیت هر مشتری را نگهداری می‌کند
CREATE TABLE IF NOT EXISTS customers_current AS
SELECT 
    id,
    LATEST_BY_OFFSET(name) AS name,
    LATEST_BY_OFFSET(op) AS last_op,
    LATEST_BY_OFFSET(ts_ms) AS last_ts
FROM customers_clean
GROUP BY id;

-- ============================================
-- 4. جدول برای S3 (فقط INSERT و UPDATE)
-- ============================================
-- این جدول فقط رکوردهایی که ایجاد یا به‌روزرسانی شده‌اند را شامل می‌شود
CREATE TABLE IF NOT EXISTS customers_to_s3 AS
SELECT 
    id,
    name,
    last_op,
    last_ts
FROM customers_current
WHERE last_op IN ('c', 'u');

-- ============================================
-- 5. جدول برای حذف‌ها (اختیاری)
-- ============================================
-- این جدول رکوردهای حذف شده را برای ممیزی نگهداری می‌کند
