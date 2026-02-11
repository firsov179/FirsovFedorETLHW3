\connect iot_data

CREATE TABLE IF NOT EXISTS hottest_days (
    date_only DATE PRIMARY KEY,
    temp NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS coldest_days (
    date_only DATE PRIMARY KEY,
    temp NUMERIC(10, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

DROP TRIGGER IF EXISTS update_hottest_days_updated_at ON hottest_days;
CREATE TRIGGER update_hottest_days_updated_at
    BEFORE UPDATE ON hottest_days
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_coldest_days_updated_at ON coldest_days;
CREATE TRIGGER update_coldest_days_updated_at
    BEFORE UPDATE ON coldest_days
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
