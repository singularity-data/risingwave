-- Create root user with all privileges.
USE mysql;
CREATE USER 'root'@'%';
GRANT ALL PRIVILEGES ON *.* TO 'root'@'%' WITH GRANT OPTION;
FLUSH PRIVILEGES;

-- Create test database with table for testing.
CREATE DATABASE test;
CREATE TABLE test.t1 (v1 int, v2 varchar(20));
