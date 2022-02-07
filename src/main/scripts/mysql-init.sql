DROP DATABASE IF EXISTS spring_batch;
DROP USER IF EXISTS `batch_user`@`%`;
CREATE DATABASE IF NOT EXISTS spring_batch CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS `batch_user`@`%` IDENTIFIED WITH mysql_native_password BY 'password';
GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, REFERENCES, INDEX, ALTER, EXECUTE, CREATE VIEW, SHOW VIEW,
CREATE ROUTINE, ALTER ROUTINE, EVENT, TRIGGER ON `spring_batch`.* TO `batch_user`@`%`;
FLUSH PRIVILEGES;