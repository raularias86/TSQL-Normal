

USE DATA_EMPRESAS;
GO
-- Truncate the log by changing the database recovery model to SIMPLE.
ALTER DATABASE DATA_EMPRESAS
SET RECOVERY SIMPLE;
GO
-- Shrink the truncated log file to 1 MB.
DBCC SHRINKFILE (DATA_EMPRESAS_Log, 1);
GO
-- Reset the database recovery model.
ALTER DATABASE DATA_EMPRESAS
SET RECOVERY FULL;
GO