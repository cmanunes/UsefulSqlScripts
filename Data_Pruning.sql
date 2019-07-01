-- PARAMETERS
declare @db varchar(50) = ?;
declare @financialTransactionXmlPruningRange int = ?;
declare @validationResultPruningRange int = ?;
declare @transactionRoutingPruningRange int = ?;

declare @sql nvarchar(max);
declare @removeDate varchar(8);
declare @partitionName varchar(100);
declare @mergeGuid varchar(50);
declare @partitionCounter int = 0;


/*************** FinancialTransactionXml ***************/

set @sql = 'SELECT @partitionCounter = count(distinct p.partition_number)
FROM sys.partitions p
	JOIN sys.tables t ON p.object_id = t.object_id
	JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
where SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''FinancialTransactionXml'';'
EXEC sp_executesql @sql, N'@partitionCounter int OUTPUT', @partitionCounter = @partitionCounter OUTPUT;

if @partitionCounter > @financialTransactionXmlPruningRange
begin
	set @sql = 'SELECT @mergeGuid = convert(varchar(50), rv.value)
	FROM sys.partitions p
		JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
		JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
		JOIN sys.partition_functions f ON f.function_id = ps.function_id
		LEFT JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
		JOIN sys.tables t ON p.object_id = t.object_id
	where p.partition_number = 1 and SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''FinancialTransactionXml'';';
	EXEC sp_executesql @sql, N'@mergeGuid varchar(50) OUTPUT', @mergeGuid = @mergeGuid OUTPUT;

	set @sql = 'SELECT @partitionName = convert(varchar(100), fg.name)
	FROM sys.partitions p
		JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
		JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
		JOIN sys.partition_functions f ON f.function_id = ps.function_id
		LEFT JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
		JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
		JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
		JOIN sys.tables t ON p.object_id = t.object_id
	where p.partition_number = 2 and SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''FinancialTransactionXml'';';
	EXEC sp_executesql @sql, N'@partitionName varchar(100) OUTPUT', @partitionName = @partitionName OUTPUT;
	
	set @sql = 'TRUNCATE TABLE txp.FinancialTransactionXml WITH (PARTITIONS (2));';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' remove FILE '+@partitionName+';';
	EXEC sp_executesql @sql;
	
	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionFinancialTransactionXml () MERGE RANGE ('''+@mergeGuid+''');';
	EXEC sp_executesql @sql;
	
	set @sql = 'Alter Database '+@db+' remove Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;
end


/*************** ValidationResult ***************/

set @sql = 'SELECT @partitionCounter = count(distinct p.partition_number)
FROM sys.partitions p
    JOIN sys.tables t ON p.object_id = t.object_id
	JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
where SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''ValidationResult'';'
EXEC sp_executesql @sql, N'@partitionCounter int OUTPUT', @partitionCounter = @partitionCounter OUTPUT;

if @partitionCounter > @validationResultPruningRange
begin
	set @sql = 'SELECT @mergeGuid = convert(varchar(50), rv.value)
	FROM sys.partitions p
		JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
		JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
		JOIN sys.partition_functions f ON f.function_id = ps.function_id
		LEFT JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
		JOIN sys.tables t ON p.object_id = t.object_id
	where p.partition_number = 1 and SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''ValidationResult'';';
	EXEC sp_executesql @sql, N'@mergeGuid varchar(50) OUTPUT', @mergeGuid = @mergeGuid OUTPUT;

	set @sql = 'SELECT @partitionName = convert(varchar(100), fg.name)
	FROM sys.partitions p
		JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
		JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
		JOIN sys.partition_functions f ON f.function_id = ps.function_id
		LEFT JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
		JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
		JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
		JOIN sys.tables t ON p.object_id = t.object_id
	where p.partition_number = 2 and SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''ValidationResult'';';
	EXEC sp_executesql @sql, N'@partitionName varchar(100) OUTPUT', @partitionName = @partitionName OUTPUT;
	
	set @sql = 'ALTER TABLE [txp].[ValidationResult] DROP CONSTRAINT [PK_ValidationResult];';
	EXEC sp_executesql @sql;

	set @sql = 'TRUNCATE TABLE txp.ValidationResult WITH (PARTITIONS (2));';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER TABLE [txp].[ValidationResult] 
	WITH CHECK ADD CONSTRAINT [PK_ValidationResult] 
	PRIMARY KEY NONCLUSTERED (ValidationResultId) on [PRIMARY];';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' remove FILE '+@partitionName+';';
	EXEC sp_executesql @sql;
	
	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionValidationResult () MERGE RANGE ('''+@mergeGuid+''');';
	EXEC sp_executesql @sql;
	
	set @sql = 'Alter Database '+@db+' remove Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;
end

/*************** TransactionRouting ***************/

set @sql = 'SELECT @partitionCounter = count(distinct p.partition_number)
FROM sys.partitions p
	JOIN sys.tables t ON p.object_id = t.object_id
	JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
where SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''TransactionRouting'';'
EXEC sp_executesql @sql, N'@partitionCounter int OUTPUT', @partitionCounter = @partitionCounter OUTPUT;

if @partitionCounter > @transactionRoutingPruningRange
begin
	set @sql = 'SELECT @mergeGuid = convert(varchar(50), rv.value)
	FROM sys.partitions p
		JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
		JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
		JOIN sys.partition_functions f ON f.function_id = ps.function_id
		LEFT JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
		JOIN sys.tables t ON p.object_id = t.object_id
	where p.partition_number = 1 and SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''TransactionRouting'';';
	EXEC sp_executesql @sql, N'@mergeGuid varchar(50) OUTPUT', @mergeGuid = @mergeGuid OUTPUT;

	set @sql = 'SELECT @partitionName = convert(varchar(100), fg.name)
	FROM sys.partitions p
		JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
		JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
		JOIN sys.partition_functions f ON f.function_id = ps.function_id
		LEFT JOIN sys.partition_range_values rv ON f.function_id = rv.function_id AND p.partition_number = rv.boundary_id
		JOIN sys.destination_data_spaces dds ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
		JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
		JOIN sys.tables t ON p.object_id = t.object_id
	where p.partition_number = 2 and SCHEMA_NAME(t.schema_id) = ''txp'' and OBJECT_NAME(i.object_id) = ''TransactionRouting'';';
	EXEC sp_executesql @sql, N'@partitionName varchar(100) OUTPUT', @partitionName = @partitionName OUTPUT;
	
	set @sql = 'TRUNCATE TABLE txp.TransactionRouting WITH (PARTITIONS (2));';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' remove FILE '+@partitionName+';';
	EXEC sp_executesql @sql;
	
	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionTransactionRouting () MERGE RANGE ('''+@mergeGuid+''');';
	EXEC sp_executesql @sql;
	
	set @sql = 'Alter Database '+@db+' remove Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;
end

