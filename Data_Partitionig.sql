declare @partitionPath varchar(250) = ?;
declare @db varchar(50) = ?;
declare @limitGuid varchar(50) = ?;

declare @financialTransactionXmlSize varchar(10) = ?;
declare @financialTransactionXmlFileGrowth varchar(10) = ?;
declare @financialTransactionXmlMaxSize varchar(10) = ?;

declare @transactionRoutingFileDetailSize varchar(10) = ?;
declare @transactionRoutingFileDetailFileGrowth varchar(10) = ?;
declare @transactionRoutingFileDetailMaxSize varchar(10) = ?;

declare @validationResultSize varchar(10) = ?;
declare @validationResultFileGrowth varchar(10) = ?;
declare @validationResultMaxSize varchar(10) = ?;

declare @transactionRoutingFileTransactionFeedbackSize varchar(10) = ?;
declare @transactionRoutingFileTransactionFeedbackFileGrowth varchar(10) = ?;
declare @transactionRoutingFileTransactionFeedbackMaxSize varchar(10) = ?;

declare @transactionRoutingFileFeedbackValidationResultSize varchar(10) = ?;
declare @transactionRoutingFileFeedbackValidationResultFileGrowth varchar(10) = ?;
declare @transactionRoutingFileFeedbackValidationResultMaxSize varchar(10) = ?;

declare @transactionRoutingFileTransactionFeedbackValidationResultSize varchar(10) = ?;
declare @transactionRoutingFileTransactionFeedbackValidationResultFileGrowth varchar(10) = ?;
declare @transactionRoutingFileTransactionFeedbackValidationResultMaxSize varchar(10) = ?;

declare @transactionRoutingSize varchar(10) = ?;
declare @transactionRoutingFileGrowth varchar(10) = ?;
declare @transactionRoutingMaxSize varchar(10) = ?;

declare @sql nvarchar(max);
declare @partitionName varchar(100);
declare @intLeftBoundary int;
declare @previousDate varchar(8) = Convert(char(6), dateadd(month, -1, getdate()), 112) + '01';
declare @nextDate varchar(8) = Convert(varchar(6), getdate(), 112) + '01';

declare @partitionCounter int;

SELECT @partitionCounter = count(distinct OBJECT_NAME(i.object_id))
FROM sys.partitions p
	JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
	JOIN sys.tables t ON p.object_id = t.object_id
WHERE SCHEMA_NAME(t.schema_id) = 'txp' and OBJECT_NAME(i.object_id) in ('ValidationResult', 'TransactionRoutingFileFeedbackValidationResult', 
	'TransactionRoutingFileTransactionFeedback', 'TransactionRoutingFileTransactionFeedbackValidationResult',
	'FinancialTransactionXml', 'TransactionRoutingFileDetail', 'TransactionRouting')
	and p.partition_number = 2;

if @partitionCounter = 7
begin
	/************ FinancialTransactionXml ************/

	set @partitionName = 'FinancialTransactionXml_' + @nextDate;

	set @sql = 'Alter Database '+@db+' Add Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' add FILE ( NAME = '''+@partitionName+''', FILENAME = '''+@partitionPath+@partitionName+'.ndf'' , SIZE = '+@financialTransactionXmlSize+' , FILEGROWTH = '+@financialTransactionXmlFileGrowth+', MAXSIZE = '+@financialTransactionXmlMaxSize+' ) TO Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION SCHEME PartitionSchemeFinancialTransactionXml NEXT USED '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionFinancialTransactionXml() SPLIT RANGE ('''+@limitGuid+''');';
	EXEC sp_executesql @sql;



	/************ TransactionRouting ************/

	set @partitionName = 'TransactionRouting_' + @nextDate;

	set @sql = 'Alter Database '+@db+' Add Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' add FILE ( NAME = '''+@partitionName+''', FILENAME = '''+@partitionPath+@partitionName+'.ndf'' , SIZE = '+@transactionRoutingSize+' , FILEGROWTH = '+@transactionRoutingFileGrowth+', MAXSIZE = '+@transactionRoutingMaxSize+' ) TO Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION SCHEME PartitionSchemeTransactionRouting NEXT USED '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionTransactionRouting() SPLIT RANGE ('''+@limitGuid+''');';
	EXEC sp_executesql @sql;



	/************ TransactionRoutingFileDetail ************/

	set @partitionName = 'TransactionRoutingFileDetail_' + @nextDate;

	set @sql = 'Alter Database '+@db+' Add Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' add FILE ( NAME = '''+@partitionName+''', FILENAME = '''+@partitionPath+@partitionName+'.ndf'' , SIZE = '+@transactionRoutingFileDetailSize+' , FILEGROWTH = '+@transactionRoutingFileDetailFileGrowth+', MAXSIZE = '+@transactionRoutingFileDetailMaxSize+' ) TO Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION SCHEME PartitionSchemeTransactionRoutingFileDetail NEXT USED '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionTransactionRoutingFileDetail() SPLIT RANGE ('''+@limitGuid+''');';
	EXEC sp_executesql @sql;



	/************ TransactionRoutingFileTransactionFeedback ************/

	set @partitionName = 'TransactionRoutingFileTransactionFeedback_' + @nextDate;

	set @sql = 'Alter Database '+@db+' Add Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' add FILE ( NAME = '''+@partitionName+''', FILENAME = '''+@partitionPath+@partitionName+'.ndf'' , SIZE = '+@transactionRoutingFileTransactionFeedbackSize+' , FILEGROWTH = '+@transactionRoutingFileTransactionFeedbackFileGrowth+', MAXSIZE = '+@transactionRoutingFileTransactionFeedbackMaxSize+' ) TO Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION SCHEME PartitionSchemeTransactionRoutingFileTransactionFeedback NEXT USED '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionTransactionRoutingFileTransactionFeedback() SPLIT RANGE ('''+@limitGuid+''');';
	EXEC sp_executesql @sql;



	/************ TransactionRoutingFileFeedbackValidationResult ************/

	set @partitionName = 'TransactionRoutingFileFeedbackValidationResult_' + @nextDate;

	set @sql = 'Alter Database '+@db+' Add Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' add FILE ( NAME = '''+@partitionName+''', FILENAME = '''+@partitionPath+@partitionName+'.ndf'' , SIZE = '+@transactionRoutingFileFeedbackValidationResultSize+' , FILEGROWTH = '+@transactionRoutingFileFeedbackValidationResultFileGrowth+', MAXSIZE = '+@transactionRoutingFileFeedbackValidationResultMaxSize+' ) TO Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION SCHEME PartitionSchemeTransactionRoutingFileFeedbackValidationResult NEXT USED '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionTransactionRoutingFileFeedbackValidationResult() SPLIT RANGE ('''+@limitGuid+''');';
	EXEC sp_executesql @sql;



	/************ TransactionRoutingFileTransactionFeedbackValidationResult ************/

	set @partitionName = 'TransactionRoutingFileTransactionFeedbackValidationResult_' + @nextDate;

	set @sql = 'Alter Database '+@db+' Add Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'Alter Database '+@db+' add FILE ( NAME = '''+@partitionName+''', FILENAME = '''+@partitionPath+@partitionName+'.ndf'' , SIZE = '+@transactionRoutingFileTransactionFeedbackValidationResultSize+' , FILEGROWTH = '+@transactionRoutingFileTransactionFeedbackValidationResultFileGrowth+', MAXSIZE = '+@transactionRoutingFileTransactionFeedbackValidationResultMaxSize+' ) TO Filegroup '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION SCHEME PartitionSchemeTransactionRoutingFileTransactionFeedbackValidationResult NEXT USED '+@partitionName+';';
	EXEC sp_executesql @sql;

	set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionTransactionRoutingFileTransactionFeedbackValidationResult() SPLIT RANGE ('''+@limitGuid+''');';
	EXEC sp_executesql @sql;



	/************ ValidationResult ************/

	set @sql = 'select @intLeftBoundary = max(FileQueueMetadataId) + 1
	from mifir.FileQueueMetadata where CreatedDate in (
		select max(CreatedDate)
		from mifir.FileQueueMetadata
		where CreatedDate >= '''+@previousDate+''' and CreatedDate < '''+@nextDate +''');'
	EXEC sp_executesql @sql,  N'@intLeftBoundary int OUTPUT', @intLeftBoundary = @intLeftBoundary OUTPUT;

	if @intLeftBoundary  is not null
	begin
		set @partitionName = 'ValidationResult_' + @nextDate;

		set @sql = 'Alter Database '+@db+' Add Filegroup '+@partitionName+';';
		EXEC sp_executesql @sql;

		set @sql = 'Alter Database '+@db+' add FILE ( NAME = '''+@partitionName+''', FILENAME = '''+@partitionPath+@partitionName+'.ndf'' , SIZE = '+@validationResultSize+' , FILEGROWTH = '+@validationResultFileGrowth+', MAXSIZE = '+@validationResultMaxSize+' ) TO Filegroup '+@partitionName+';';
		EXEC sp_executesql @sql;

		set @sql = 'ALTER PARTITION SCHEME PartitionSchemeValidationResult NEXT USED '+@partitionName+';';
		EXEC sp_executesql @sql;

		set @sql = 'ALTER PARTITION FUNCTION PartitionFunctionValidationResult() SPLIT RANGE ('''+convert(varchar,@intLeftBoundary)+''');';
		EXEC sp_executesql @sql;
	end
end
else
begin
	declare @ErrorMessage varchar(100) = 'Before running partition package please create previous partitions';

	RAISERROR(
		@ErrorMessage, -- Message Text
		11, -- Severity
		1 -- State
	);
end
