BEGIN TRY
	BEGIN TRANSACTION;

	update [txp].[DerrivateInstrument]
	set [ExpiryDate] = DATEADD(day, 1, [ExpiryDate])
	where 
		DATEPART(TZoffset, DATEADD(MINUTE, DATEPART(TZoffset, SYSDATETIMEOFFSET()), convert(datetime,[ExpiryDate])) AT TIME ZONE 'GMT Standard Time') = 60;

	COMMIT;
	PRINT 'Data update for [ExpiryDate] column in table [txp].[DerrivateInstrument] finished successfully';
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK;
		PRINT 'Data update for [ExpiryDate] column in table [txp].[DerrivateInstrument] did not finished successfully';
	END;
END CATCH


BEGIN TRY
	BEGIN TRANSACTION;

	update [txp].[PersonIdentification]
	set [BirthDate] = DATEADD(day, 1, [BirthDate])
	where 
		DATEPART(TZoffset, DATEADD(MINUTE, DATEPART(TZoffset, SYSDATETIMEOFFSET()), convert(datetime,[BirthDate])) AT TIME ZONE 'GMT Standard Time') = 60
	and [BirthDate] >= '1753-01-01';

	COMMIT;
	PRINT 'Data update for [BirthDate] column in table [txp].[PersonIdentification] finished successfully';
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK;
		PRINT 'Data update for [BirthDate] column in table [txp].[PersonIdentification] did not finished successfully';
	END;
END CATCH


BEGIN TRY
	BEGIN TRANSACTION;

	update [txp].[DebtInstrument]
	set [MaturityDate] = DATEADD(day, 1, [MaturityDate])
	where 
		DATEPART(TZoffset, DATEADD(MINUTE, DATEPART(TZoffset, SYSDATETIMEOFFSET()), convert(datetime,[MaturityDate])) AT TIME ZONE 'GMT Standard Time') = 60;

	COMMIT;
	PRINT 'Data update for [MaturityDate] column in table [txp].[DebtInstrument] finished successfully';
END TRY
BEGIN CATCH
	IF @@TRANCOUNT > 0
	BEGIN
		ROLLBACK;
		PRINT 'Data update for [MaturityDate] column in table [txp].[DebtInstrument] did not finished successfully';
	END;
END CATCH
