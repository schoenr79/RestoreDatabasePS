USE [master] --### Otherwise use your favourite database
GO

SET NUMERIC_ROUNDABORT OFF
GO
SET ANSI_PADDING, ANSI_WARNINGS, CONCAT_NULL_YIELDS_NULL, ARITHABORT, QUOTED_IDENTIFIER, ANSI_NULLS ON
GO
SET XACT_ABORT ON
GO
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
GO
BEGIN TRANSACTION
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating schemas'
GO
CREATE SCHEMA [base]
AUTHORIZATION [dbo]
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
CREATE SCHEMA [mgmt]
AUTHORIZATION [dbo]
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [base].[fn_KillProcessesByDb]'
GO


-- =============================================
-- Author:		Roland Schoen <schoenr@gmx.net>
-- Create date: 2015-02-23
-- Change date: 2016-02-28
-- Description: Kills user processes in a specific database
-- Change Log:
-- 2016-02-28 [RS] Added downward compatibility for SQLServer 2008
-- =============================================
CREATE FUNCTION [base].[fn_KillProcessesByDb]
(
	  @restore_db	SYSNAME	-- ## Fully qualified file path
)
RETURNS INT
AS
BEGIN
	/*
	### Kill remaining processes in the Database
	*/
	DECLARE @cmd	NVARCHAR(1024)
		  , @spid	INT
	;

	DECLARE c_proc_killer CURSOR FAST_FORWARD FOR
		SELECT s.spid FROM master..sysprocesses AS s
		WHERE s.dbid = DB_ID(@restore_db) AND s.spid <> @@SPID
	OPEN c_proc_killer
	  FETCH NEXT FROM c_proc_killer INTO @spid
		WHILE @@FETCH_STATUS = 0
		BEGIN
		  SET @cmd = 'KILL ' + CAST(@spid AS NVARCHAR(10));
		  
		  EXEC sys.sp_executeSQL @cmd; 
		  
		  FETCH NEXT FROM c_proc_killer INTO @spid;
		END
	CLOSE c_proc_killer;
	DEALLOCATE c_proc_killer;

	RETURN 1
END
	

GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [base].[fn_GetFileNameByPath]'
GO

-- =============================================
-- Author:		Roland Schoen <schoenr@gmx.net>
-- Create date: 2015-07-21
-- Change date: 2015-07-21
-- Description: Returns the file name in a file path
-- =============================================
CREATE FUNCTION [base].[fn_GetFileNameByPath]
(
	  @file_path	NVARCHAR(4000)	-- ## Fully qualified file path
)
RETURNS NVARCHAR(255)
AS
BEGIN
	DECLARE @FileName NVARCHAR(255);

	-- SELECT statement to gather the file name
	SELECT @FileName = RIGHT(@file_path, CHARINDEX('\', REVERSE(REPLACE(@file_path, '/', '\')))-1);

	-- Return value
	RETURN @FileName;
END
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [base].[fn_FileExists]'
GO
CREATE FUNCTION [base].[fn_FileExists](@FilePath NVARCHAR(512))
RETURNS BIT
AS
BEGIN
     DECLARE @ret_val INT;

     EXEC sys.xp_fileexist @FilePath, @ret_val OUTPUT;

     RETURN CAST(@ret_val AS BIT);
END;
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [base].[FilePath_Check]'
GO
-- =============================================
-- Author:		Roland Schoen <schoenr@gmx.net>
-- Create date: 2015-06-14
-- Change date: 2015-06-14
-- Description: Checks if a file exist an creates sub directories
-- =============================================
CREATE PROCEDURE [base].[FilePath_Check]
(
	-- Add the parameters for the function here
	  @file_path		NVARCHAR(1024)
	, @subdir_create	BIT	= 0
	, @subdir_path		NVARCHAR(1024)
)
WITH EXEC AS CALLER
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @file_exist			INT
		  , @file_is_dir		INT
		  , @parent_dir_exist	INT
		  , @status				INT
		  , @status_file		INT	= 0
		  , @status_subdir		INT	= 0
	;

	DECLARE @tbl_result_fileexist	TABLE (   file_exist		INT	
										    , file_is_dir		INT
											, parent_dir_exist	INT
										  );

	-- file check
	INSERT INTO @tbl_result_fileexist
	(   file_exist
	  , file_is_dir
	  , parent_dir_exist
	)
	EXEC xp_fileexist @file_path

	-- Assign variables
	SELECT @file_exist			= file_exist
		 , @file_is_dir			= file_is_dir
		 , @parent_dir_exist	= parent_dir_exist
	FROM @tbl_result_fileexist;

	IF ( @file_exist = 1 AND @file_is_dir = 0 AND @parent_dir_exist = 1 )
	BEGIN
		-- ### File exists and is accessable
		SET @status_file = 1;
	END
	ELSE
	BEGIN
		SET @status_file = -1;
	END

	-- ### Check if the sub directory exists
	IF NOT ( @subdir_path IS NULL OR @subdir_path = '' ) AND ( @subdir_create = 1 )
	BEGIN
		-- Clean up the table variable
		DELETE FROM @tbl_result_fileexist;

		-- file check
		INSERT INTO @tbl_result_fileexist
		(   file_exist
		  , file_is_dir
		  , parent_dir_exist
		)
		EXEC xp_fileexist @subdir_path;

		-- Assign variables
		SELECT @file_exist			= file_exist
			 , @file_is_dir			= file_is_dir
			 , @parent_dir_exist	= parent_dir_exist
		FROM @tbl_result_fileexist;

		IF ( @file_exist = 0 AND @file_is_dir = 0 AND @parent_dir_exist = 1 )
		BEGIN
			-- ### Creating sub directory,
			SET @status_subdir = 2;
			EXEC sys.xp_create_subdir @subdir_path;
		END
		ELSE IF ( @file_exist = 0 AND @file_is_dir = 1 AND @parent_dir_exist = 1 )
		BEGIN
			SET @status_subdir = 3;
		END
		ELSE
		BEGIN
			SET @status_subdir = -2;
		END

	END

	/*	
		Return status
		-3 = File and parent directory of sub dir path does not exist
		-1 = File exists and parent directory of sub dir path does not exist / can't create sub directory 
		 1 = File exists
		 2 = Subdirectory created
		 3 = File exists and sub directory was created
		 4 = File exists and sub directory already exists
	*/

	SET @status = ( @status_file + @status_subdir );

	RETURN @status;
END
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [base].[fn_GetSqlServerVersion]'
GO
-- =============================================
-- Author:		Roland Schoen <schoenr@gmx.net>
-- Create date: 2016-10-21
-- Description:	Function to retrieve the sql server version properly
-- =============================================
CREATE FUNCTION [base].[fn_GetSqlServerVersion]()
RETURNS @sql_server_version TABLE (
	product_version NVARCHAR(128)
  , common_version AS SUBSTRING(product_version, 1, CHARINDEX('.', product_version) + 1)
  , major AS PARSENAME(CONVERT(VARCHAR(32), product_version), 4)
  , minor AS PARSENAME(CONVERT(VARCHAR(32), product_version), 3)
  , build AS PARSENAME(CONVERT(VARCHAR(32), product_version), 2)
  , revision AS PARSENAME(CONVERT(VARCHAR(32), product_version), 1)
)
AS
BEGIN
	INSERT @sql_server_version ( product_version ) 
		SELECT CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128))
	;
	RETURN;
END
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [base].[BackupSet_RestoreDatabaseCmd_Create]'
GO
-- ================================================================
-- ### Author:      Roland Schoen <schoenr@gmx.net>
-- ### Created:     20.07.2015
-- ### Changed:     05.03.2016
-- ### Description: Creates RESTORE DATABASE commands
-- ### Change log:
-- 2016-03-05 [RS] Added move_files parameter - Decides if data and 
--				   log files should be moved to an alternate file location.
-- 2018-04-11 [RS]: Added compatibility for SQL Server Version 13.0
-- ================================================================
CREATE PROCEDURE [base].[BackupSet_RestoreDatabaseCmd_Create]
(
    @backup_set				NVARCHAR(512)	= NULL
  , @backup_db_name			SYSNAME			= NULL		-- ### Database name from the backup file
  , @restore_db_name		SYSNAME			= NULL		-- ### Database name for the restore
  , @restore_db_data_path	NVARCHAR(1024)	= NULL		-- ### Data file restore path
  , @restore_db_log_path	NVARCHAR(1024)  = NULL		-- ### Log file restore path
  , @move_files				BIT				= 0			-- ### Using @restore_db_data_path & @restore_db_log_path to move database logfiles
  , @debug_mode				BIT				= 0			-- ### Enables | Disables debug output
)
WITH EXECUTE AS CALLER
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @CrLf					NCHAR( 2 )		= NCHAR( 13 ) + NCHAR( 10 )
		  , @Cmd					NVARCHAR(4000)
	      , @ret_val				INT				= 0
		  , @restore_command		NVARCHAR(4000)
		  , @restore_command_part	NVARCHAR(4000)
		  , @common_version			NVARCHAR(128)
		  , @build_version			NVARCHAR(128)
	;

	-- Retrieve server version
	SELECT @common_version = ver.common_version
		 , @build_version = ver.build
	FROM base.fn_GetSqlServerVersion() AS ver
	;

	-- ### Table variable for the database files and the restore command
	CREATE TABLE #tbl_databasefiles (    id						INT IDENTITY(1,1)
									   , LogicalName			NVARCHAR(512)
									   , PhysicalName			NVARCHAR(512)
									   , Type					CHAR(1)
									   , FileGroupName			NVARCHAR(512)
									   , Size					BIGINT
									   , MaxSize				BIGINT
									   , FileId					BIGINT	
									   , CreateLSN				SQL_VARIANT
									   , DropLSN				SQL_VARIANT
									   , UniqueId				UNIQUEIDENTIFIER
									   , ReadOnlyLSN			SQL_VARIANT
									   , ReadWriteLSN			SQL_VARIANT
									   , BackupSizeInBytes		BIGINT
									   , SourceBlockSize		BIGINT
									   , FileGroupId			BIGINT
									   , LogGroupGUID			UNIQUEIDENTIFIER
									   , DifferentialBaseLSN	SQL_VARIANT
									   , DifferentialBaseGUID	UNIQUEIDENTIFIER
									   , IsReadOnly				BIT
									   , IsPresent				BIT
									   , TDEThumbprint			SQL_VARIANT
									   , RestoreCommand			NVARCHAR(4000)
									 )
	;


	-- ### Read database files from backup set and put it into the table variable
	SET @restore_command_part = N'RESTORE DATABASE ' + QUOTENAME(@restore_db_name) + N' FROM DISK = ' + QUOTENAME(@backup_set, CHAR(39)) + N' WITH FILE = 1 ' + @CrLf;

	INSERT INTO #tbl_databasefiles (RestoreCommand) VALUES (@restore_command_part);

	-- ### Move data and logfiles to a another location
	-- ### Currently supports only a single data file one log file location
	IF ( @move_files = 1 ) 
	BEGIN
	-- ### Read database files from backup set and put it into the table variable
	SET @Cmd = 'RESTORE FILELISTONLY FROM DISK = ' + QUOTENAME(@backup_set, CHAR(39)) + ' WITH NOUNLOAD;';

	IF (@common_version = '13.0')
	BEGIN
		ALTER TABLE #tbl_databasefiles
			ADD SnapshotUrl SQL_VARIANT

		INSERT INTO #tbl_databasefiles
		(     LogicalName
			, PhysicalName
			, Type
			, FileGroupName
			, Size
			, MaxSize
			, FileId
			, CreateLSN
			, DropLSN
			, UniqueId
			, ReadOnlyLSN
			, ReadWriteLSN
			, BackupSizeInBytes
			, SourceBlockSize
			, FileGroupId
			, LogGroupGUID
			, DifferentialBaseLSN
			, DifferentialBaseGUID
			, IsReadOnly
			, IsPresent
			, TDEThumbprint
			, SnapshotUrl
		)
		EXEC( @Cmd )
		;
	END
	ELSE
	BEGIN
		INSERT INTO #tbl_databasefiles
		(     LogicalName
			, PhysicalName
			, Type
			, FileGroupName
			, Size
			, MaxSize
			, FileId
			, CreateLSN
			, DropLSN
			, UniqueId
			, ReadOnlyLSN
			, ReadWriteLSN
			, BackupSizeInBytes
			, SourceBlockSize
			, FileGroupId
			, LogGroupGUID
			, DifferentialBaseLSN
			, DifferentialBaseGUID
			, IsReadOnly
			, IsPresent
			, TDEThumbprint
			, SnapshotUrl
		)
		EXEC( @Cmd )
		;
	END




	-- ### Adding MOVE actions to RestoreCommand
	UPDATE dbf SET
		RestoreCommand = N', MOVE N' + QUOTENAME(CAST(dbf.LogicalName AS NVARCHAR(512)), CHAR(39)) 
					   + N' TO N' + QUOTENAME(((SELECT CASE dbf.[Type] WHEN 'L' THEN @restore_db_log_path ELSE @restore_db_data_path END) + '\' + base.fn_GetFileNameByPath(REPLACE(dbf.PhysicalName, @backup_db_name, @restore_db_name))), CHAR(39)) 
					   + ' ' + @CrLf
	FROM #tbl_databasefiles AS dbf

	WHERE dbf.id = id
	  AND NOT dbf.[Type] IS NULL
	;
	END

	-- ### Set last values for the first database restore command
	SET @restore_command_part = + N', NORECOVERY, NOUNLOAD, REPLACE, STATS = 5;';
	
	INSERT INTO #tbl_databasefiles (RestoreCommand) VALUES (@restore_command_part);

	IF ( @debug_mode = 1 )
		BEGIN
		SELECT td.id
			 , td.LogicalName
			 , td.PhysicalName
			 , td.Type
			 , td.FileGroupName
			 , td.FileId
			 , td.UniqueId
			 , td.BackupSizeInBytes
			 , td.FileGroupId
			 , td.IsReadOnly
			 , td.RestoreCommand
		FROM #tbl_databasefiles AS td
		;
	END;
	
	WITH cte_RestoreCmd AS ( 
		SELECT td.id
		     , td.RestoreCommand
		FROM #tbl_databasefiles AS td
		WHERE td.id = 1
		UNION ALL
		SELECT td.id
		     , r_cte.RestoreCommand + td.RestoreCommand
		FROM cte_RestoreCmd AS r_cte
		INNER JOIN #tbl_databasefiles AS td
				ON td.id = r_cte.id + 1
	)
	SELECT @restore_command = RestoreCommand
	FROM cte_RestoreCmd
	WHERE id = (SELECT MAX(id) FROM #tbl_databasefiles)
	;

	-- ### Prepare the database restore commands table
	IF OBJECT_ID('tempdb..##tbl_restore_commands') IS NULL
	BEGIN
		CREATE TABLE ##tbl_restore_commands (   id INT IDENTITY(1,1)
											  , restore_cmd NVARCHAR(4000)
											  , restore_desc NVARCHAR(128)
											  , process_id INT
											  , task_run INT DEFAULT(0)
											)
		;
	END
	
	SET @restore_command_part = N'RESTORE DATABASE ' + QUOTENAME(@restore_db_name) + N' WITH RECOVERY; ';

	INSERT INTO ##tbl_restore_commands ( restore_cmd, restore_desc, process_id ) 
	VALUES ( @restore_command, N'RESTORE_DB_START', @@SPID )
	     , ( @restore_command_part, N'RESTORE_DB_END', @@SPID )
	;

	RETURN @ret_val;
END
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [base].[BackupSet_Check]'
GO
-- =============================================
-- Author:		Roland Schoen <schoenr@gmx.net>
-- Create date: 2015-06-14
-- Change date: 2018-04-11
-- Description: Checks if a file exist an creates sub directories
-- Change Log:
-- [RS] 2016-10-24: Added backwards compatibility for SQL Server 2008 R2, 2014 and 2014 SP2 
-- [RS] 2018-04-11: Added compatibility for SQL Server Version 13.0
-- =============================================
CREATE PROCEDURE [base].[BackupSet_Check]
(
	-- Add the parameters for the function here
	    @backup_set				NVARCHAR(512)
	  , @bs_DbName				SYSNAME			OUT
	  , @bs_DbCompatLevel		INT				OUT
	  , @bs_DbRecoveryModel		VARCHAR(12)		OUT
	  , @bs_BackupSizeMB		BIGINT			OUT
	  , @bs_IsCompressed		BIT				OUT
	  , @bs_BackupCompressedMB	BIGINT			OUT
	  , @bs_LsnFirst			SQL_VARIANT		OUT
	  , @bs_LsnLast				SQL_VARIANT		OUT
	  , @bs_LsnCheckpoint		SQL_VARIANT		OUT
	  , @bs_LsnDbBackup			SQL_VARIANT		OUT
	  , @bs_IsDamaged			SMALLINT		OUT
	  , @bs_IncompleteMetadata	SMALLINT		OUT
)
WITH EXEC AS CALLER
AS
BEGIN
	SET NOCOUNT ON;

	DECLARE   @Status			INT	= 0
			, @common_version	NVARCHAR(128)
			, @build_version	NVARCHAR(128)
			, @Cmd				NVARCHAR(4000) = 'RESTORE HEADERONLY FROM DISK = ' + QUOTENAME(@backup_set, CHAR(39)) + ' WITH NOUNLOAD;'
	;

	-- Retrieve server version
	SELECT @common_version = ver.common_version
		 , @build_version = ver.build
	FROM base.fn_GetSqlServerVersion() AS ver
	;

	-- Tmp table to store backup header informations (base table)
	CREATE TABLE #tbl_backupset (	  backupfile_id			INT			DEFAULT((0))							
									, BackupName			SQL_VARIANT
									, BackupDescr			SQL_VARIANT
									, BackupType			SQL_VARIANT
									, ExpirationDate		SQL_VARIANT
									, compressed			BIT
									, position				SQL_VARIANT
									, deviceType			SQL_VARIANT
									, userName				SQL_VARIANT
									, serverName			SQL_VARIANT
									, dbName				SQL_VARIANT
									, dbVersion				SQL_VARIANT
									, dbCdate				SMALLDATETIME
									, backupSize			BIGINT
									, firstLSN				SQL_VARIANT
									, lastLSN				SQL_VARIANT
									, checkpointLSN			SQL_VARIANT
									, dbBackupLSN			SQL_VARIANT
									, backupStart			SMALLDATETIME
									, backupEnd				SMALLDATETIME
									, sort					SQL_VARIANT
									, cpage					SQL_VARIANT
									, unicodeLid			SQL_VARIANT
									, unicodeCompId			SQL_VARIANT
									, compatlevel			SQL_VARIANT
									, SwVendorId			SQL_VARIANT
									, SwVerMaj				SQL_VARIANT
									, SwVerMin				SQL_VARIANT
									, SwVerBuild			SQL_VARIANT
									, Machinename			SQL_VARIANT
									, Flags					SQL_VARIANT
									, BindingId				SQL_VARIANT
									, RecoveryForkId		SQL_VARIANT
									, CollationID			SQL_VARIANT
									, FamilyGuid			SQL_VARIANT
									, Bulklogged			SQL_VARIANT
									, isSnapshot			SMALLINT
									, isReadonly			SMALLINT
									, isSingleuser			SQL_VARIANT
									, backupChecksum		SQL_VARIANT
									, isDamaged				SMALLINT
									, Beginlogchain			SQL_VARIANT
									, incompleteMetadata	SMALLINT
									, forceOffline			SQL_VARIANT
									, isCopyonly			SMALLINT
									, FirstRecoveryForkID	SQL_VARIANT
									, ForkPointLSN			SQL_VARIANT
									, RecoveryModel			SQL_VARIANT
									, DifferentialBaseLSN	SQL_VARIANT
									, DifferentialBaseGUID	SQL_VARIANT
									, BackupTypeDescr		SQL_VARIANT
									, BackupSetGuid			SQL_VARIANT
									, CompressedBackupSize	BIGINT
								);

	/*
	   ### Backward compatibility Backup header 
	   ### Read restore header from files and put it into the @tbl_backupset table
	*/
	IF (@common_version = '10.5' AND @build_version >= '4000')
	BEGIN
		-- ### Copmatibility level: SQL Server 2008R2 SP2
		INSERT INTO #tbl_backupset (  BackupName, BackupDescr, BackupType, ExpirationDate, compressed, position, deviceType
									, userName	, serverName, dbName, dbVersion	, dbCdate, backupSize, firstLSN, lastLSN, checkpointLSN
									, dbBackupLSN, backupStart, backupEnd, sort, cpage, unicodeLid, unicodeCompId, compatlevel, SwVendorId
									, SwVerMaj, SwVerMin, SwVerBuild, Machinename, Flags, BindingId, RecoveryForkId, CollationID
									, FamilyGuid, Bulklogged, isSnapshot, isReadonly, isSingleuser, backupChecksum, isDamaged
									, Beginlogchain, incompleteMetadata, forceOffline, isCopyonly, FirstRecoveryForkID, ForkPointLSN
									, RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescr, BackupSetGuid
									, CompressedBackupSize
		)
		EXEC( @Cmd );
	END
	ELSE IF (@common_version = '12.0' AND @build_version < '5000')
	BEGIN
		-- ### Copmatibility level: SQL Server 2014
		ALTER TABLE #tbl_backupset 
			ADD Containment SQL_VARIANT NULL;

		INSERT INTO #tbl_backupset (  BackupName, BackupDescr, BackupType, ExpirationDate, compressed, position, deviceType
									, userName	, serverName, dbName, dbVersion	, dbCdate, backupSize, firstLSN, lastLSN, checkpointLSN
									, dbBackupLSN, backupStart, backupEnd, sort, cpage, unicodeLid, unicodeCompId, compatlevel, SwVendorId
									, SwVerMaj, SwVerMin, SwVerBuild, Machinename, Flags, BindingId, RecoveryForkId, CollationID
									, FamilyGuid, Bulklogged, isSnapshot, isReadonly, isSingleuser, backupChecksum, isDamaged
									, Beginlogchain, incompleteMetadata, forceOffline, isCopyonly, FirstRecoveryForkID, ForkPointLSN
									, RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescr, BackupSetGuid
									, CompressedBackupSize, Containment
		)
		EXEC( @Cmd );
	END
	ELSE IF ((@common_version = '12.0' AND @build_version >= '5000') OR @common_version = '13.0')
	BEGIN
		-- ### Copmatibility level: SQL Server 2014 SP2
		ALTER TABLE #tbl_backupset 
			ADD Containment				SQL_VARIANT NULL
			  , KeyAlgorithm			SQL_VARIANT NULL
			  , EncryptorThumbprint 	SQL_VARIANT NULL
			  , EncryptorType			SQL_VARIANT NULL
		;
		
		INSERT INTO #tbl_backupset (  BackupName, BackupDescr, BackupType, ExpirationDate, compressed, position, deviceType
									, userName	, serverName, dbName, dbVersion	, dbCdate, backupSize, firstLSN, lastLSN, checkpointLSN
									, dbBackupLSN, backupStart, backupEnd, sort, cpage, unicodeLid, unicodeCompId, compatlevel, SwVendorId
									, SwVerMaj, SwVerMin, SwVerBuild, Machinename, Flags, BindingId, RecoveryForkId, CollationID
									, FamilyGuid, Bulklogged, isSnapshot, isReadonly, isSingleuser, backupChecksum, isDamaged
									, Beginlogchain, incompleteMetadata, forceOffline, isCopyonly, FirstRecoveryForkID, ForkPointLSN
									, RecoveryModel, DifferentialBaseLSN, DifferentialBaseGUID, BackupTypeDescr, BackupSetGuid
									, CompressedBackupSize, Containment, KeyAlgorithm, EncryptorThumbprint, EncryptorType
		)
		EXEC( @Cmd );
	END

	SELECT @bs_DbName = CONVERT(sysname, tb.dbName)
	     , @bs_DbCompatLevel = CONVERT(INT, tb.compatlevel)
	     , @bs_DbRecoveryModel = CONVERT(VARCHAR(12), tb.RecoveryModel)
	     , @bs_BackupSizeMB = CONVERT(BIGINT, tb.backupSize)
	     , @bs_IsCompressed = CONVERT(BIT, tb.compressed)
	     , @bs_BackupCompressedMB = CONVERT(BIGINT, tb.CompressedBackupSize)
	     , @bs_LsnFirst = tb.firstLSN -- CONVERT(SQL_VARIANT, tb.firstLSN)
	     , @bs_LsnLast = tb.lastLSN -- CONVERT(SQL_VARIANT, tb.lastLSN)
	     , @bs_LsnCheckpoint = tb.checkpointLSN --CONVERT(SQL_VARIANT, tb.checkpointLSN)
	     , @bs_LsnDbBackup = tb.dbBackupLSN --CONVERT(SQL_VARIANT, tb.dbBackupLSN)
	     , @bs_IsDamaged = CONVERT(SMALLINT, tb.isDamaged)
	     , @bs_IncompleteMetadata = CONVERT(SMALLINT, tb.incompleteMetadata)
	FROM #tbl_backupset AS tb;

	-- Return status
	RETURN @Status;
END
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
PRINT N'Creating [mgmt].[RestoreDatabaseFromDisk]'
GO
-- ================================================================
-- ### Author:      Roland Schoen <schoenr@gmx.net>
-- ### Created:     19.02.2016
-- ### Changed:     07.03.2016
-- ### Description: Restore a database backup from Disk
-- ### Change log:
-- 2016-03-05 [RS]	Added @restore_move_files parameter
-- 2016-03-07 [RS]	Put database in single user mode / multi user mode,
--					on start / end if restoring to existing DB
-- ================================================================
CREATE PROCEDURE [mgmt].[RestoreDatabaseFromDisk] (
		@recoveryMode			SQL_VARIANT		= NULL		-- ### 10=Simple
	  , @backupFile				NVARCHAR(4000)	= NULL		-- ### Define the source database name, the db backup contains
	  , @backupLogs				NVARCHAR(MAX)	= NULL		-- ### A comma seperated list of transaction log files
	  , @restore_db_name		SYSNAME			= NULL		-- ### Restore database name
	  , @restore_db_data_path	NVARCHAR(1024)	= NULL		-- ### Restore data file path on the database server
	  , @restore_db_log_path	NVARCHAR(1024)	= NULL		-- ### Restore log file path on the database server
	  , @restore_move_files		BIT				= 0			-- ### Moves data and logfiles to a another location. If disabled @restore_db_data_path and @restore_db_log_path will be ignored
	  , @debug_mode				BIT				= 1			-- ### Enable / Disable debug mode 0=disabled | 1=enabled
)
AS
BEGIN

-- Variables
	SET NOCOUNT ON;

	DECLARE	  @CrLf				NCHAR( 2 )			= NCHAR( 13 ) + NCHAR( 10 )
			, @ErrMsg			VARCHAR( 4000 )		= NULL
			, @ErrServity		SMALLINT			= 10
			, @ErrState			SMALLINT			= 1
			, @InfoMsg			VARCHAR( MAX )		= NULL
			, @RecoveryType		SMALLINT
			, @created_db_data_path INT = 0
			, @created_db_log_path INT  = 0
			, @BackupSet_Exists	INT
			, @RetVal			INT
			, @BackupSet_DbName SYSNAME
			, @BackupSet_DbCompatLevel INT
			, @BackupSet_RecoveryModel VARCHAR(8)
			, @BackupSet_SizeMB BIGINT
			, @BackupSet_Compressed BIT
			, @BackupSet_SizeCompressedMB BIGINT
			, @BackupSet_FirstLSN SQL_VARIANT
			, @BackupSet_LastLSN SQL_VARIANT
			, @BackupSet_CheckpointLSN SQL_VARIANT
			, @BackupSet_DbBackupLSN SQL_VARIANT
			, @BackupSet_IsDamaged SMALLINT
			, @BackupSet_IncompleteMetadata SMALLINT
	;


	SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'START' + REPLICATE( '*', 4 ) + ' --> '
				 + 'DATABASE RESTORE...'
				 + @CrLf +@CrLf;
	PRINT @InfoMsg;

	GOTO pre_checks
	
	pre_checks:
	-- ### Do some housekeeping
	IF ( OBJECT_ID('tempdb..##tbl_restore_commands') IS NOT NULL )
	BEGIN
		IF ( SELECT COUNT(*) AS running_tasks FROM ##tbl_restore_commands WHERE task_run = 1 ) = 0
		BEGIN
			SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'HOUSEKEEPING' + REPLICATE( '*', 4 ) + ' --> '
						 + 'Dropping temp table...'
						 + @CrLf +@CrLf;
			PRINT @InfoMsg;

			DROP TABLE ##tbl_restore_commands;
		END
	END

	/*
	### Recovery Types
	10 = SIMPLE
	20 = FULL
	/// TODO
		20 = FULL with recovery		( Transaction Log's are required )
	*/

	IF @recoveryMode = 10 OR @recoveryMode = 'SIMPLE' 
	BEGIN
		SET @RecoveryType = 10;
		GOTO restore_db_check;
	END
	ELSE
	BEGIN
		-- ### Recovery mode not defined
		SET @ErrMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'ERROR' + REPLICATE( '*', 4 ) + ' --> '
					+ 'RecoveryMode ist not defined'
					+ @CrLf +@CrLf;

		GOTO script_error;
	END

	-- ### Performing database restore pre checks
	restore_log_check:
	-- /// TODO Transaction log recovery

	-- ### Performing database restore pre checks
	restore_db_check:
	SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'INFO' + REPLICATE( '*', 4 ) + ' --> '
				 + 'Performing some pre checks...'
				 + @CrLf +@CrLf;
	PRINT @InfoMsg;

	-- ### Validate if the BackupSet exists
	SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'INFO' + REPLICATE( '*', 4 ) + ' --> '
				 + 'Check if the backup file exists...'
				 + @CrLf;
	PRINT @InfoMsg;

	IF ( (SELECT base.fn_FileExists(@backupFile)) = 1 )
	BEGIN
		SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'INFO' + REPLICATE( '*', 4 ) + ' --> '
					 + 'BackupSet (' + base.fn_GetFileNameByPath(@backupFile) + ') is availabe on Host: ' + CAST(@@SERVERNAME AS VARCHAR)
					 + @CrLf +@CrLf;
		PRINT @InfoMsg;

		-- ### Performing backup set check / reading backup headers
		EXEC base.BackupSet_Check @backup_set = @backupFile
								, @bs_DbName = @BackupSet_DbName OUT
								, @bs_DbCompatLevel = @BackupSet_DbCompatLevel OUT
								, @bs_DbRecoveryModel = @BackupSet_RecoveryModel OUT
								, @bs_BackupSizeMB = @BackupSet_SizeMB OUT
								, @bs_IsCompressed = @BackupSet_Compressed OUT
								, @bs_BackupCompressedMB = @BackupSet_SizeCompressedMB OUT
								, @bs_LsnFirst = @BackupSet_FirstLSN OUT
								, @bs_LsnLast = @BackupSet_LastLSN OUT
								, @bs_LsnCheckpoint = @BackupSet_CheckpointLSN OUT
								, @bs_LsnDbBackup = @BackupSet_DbBackupLSN OUT
								, @bs_IsDamaged = @BackupSet_IsDamaged OUT
								, @bs_IncompleteMetadata = @BackupSet_IncompleteMetadata OUT
		;

		-- ### Go ahead if backup is ok
		IF ( @BackupSet_IsDamaged = 0 OR @BackupSet_IncompleteMetadata = 0 )
		BEGIN
			SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'INFO' + REPLICATE( '*', 4 ) + ' --> '
						 + 'BackupSet (' + base.fn_GetFileNameByPath(@backupFile) + ') looks OK!' + @CrLf + @CrLf
						 + 'Recovery Model = ' + @BackupSet_RecoveryModel + @CrLf
						 + 'First LSN: ' + CAST(@BackupSet_FirstLSN AS VARCHAR(128)) + ', '
						 + 'Last LSN: ' + CAST(@BackupSet_LastLSN AS VARCHAR(128)) + ', '
						 + 'Checkpoint LSN: ' + CAST(@BackupSet_CheckpointLSN AS VARCHAR(128)) + @CrLf
						 + @CrLf;

			PRINT @InfoMsg;
		
			DECLARE @tbl_restore_db TABLE (
				id	INT 
			  , command NVARCHAR(4000)
			);
		
			EXEC base.BackupSet_RestoreDatabaseCmd_Create @backup_set = @backupFile
														, @backup_db_name = @BackupSet_DbName
														, @restore_db_name = @restore_db_name
														, @restore_db_data_path = @restore_db_data_path
														, @restore_db_log_path = @restore_db_log_path
														, @move_files = @restore_move_files
														, @debug_mode = 0
			;

			-- ### Preparing restore comands
			DECLARE @restore_start NVARCHAR(4000)
				  , @restore_end NVARCHAR(4000)
			;

			-- ### Prepare database restore commands

			-- ### Single/Multi user access if the database exists
			IF EXISTS ( SELECT 1 FROM sys.databases AS d WHERE d.name = @restore_db_name )
			BEGIN
				DECLARE @restore_prepare NVARCHAR(4000) = 'ALTER DATABASE ' + QUOTENAME(@restore_db_name) +' SET SINGLE_USER WITH ROLLBACK IMMEDIATE;'
					  , @restore_finish NVARCHAR(4000) = 'ALTER DATABASE ' + QUOTENAME(@restore_db_name) +' SET MULTI_USER WITH ROLLBACK IMMEDIATE;'
				;
			END

			-- ### Start database restore command
			SELECT @restore_start = restore_cmd 
			FROM ##tbl_restore_commands 
			WHERE restore_desc = 'RESTORE_DB_START' AND process_id = @@SPID;

			-- ### End database restore command
			SELECT @restore_end = restore_cmd 
			FROM ##tbl_restore_commands 
			WHERE restore_desc = 'RESTORE_DB_END' AND process_id = @@SPID;

			-- ### Database restore
			IF ( @debug_mode = 1 )
			BEGIN
				SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'DEBUG / COMMAND PATTERN' + REPLICATE( '*', 4 ) + ' --> ' + @CrLf + @CrLf
				
				PRINT @InfoMsg;
				
				PRINT @restore_prepare + @CrLf + @CrLf;
				PRINT @restore_start + @CrLf + @CrLf;
				PRINT @restore_end + @CrLf + @CrLf;
				PRINT @restore_finish + @CrLf + @CrLf;
							
				-- ### Debug mode goes directly to script end..	
				GOTO script_end;
			END
			ELSE
			BEGIN

				-- ### Creating restore directories if necessary
				EXEC @created_db_data_path = base.FilePath_Check @file_path = NULL
															   , @subdir_create = 1
															   , @subdir_path = @restore_db_data_path
				;

				IF ( @created_db_data_path = 1 )
				BEGIN
					SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'EXEC' + REPLICATE( '*', 4 ) + ' --> '
								 + 'Created db data directory: ' + @restore_db_data_path + @CrLf + @CrLf
					PRINT @InfoMsg;	
				END

				EXEC @created_db_log_path  = base.FilePath_Check @file_path = NULL
															   , @subdir_create = 1
															   , @subdir_path = @restore_db_log_path
				;

				IF ( @created_db_log_path = 1 )
				BEGIN
					SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'EXEC' + REPLICATE( '*', 4 ) + ' --> '
								 + 'Created db log directory: ' + @restore_db_log_path 
								 + @CrLf + @CrLf;
					PRINT @InfoMsg;	
				END

				SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'EXEC' + REPLICATE( '*', 4 ) + ' --> '
							 + 'Starting database restore...' 
							 + @CrLf + @CrLf;
				PRINT @InfoMsg;	

				-- ### Set single user mode / Stop / Killing running processes in restore database if the restore database is available
				IF EXISTS ( SELECT 1 FROM sys.databases AS d WHERE d.name = @restore_db_name )
				BEGIN 
					SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'EXEC' + REPLICATE( '*', 4 ) + ' --> '
								 + 'Put database into SINGLE_USER mode...' 
								 + @CrLf + @CrLf;
					PRINT @InfoMsg;
					
					EXEC ( @restore_prepare );

					SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'EXEC' + REPLICATE( '*', 4 ) + ' --> '
								 + 'Stop running processes...' 
								 + @CrLf + @CrLf;
					PRINT @InfoMsg;	

					EXEC base.fn_KillProcessesByDb @restore_db = @restore_db_name;
				END

				-- ### Execute database restore start command
				UPDATE ##tbl_restore_commands SET
					task_run = 1
				WHERE restore_desc = 'RESTORE_DB_START' 
				  AND process_id = @@SPID;

				EXEC ( @restore_start )
			
				UPDATE ##tbl_restore_commands SET
					task_run = 0
				WHERE restore_desc = 'RESTORE_DB_START' 
				  AND process_id = @@SPID;

				PRINT @CrLf;

				-- ### Finish database restore
				SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'EXEC' + REPLICATE( '*', 4 ) + ' --> '
							 + 'Finishing database restore...' 
							 + @CrLf + @CrLf;
				PRINT @InfoMsg;	

				-- ### Execute database restore end command
				UPDATE ##tbl_restore_commands SET
					task_run = 1
				WHERE restore_desc = 'RESTORE_DB_END'
				  AND process_id = @@SPID;

				EXEC ( @restore_end )

				UPDATE ##tbl_restore_commands SET
					task_run = 0
				WHERE restore_desc = 'RESTORE_DB_END'
				  AND process_id = @@SPID;

				IF EXISTS ( SELECT 1 FROM sys.databases AS d WHERE d.name = @restore_db_name )
				BEGIN 
					SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'EXEC' + REPLICATE( '*', 4 ) + ' --> '
								 + 'Put database into MULTI_USER mode...' 
								 + @CrLf + @CrLf;
					PRINT @InfoMsg;
					
					EXEC ( @restore_finish );
				END

				PRINT @CrLf;

				GOTO script_finished;
			END

		END
		ELSE
		BEGIN
			-- ### Backup set damaged stoppoing restore action
			SET @ErrMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'ERROR' + REPLICATE( '*', 4 ) + ' --> '
						+ 'BackupSet (' + @backupFile + ') is damaged, aborting database restore!'
						+ @CrLf +@CrLf;

			GOTO script_error
		END
	END
	ELSE
	BEGIN
		-- ### Database backup is damaged -> script error
		SET @ErrMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'ERROR' + REPLICATE( '*', 4 ) + ' --> '
					+ 'BackupSet (' + @backupFile + ') not found on Host: ' + CAST(@@SERVERNAME AS VARCHAR)
					+ @CrLf +@CrLf;
		GOTO script_error
	END

	GOTO script_end

	-- ### Error Section
	script_error:
	SET @RetVal = 0;

	RAISERROR( @ErrMsg, @ErrServity, @ErrState ) WITH NOWAIT;
	GOTO script_warning

	-- ### Warning Section
	script_warning:
	SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'WARNING' + REPLICATE( '*', 4 ) + ' --> '
				 + 'Script Aborted!'
				 + @CrLf +@CrLf;

	PRINT @InfoMsg;
	GOTO script_end;

	-- ### Finished Section
	script_finished:
	SET @RetVal = 1;
	SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'INFO' + REPLICATE( '*', 4 ) + ' --> '
				 + 'Finished successful!'
				 + @CrLf +@CrLf;

	PRINT @InfoMsg;

	script_end:
	SET @InfoMsg = '[' + CONVERT( NVARCHAR, GETDATE(), 120 ) + '] ' + REPLICATE( '*', 4 ) + 'END' + REPLICATE( '*', 4 ) + ' --> '
				 + 'DATABASE RESTORE'
				 + @CrLf +@CrLf;

	PRINT @InfoMsg;
	
END
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
COMMIT TRANSACTION
GO
IF @@ERROR <> 0 SET NOEXEC ON
GO
-- This statement writes to the SQL Server Log so SQL Monitor can show this deployment.
IF HAS_PERMS_BY_NAME(N'sys.xp_logevent', N'OBJECT', N'EXECUTE') = 1
BEGIN
    DECLARE @databaseName AS nvarchar(2048), @eventMessage AS nvarchar(2048)
    SET @databaseName = REPLACE(REPLACE(DB_NAME(), N'\', N'\\'), N'"', N'\"')
    SET @eventMessage = N'RestoreDatabasePS: { "deployment": { "description": "RestoreDatabasePS deployed to ' + @databaseName + N'", "database": "' + @databaseName + N'" }}'
    EXECUTE sys.xp_logevent 55000, @eventMessage
END
GO
DECLARE @Success AS BIT
SET @Success = 1
SET NOEXEC OFF
IF (@Success = 1) PRINT 'The database update succeeded'
ELSE BEGIN
	IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION
	PRINT 'The database update failed'
END
GO
