<#
.SYNOPSIS
   Initiates a SQL Server database restore
.DESCRIPTION
    Performs a database restore actions on a SQL-Server
    Requires the stored procedure mgmt.RestoreDatabaseFromDisk

    - Features:
        * Simple Database Recovery support
        * Uses the youngest / last backup file in a given folder, for restore
        * Overwrites an existing database
        * Move Data and Logfiles do different locations
        * Uses SQLCMD.exe to run trigger actions in SQL-Server
        * Debug mode, wirtes the SQL restore command to the output window

    - Compatibility:
        * SQL Server 2008 R2
        * SQL Server 2014 SP2 / CU4
        * SQL Server 2016    
.NOTES
    Author: Roland Schoen <schoenr@gmx.net>
.PARAMETER RecoveryMode 
	SIMPLE = Restrores a db backup without transaction log files
.PARAMETER DatabaseBackups
	Path to the database backups
.PARAMETER BackupFileMask
	A file extension filter for the backup files
	The default value is *.bak
.PARAMETER RestoreDatabaseServer
	Destination database server, where the restore should start
.PARAMETER RestoreDatabaseName
	The name of the restore databsaes
.PARAMETER MoveFiles
	Enables moving database data and logfiles to an alternate location
	Currently there is only one data file location (for all data files) and
	one log file location supported
.PARAMETER RestoreDataPath
	Destination path for the datafile location
.PARAMETER RestoreLogPath
	Destination path for the transaction log file location
.PARAMETER ManagementDb
    Name of the DB where the restore procedures stored
    Default is [master]    
.PARAMETER WhatIf
	Enables | Disables debug mode								
.EXAMPLE
	Run script in debug mode - No execution will be performed
   .\Restore-DatabaseFromDisk.ps1 -RecoveryMode "simple" -DatabaseBackups "D:\archive\backups\mydb\fullbackups" -BackupFileMask "*.bak" -RestoreDatabaseServer "SERVER01\INSTANCE" -RestoreDatabaseName "my_restore_db" -MoveFiles -RestoreDataPath "C:\volues\sqlserver\data" -RestoreLogPath "C:\volues\sqlserver\log" -WhatIf
.EXAMPLE
	Restore database to original location
   .\Restore-DatabaseFromDisk.ps1 -RecoveryMode "simple" -DatabaseBackups "D:\archive\backups\master_db\fullbackups" -RestoreDatabaseServer "DB01\PROD" -RestoreDatabaseName "master_db"
.EXAMPLE
	Move files to an alternate location, for example if you restore on a test system
	.\Restore-DatabaseFromDisk.ps1 -RecoveryMode "simple" -DatabaseBackups "D:\archive\backups\master_db\fullbackups" -BackupFileMask "*.dbbak" -RestoreDatabaseServer "DEVDB01\INSTANCE" -RestoreDatabaseName "master_db_dev" -MoveFiles -RestoreDataPath "C:\volues\sqlserver\data\master_db_dev" -RestoreLogPath "C:\volues\sqlserver\data\master_db_dev\log"
#>
<# Script Parameter #>
[CmdletBinding(DefaultParameterSetName = "ParamBag-Restore-DatabaseFromDisk")]
param(  [parameter(mandatory=$true, helpmessage="Set the recovery mode for the database restore")][ValidateSet("SIMPLE")][string]$RecoveryMode
	  , [parameter(mandatory=$true, helpmessage="The path for the backup files")][System.IO.DirectoryInfo]$DatabaseBackups
	  , [parameter(mandatory=$false, helpmessage="DB backup filemask like *.bak or *.bck")][string]$BackupFileMask = "*.bak"
	  , [parameter(mandatory=$true, helpmessage="Target database server")][string]$RestoreDatabaseServer
	  , [parameter(mandatory=$true, helpmessage="Target restore database")][string]$RestoreDatabaseName
	  , [parameter(mandatory=$false, helpmessage="Move data and log files switch")][switch]$MoveFiles=$false
	  , [parameter(mandatory=$false, helpmessage="Target restore db data file path")][string]$RestoreDataPath
	  , [parameter(mandatory=$false, helpmessage="Target restore db log file path")][string]$RestoreLogPath
      , [parameter(mandatory=$false, helpmessage="Name of the Database with the restore functions")][string]$ManagementDb="master"      
	  , [parameter(mandatory=$false, helpmessage="Enable/Disable debug mode")][switch]$WhatIf=$false
)

<# Variable section #>
[string]$CrLf = "`r`n`r`n"

[string]$recovery_mode = $RecoveryMode

[System.IO.DirectoryInfo]$db_backup_path = $DatabaseBackups

[string]$db_backup_filemask = $BackupFileMask

[string]$restore_db_host = $RestoreDatabaseServer

[string]$restore_db_name = $RestoreDatabaseName

if ( $MoveFiles ) {
	[string]$move_files = "1"
	[string]$restore_db_data_path = $RestoreDataPath
	[string]$restore_db_log_path = $RestoreLogPath
	
	if ( [string]::IsNullOrEmpty($restore_db_data_path) ) { throw "RestoreDataPath is missing..." }
	if ( [string]::IsNullOrEmpty($restore_db_log_path) ) { throw "RestoreLogPath is missing..." }	
}
else {
	[string]$move_files = "0"
	[string]$restore_db_data_path = "C:\tmp\data"
	[string]$restore_db_log_path = "C:\tmp\log"
}

# Check for debug mode
if ( $WhatIf ) {
	[string]$debug_mode	= "1"
} else {
	[string]$debug_mode	= "0"
}

[string]$mgmt_db = $ManagementDb

# Retrieve the last backup from the datbase backup directory folder
$last_backup = ( Get-ChildItem -Path $db_backup_path.FullName -Filter $db_backup_filemask | Sort-Object -Property "LastWriteTime" -Descending | Select-Object -First 1 )

[System.IO.FileInfo]$backup_file = ( $db_backup_path.FullName + "\" + $last_backup )

# Database restore commands
[string]$mgmt_db_restore_cmd = "`"EXEC mgmt.RestoreDatabaseFromDisk @recoveryMode='$recovery_mode', @backupFile=N'$backup_file', @backupLogs=NULL, @restore_db_name='$restore_db_name', @restore_db_data_path=N'$restore_db_data_path', @restore_db_log_path=N'$restore_db_log_path', @restore_move_files=$move_files, @debug_mode=$debug_mode;`""
[string]$change_db_owner_cmd = "`"EXEC dbo.sp_changedbowner @loginame = N'sa', @map=false;`""
<# Main section #>
CLS

Write-Host " "
Write-Host "### ------------------- Restore Parameter Info ------------------- ###"
Write-Host "-recovery_mode " $recovery_mode
Write-Host "-db_backup_path " $db_backup_path
Write-Host "-db_backup_filemask " $db_backup_filemask
Write-Host "-restore_db_host " $restore_db_host
Write-Host "-restore_db_name " $restore_db_name
if ($move_files -eq "1") {
	Write-Host "-restore_db_data_path " $restore_db_data_path
	Write-Host "-restore_db_log_path " $restore_db_log_path
}
Write-Host "-debug_mode " $debug_mode
if ($debug_mode -eq "1") {
	Write-Host "-mgmt_db " $mgmt_db
	Write-Host "-restore_cmd " $mgmt_db_restore_cmd
}
Write-Host "### ---------------------------------------------------------------- ###"
Write-Host " "

# Check if the backup file is available
if ( -Not ($backup_file.Exists)) {
	$errMsg = "Can't find database backup in: $backup_file$db_backup_filemask" 
	throw $errMsg
}

Write-Host "### Processing database restore..." -ForegroundColor Magenta
Write-Host " "

Write-Host "### Executing restore command (sqlcmd.exe)" -ForegroundColor Yellow
Write-Host " "

# ### Executing sqlcmd.exe
SQLCMD.EXE -S $restore_db_host -d $mgmt_db -Q $mgmt_db_restore_cmd

Write-Host "### Changing database owner to 'sa' ($restore_db_name)" -ForegroundColor Yellow
Write-Host " "
SQLCMD.EXE -S $restore_db_host -d $restore_db_name -Q $change_db_owner_cmd

Write-Host "### Database restore script - Done!" -ForegroundColor DarkGreen
Write-Host " "