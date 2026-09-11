/*
================================================================================
Fix DoNotOrphanTypes Table to Match Actual Schema
================================================================================
The actual Atlas_Prd schema uses ReportObjectTypeID (int) not ObjectType (string).
This script recreates the table with the correct structure.

Run this script on: Atlas_Staging
================================================================================
*/

USE Atlas_Staging;
GO

-- Drop and recreate with correct schema
IF OBJECT_ID('prd_v2.DoNotOrphanTypes', 'U') IS NOT NULL
    DROP TABLE prd_v2.DoNotOrphanTypes;
GO

CREATE TABLE prd_v2.DoNotOrphanTypes (
    ReportObjectTypeID  INT NOT NULL PRIMARY KEY,
    TypeName            NVARCHAR(100) NULL,  -- For reference only
    IsActive            BIT NOT NULL DEFAULT 1,
    Notes               NVARCHAR(500) NULL,
    CreatedDate         DATETIME NOT NULL DEFAULT GETDATE()
);
GO

-- Check if ReportObjectType lookup table exists in Atlas_Prd
-- and insert matching values
IF EXISTS (SELECT 1 FROM Atlas_Prd.INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'ReportObjectType')
BEGIN
    INSERT INTO prd_v2.DoNotOrphanTypes (ReportObjectTypeID, TypeName, Notes)
    SELECT ReportObjectTypeID, Name, 'Auto-populated from Atlas_Prd.dbo.ReportObjectType'
    FROM Atlas_Prd.dbo.ReportObjectType
    WHERE Name IN (
        'SSRS Folder',
        'SSRS Datasource', 
        'SSRS Linked Report',
        'Tableau Folder',
        'SQL View',
        'SQL Stored Procedure'
    );
    
    PRINT 'Populated DoNotOrphanTypes from Atlas_Prd.dbo.ReportObjectType';
END
ELSE
BEGIN
    PRINT 'NOTE: Atlas_Prd.dbo.ReportObjectType table not found.';
    PRINT 'You will need to manually insert ReportObjectTypeID values.';
    PRINT '';
    PRINT 'Example:';
    PRINT 'INSERT INTO prd_v2.DoNotOrphanTypes (ReportObjectTypeID, TypeName) VALUES (1, ''SSRS Folder'');';
END
GO

-- Show what was created
SELECT * FROM prd_v2.DoNotOrphanTypes;
GO
