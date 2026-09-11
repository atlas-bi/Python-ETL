/*******************************************************************************
 * Atlas ETL Migration — Week 1: usp_Atlas_LDAP (Pipeline B)
 * 
 * Migrated from: ETL-LDAP SSIS Package
 * Purpose: Extract user and group membership data from Azure AD / Clarity
 *
 * ┌─────────────────────────────────────────────────────────────────────────┐
 * │  PIPELINE B AMENDMENT                                                   │
 * │  Step 1 uses LEFT OUTER JOINs matching SSIS ETL-LDAP.dtsx exactly.    │
 * │  All Azure AD users kept; EpicId resolved via EMPtoAzureMap →         │
 * │  raw_v2.CLARITY_EMP.SYSTEM_LOGIN. NO LINKED SERVER REQUIRED.          │
 * └─────────────────────────────────────────────────────────────────────────┘
 *
 * Dependencies:
 *   - raw_v2.CLARITY_EMP populated by atlas_clarity_extractor.py
 *   - raw_v2.ClarityUserGroups populated by atlas_clarity_extractor.py
 *   - dbo.AzureADUsers, dbo.AzureADGroups, dbo.AzureUserGroupMap,
 *     dbo.EMPtoAzureMap (local Atlas_Staging tables)
 *   - stage_v2 schema (created in Week 1)
 *   - Logging: etl.usp_Atlas_LogStart, etl.usp_Atlas_LogEnd (Week 1)
 *
 * Execution:
 *   EXEC etl.usp_Atlas_LDAP;
 *   EXEC etl.usp_Atlas_LDAP @RaiseErrorOnFail = 0;
 *
 * Author:  Larry Duren
 * Date:    March 2026
 * Version: 4.0 (uses raw_v2 in place of linked server queries)
 * Changes:
 *   v4.0 - Reordered Steps 4-7: Clarity users/groups/memberships now populate
 *          stage_v2 BEFORE the prd_v2 user merge. Fixes bug where 121K Clarity
 *          SYSTEM_LOGIN users were added to stage_v2 after Step 4 had already
 *          merged to prd_v2, leaving them unresolvable by usp_Atlas_Merge.
 ******************************************************************************/

USE Atlas_Staging;
GO

IF OBJECT_ID('etl.usp_Atlas_LDAP', 'P') IS NOT NULL
    DROP PROCEDURE etl.usp_Atlas_LDAP;
GO

CREATE PROCEDURE etl.usp_Atlas_LDAP
    @ExecutionID            UNIQUEIDENTIFIER = NULL,
    @RaiseErrorOnFail       BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    DECLARE @PackageName NVARCHAR(100) = 'ETL-LDAP';
    DECLARE @LogID BIGINT;
    DECLARE @StepSequence INT = 0;
    DECLARE @RowCount INT;
    DECLARE @TotalRows INT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    IF @ExecutionID IS NULL
        SET @ExecutionID = NEWID();
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- PIPELINE B: Validate raw_v2.CLARITY_EMP is populated
    -- (atlas_clarity_extractor.py must run before this SP)
    -- ══════════════════════════════════════════════════════════════════════════

    DECLARE @RawEmpCount INT;
    SELECT @RawEmpCount = COUNT(*) FROM raw_v2.CLARITY_EMP;

    IF @RawEmpCount = 0
    BEGIN
        PRINT 'WARNING: raw_v2.CLARITY_EMP is empty. Run atlas_clarity_extractor.py first.';
        IF @RaiseErrorOnFail = 1
            RAISERROR('raw_v2.CLARITY_EMP is empty. Run atlas_clarity_extractor.py first.', 16, 1);
        RETURN 1;
    END;

    PRINT 'Pipeline B: raw_v2.CLARITY_EMP contains ' + CAST(@RawEmpCount AS VARCHAR(10)) + ' rows';
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 1: Load Users from Azure AD (matches SSIS ETL-LDAP.dtsx exactly)
    -- ══════════════════════════════════════════════════════════════════════════
    -- LEFT OUTER JOINs keep all Azure AD users (SSIS pattern — not INNER).
    -- EpicId resolved via EMPtoAzureMap → raw_v2.CLARITY_EMP.SYSTEM_LOGIN.
    -- User attributes (name, dept, title, phone) from AzureADUsers directly.
    -- No ClarityDepartments dependency.
    
    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart 
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 1 - Load Azure AD to Clarity User Mapping (Pipeline B)',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;
    
    BEGIN TRY
        TRUNCATE TABLE stage_v2.ReportObjectUser;

        -- Column mapping (matches SSIS ETL-LDAP.dtsx INSERT into stage.ReportObjectUser):
        --   Username    ← a.UserPrincipalName    AccountName ← a.UserPrincipalName
        --   DisplayName ← a.DisplayName          FullName    ← CONCAT(GivenName, Surname)
        --   FirstName   ← a.GivenName            LastName    ← a.Surname
        --   Department  ← a.Department            Title       ← a.JobTitle
        --   Phone       ← a.TelephoneNumber      Email       ← a.UserPrincipalName
        --   EpicId      ← emp.USER_ID (via EMPtoAzureMap → CLARITY_EMP.SYSTEM_LOGIN)
        --   EmployeeID  ← NULL (SSIS passes NULL)
        -- JOIN chain (LEFT OUTER — all Azure AD users kept, matching SSIS):
        --   a.UserPrincipalName = eam.AZURE_upn
        --   eam.Epic_AccountID = emp.SYSTEM_LOGIN
        INSERT INTO stage_v2.ReportObjectUser (
            Username,
            EmployeeID,
            AccountName,
            DisplayName,
            FullName,
            FirstName,
            LastName,
            Department,
            Title,
            Phone,
            Email,
            EpicId,
            LastLoadDate,
            ExtractDate
        )
        SELECT DISTINCT
            a.UserPrincipalName                         AS Username,
            NULL                                        AS EmployeeID,
            a.UserPrincipalName                         AS AccountName,
            a.DisplayName                               AS DisplayName,
            CONCAT(a.GivenName, N' ', a.Surname)        AS FullName,
            a.GivenName                                 AS FirstName,
            a.Surname                                   AS LastName,
            a.Department                                AS Department,
            a.JobTitle                                   AS Title,
            a.TelephoneNumber                           AS Phone,
            a.UserPrincipalName                         AS Email,
            CAST(emp.USER_ID AS NVARCHAR(MAX))          AS EpicId,
            GETDATE()                                   AS LastLoadDate,
            GETDATE()                                   AS ExtractDate
        FROM dbo.AzureADUsers a
        LEFT OUTER JOIN dbo.EMPtoAzureMap eam
            ON a.UserPrincipalName = eam.AZURE_upn
        LEFT OUTER JOIN raw_v2.CLARITY_EMP emp
            ON eam.Epic_AccountID = emp.SYSTEM_LOGIN
        WHERE a.UserPrincipalName IS NOT NULL
          AND a.UserPrincipalName <> ''
          AND a.UserPrincipalName NOT LIKE '%#EXT#%';

        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 1 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' users loaded to stage_v2.ReportObjectUser';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 1 failed: ' + @ErrorMessage;
    END CATCH
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 2: Load Azure AD Groups (matches SSIS: all groups, no name filter)
    -- SSIS column mapping: DisplayName+' (group)' → GroupName,
    --   ObjectType → GroupType, 'Azure AD' → SourceSystem.
    -- EXISTS filter retains SSIS INNER JOIN semantic: only groups with at
    -- least one member present in dbo.AzureADUsers.
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 2 - Load Azure AD Groups',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        TRUNCATE TABLE stage_v2.ReportObjectUserGroups;

        INSERT INTO stage_v2.ReportObjectUserGroups (
            GroupName,
            GroupDescription,
            GroupType,
            SourceSystem,
            ExtractDate,
            EpicId,
            AccountName,
            GroupEmail
        )
        SELECT DISTINCT
            CONCAT(aag.DisplayName, ' (group)')    AS GroupName,
            aag.Description                         AS GroupDescription,
            N'Group'                                AS GroupType,
            'Azure AD'                              AS SourceSystem,
            GETDATE()                               AS ExtractDate,
            NULL                                    AS EpicId,
            aag.DisplayName                         AS AccountName,
            aag.Mail                                AS GroupEmail
        FROM dbo.AzureADGroups aag
        WHERE aag.DisplayName IS NOT NULL
          AND EXISTS (
              SELECT 1
              FROM dbo.AzureUserGroupMap agm
              INNER JOIN dbo.AzureADUsers aad
                  ON agm.UserObjectID = aad.ObjectId
              WHERE agm.GroupObjectID = aag.ObjectId
          );
        
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 2 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' groups loaded to stage_v2.ReportObjectUserGroups';
        
    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 2 failed: ' + @ErrorMessage;
    END CATCH
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 3: Load Group Memberships (matches SSIS: all memberships, no filter)
    -- GroupName includes ' (group)' suffix to match Step 2 (Merge joins on it).
    -- INNER JOINs ensure only groups with members in AzureADUsers are included.
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID = @ExecutionID,
        @PackageName = @PackageName,
        @StepName = 'Step 3 - Load Group Memberships',
        @StepSequence = @StepSequence,
        @LogID = @LogID OUTPUT;

    BEGIN TRY
        TRUNCATE TABLE stage_v2.ReportObjectUserGroupMembers;

        INSERT INTO stage_v2.ReportObjectUserGroupMembers (
            GroupName,
            MemberName,
            MemberEmail,
            MemberType,
            SourceSystem,
            ExtractDate,
            GroupType,
            EpicId
        )
        SELECT
            CONCAT(aag.DisplayName, ' (group)')    AS GroupName,
            aad.UserPrincipalName                   AS MemberName,
            aad.Mail                                AS MemberEmail,
            'User'                                  AS MemberType,
            'Azure AD'                              AS SourceSystem,
            GETDATE()                               AS ExtractDate,
            N'Group'                                AS GroupType,
            NULL                                    AS EpicId
        FROM dbo.AzureUserGroupMap agm
        INNER JOIN dbo.AzureADGroups aag
            ON agm.GroupObjectID = aag.ObjectId
        INNER JOIN dbo.AzureADUsers aad
            ON agm.UserObjectID = aad.ObjectId
        WHERE aag.DisplayName IS NOT NULL;
        
        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;
        
        EXEC etl.usp_Atlas_LogEnd 
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';
        
        PRINT 'Step 3 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' memberships loaded';
        
    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 3 failed: ' + @ErrorMessage;
    END CATCH
    
    
    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 4: Append Clarity Security Groups → stage_v2.ReportObjectUserGroups
    -- Source: raw_v2.ClarityUserGroups (populated by atlas_clarity_extractor.py)
    -- SELECT DISTINCT on GroupName since raw table has one row per user-group pair.
    -- No TRUNCATE — appends after Azure AD groups from Step 2.
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = 'Step 4 - Append Clarity Security Groups',
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectUserGroups (
            GroupName,
            GroupDescription,
            GroupType,
            SourceSystem,
            ExtractDate,
            EpicId,
            AccountName,
            GroupEmail
        )
        SELECT DISTINCT
            cug.GroupName                               AS GroupName,
            NULL                                        AS GroupDescription,
            cug.GroupSource                             AS GroupType,
            N'Clarity'                                  AS SourceSystem,
            GETDATE()                                   AS ExtractDate,
            cug.GroupId                                 AS EpicId,
            NULL                                        AS AccountName,
            NULL                                        AS GroupEmail
        FROM raw_v2.ClarityUserGroups cug
        WHERE cug.GroupName IS NOT NULL;

        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 4 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' Clarity groups appended to stage_v2.ReportObjectUserGroups';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 4 failed: ' + @ErrorMessage;
    END CATCH


    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 5: Append Clarity Users → stage_v2.ReportObjectUser
    -- Source: raw_v2.ClarityUsernameLinks (SSIS ETL-Clarity "Clarity Users")
    -- Username = CASE WHEN domain_name = '' THEN [Name] ELSE domain_name END
    -- EpicId = user_Id. All other user columns NULL.
    -- NOT EXISTS anti-join excludes users already loaded by Azure AD (Step 1).
    -- No TRUNCATE — appends after Azure AD users from Step 1.
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = 'Step 5 - Append Clarity Users',
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectUser (
            Username,
            EmployeeID,
            AccountName,
            DisplayName,
            FullName,
            FirstName,
            LastName,
            Department,
            Title,
            Phone,
            Email,
            EpicId,
            LastLoadDate,
            ExtractDate
        )
        SELECT DISTINCT
            CASE WHEN cul.domain_name = '' THEN cul.[Name]
                 ELSE cul.domain_name
            END                                         AS Username,
            NULL                                        AS EmployeeID,
            CASE WHEN cul.domain_name = '' THEN cul.[Name]
                 ELSE cul.domain_name
            END                                         AS AccountName,
            cul.[Name]                                  AS DisplayName,
            NULL                                        AS FullName,
            NULL                                        AS FirstName,
            NULL                                        AS LastName,
            NULL                                        AS Department,
            NULL                                        AS Title,
            NULL                                        AS Phone,
            NULL                                        AS Email,
            cul.user_Id                                 AS EpicId,
            GETDATE()                                   AS LastLoadDate,
            GETDATE()                                   AS ExtractDate
        FROM raw_v2.ClarityUsernameLinks cul
        WHERE CASE WHEN cul.domain_name = '' THEN cul.[Name]
                   ELSE cul.domain_name
              END IS NOT NULL
          AND NOT EXISTS (
              SELECT 1
              FROM stage_v2.ReportObjectUser existing
              WHERE existing.Username = CASE WHEN cul.domain_name = ''
                                             THEN cul.[Name]
                                             ELSE cul.domain_name
                                        END
          );

        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 5 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' Clarity users appended to stage_v2.ReportObjectUser';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 5 failed: ' + @ErrorMessage;
    END CATCH


    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 6: Append Clarity Security Group Memberships
    --         → stage_v2.ReportObjectUserGroupMembers
    -- Source: raw_v2.ClarityUserGroups + raw_v2.ClarityUsernameLinks
    -- MemberName resolved via ClarityUsernameLinks (SSIS "Clarity Users" pattern):
    --   CASE WHEN domain_name = '' THEN [Name] ELSE domain_name END
    -- INNER JOIN to ClarityUsernameLinks excludes users without a resolvable
    -- username (matches SSIS pre-filter behavior).
    -- No TRUNCATE — appends after Azure AD memberships from Step 3.
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = 'Step 6 - Append Clarity Security Group Memberships',
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        INSERT INTO stage_v2.ReportObjectUserGroupMembers (
            GroupName,
            MemberName,
            MemberEmail,
            MemberType,
            SourceSystem,
            ExtractDate,
            GroupType,
            EpicId
        )
        SELECT DISTINCT
            cug.GroupName                               AS GroupName,
            CASE WHEN cul.domain_name = '' THEN cul.[Name]
                 ELSE cul.domain_name
            END                                         AS MemberName,
            NULL                                        AS MemberEmail,
            N'User'                                     AS MemberType,
            N'Clarity'                                  AS SourceSystem,
            GETDATE()                                   AS ExtractDate,
            cug.GroupSource                             AS GroupType,
            cug.GroupId                                 AS EpicId
        FROM raw_v2.ClarityUserGroups cug
        INNER JOIN raw_v2.ClarityUsernameLinks cul
            ON cug.USER_ID = cul.user_Id
        WHERE cug.GroupName IS NOT NULL
          AND CASE WHEN cul.domain_name = '' THEN cul.[Name]
                   ELSE cul.domain_name
              END IS NOT NULL;

        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 6 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' Clarity memberships appended';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 6 failed: ' + @ErrorMessage;
    END CATCH


    -- ══════════════════════════════════════════════════════════════════════════
    -- STEP 7: Merge ALL staged users into prd_v2.[User]
    -- Runs AFTER Steps 1+5 so both Azure AD and Clarity users are in staging.
    -- Match key: Username (natural key — UserID is identity, auto-generated).
    -- WHEN NOT MATCHED: insert all ETL-populated columns; app-managed columns
    --   (LastLogin, Fullname_calc, Firstname_calc, ProfilePhoto, Base) are
    --   NULL on initial insert and managed by the application.
    -- WHEN MATCHED: update ETL-populated columns only — never overwrite
    --   app-managed columns.
    -- ══════════════════════════════════════════════════════════════════════════

    SET @StepSequence = @StepSequence + 1;
    EXEC etl.usp_Atlas_LogStart
        @ExecutionID  = @ExecutionID,
        @PackageName  = @PackageName,
        @StepName     = 'Step 7 - Merge Users to prd_v2.[User]',
        @StepSequence = @StepSequence,
        @LogID        = @LogID OUTPUT;

    BEGIN TRY
        MERGE INTO prd_v2.[User] AS tgt
        USING (
            SELECT *
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY Username
                        ORDER BY EpicId DESC  -- prefer non-NULL EpicId
                    ) AS rn
                FROM stage_v2.ReportObjectUser
            ) ranked
            WHERE rn = 1
        ) AS src
            ON tgt.Username = src.Username
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                Username, EmployeeID, AccountName, DisplayName, FullName,
                FirstName, LastName, Department, Title, Phone, Email, EpicId,
                LastLoadDate,
                -- App-managed: NULL on insert, never overwritten by ETL
                LastLogin, Fullname_calc, Firstname_calc, ProfilePhoto, Base
            )
            VALUES (
                src.Username, src.EmployeeID, src.AccountName, src.DisplayName, src.FullName,
                src.FirstName, src.LastName, src.Department, src.Title, src.Phone, src.Email, src.EpicId,
                GETDATE(),
                NULL, NULL, NULL, NULL, NULL
            )
        WHEN MATCHED THEN
            UPDATE SET
                -- ETL-populated columns — refreshed on every run
                tgt.EmployeeID   = src.EmployeeID,
                tgt.AccountName  = src.AccountName,
                tgt.DisplayName  = src.DisplayName,
                tgt.FullName     = src.FullName,
                tgt.FirstName    = src.FirstName,
                tgt.LastName     = src.LastName,
                tgt.Department   = src.Department,
                tgt.Title        = src.Title,
                tgt.Phone        = src.Phone,
                tgt.Email        = src.Email,
                tgt.EpicId       = src.EpicId,
                tgt.LastLoadDate = GETDATE()
                -- Explicitly NOT updating: LastLogin, Fullname_calc, Firstname_calc,
                --                          ProfilePhoto, Base
        ;

        SET @RowCount = @@ROWCOUNT;
        SET @TotalRows = @TotalRows + @RowCount;

        EXEC etl.usp_Atlas_LogEnd
            @LogID = @LogID,
            @RowsAffected = @RowCount,
            @Status = 'Success';

        PRINT 'Step 7 completed: ' + CAST(@RowCount AS VARCHAR(10)) + ' rows merged to prd_v2.[User]';

    END TRY
    BEGIN CATCH
        EXEC etl.usp_Atlas_LogError @LogID = @LogID;
        SET @ErrorMessage = ERROR_MESSAGE();

        IF @RaiseErrorOnFail = 1
            THROW;
        ELSE
            PRINT 'Step 7 failed: ' + @ErrorMessage;
    END CATCH


    -- ══════════════════════════════════════════════════════════════════════════
    -- COMPLETE
    -- ══════════════════════════════════════════════════════════════════════════

    PRINT '';
    PRINT 'ETL-LDAP completed (Pipeline B). Total rows staged: ' + CAST(@TotalRows AS VARCHAR(10));
    
END
GO

PRINT 'Created procedure: etl.usp_Atlas_LDAP (uses raw_v2 in place of linked server queries)';
GO
