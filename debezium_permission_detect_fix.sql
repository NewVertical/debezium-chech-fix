-- =============================================================================
-- Debezium Oracle Source Connector - Permission Review & Re-apply Script
-- Confluent Kafka / Debezium Oracle LogMiner Connector
--
-- Usage:
--   Set the variables in the CONFIGURATION section below before running.
--   Run as a DBA user (SYSDBA or user with DBA role).
--
-- Supports both CDB (multitenant) and non-CDB Oracle deployments.
-- =============================================================================

-- =============================================================================
-- CONFIGURATION - Adjust these values before running
-- =============================================================================

-- Debezium connector user (e.g., C##DBZUSER for CDB, DBZUSER for non-CDB)
DEFINE DEBEZIUM_USER = 'C##DBZUSER'

-- Schema that owns the heartbeat table (often same as DEBEZIUM_USER)
DEFINE HEARTBEAT_SCHEMA = 'C##DBZUSER'

-- Heartbeat table name (must match heartbeat.action.query in connector config)
DEFINE HEARTBEAT_TABLE = 'DEBEZIUM_HEARTBEAT'

-- Set to TRUE if running against a CDB (multitenant) database, FALSE otherwise
-- When TRUE, CONTAINER=ALL is appended to all grants
DEFINE IS_CDB = 'TRUE'

-- Derive the CONTAINER clause from &&IS_CDB so all grants reference one variable
COLUMN container_clause NEW_VALUE container_clause NOPRINT
SELECT CASE WHEN UPPER('&&IS_CDB') = 'TRUE' THEN 'CONTAINER = ALL' ELSE ' ' END
    AS container_clause
FROM dual;

-- =============================================================================
-- SECTION 1: DIAGNOSTIC - Review Current Permissions
-- =============================================================================

PROMPT ============================================================
PROMPT  SECTION 1: Current System Privileges for &&DEBEZIUM_USER
PROMPT ============================================================

SELECT
    grantee,
    privilege,
    admin_option,
    common,
    inherited
FROM dba_sys_privs
WHERE grantee = UPPER('&&DEBEZIUM_USER')
ORDER BY privilege;

PROMPT
PROMPT ============================================================
PROMPT  SECTION 1b: Current Role Grants for &&DEBEZIUM_USER
PROMPT ============================================================

SELECT
    grantee,
    granted_role,
    admin_option,
    default_role,
    common,
    inherited
FROM dba_role_privs
WHERE grantee = UPPER('&&DEBEZIUM_USER')
ORDER BY granted_role;

PROMPT
PROMPT ============================================================
PROMPT  SECTION 1c: Current Object/Table Privileges for &&DEBEZIUM_USER
PROMPT ============================================================

SELECT
    grantee,
    owner,
    table_name,
    privilege,
    grantable,
    common,
    inherited
FROM dba_tab_privs
WHERE grantee = UPPER('&&DEBEZIUM_USER')
ORDER BY owner, table_name, privilege;

PROMPT
PROMPT ============================================================
PROMPT  SECTION 1d: Heartbeat Table Existence Check
PROMPT ============================================================

SELECT
    owner,
    table_name,
    status,
    num_rows,
    last_analyzed
FROM dba_tables
WHERE owner    = UPPER('&&HEARTBEAT_SCHEMA')
  AND table_name = UPPER('&&HEARTBEAT_TABLE');

PROMPT
PROMPT ============================================================
PROMPT  SECTION 1e: Supplemental Logging Status
PROMPT ============================================================

SELECT
    log_mode,
    supplemental_log_data_min   AS supp_min,
    supplemental_log_data_pk    AS supp_pk,
    supplemental_log_data_ui    AS supp_ui,
    supplemental_log_data_fk    AS supp_fk,
    supplemental_log_data_all   AS supp_all
FROM v$database;

PROMPT
PROMPT ============================================================
PROMPT  SECTION 1f: Archived Log Destination Status
PROMPT ============================================================

SELECT
    dest_id,
    dest_name,
    status,
    target,
    archiver,
    schedule,
    destination
FROM v$archive_dest_status
WHERE status != 'INACTIVE'
ORDER BY dest_id;

-- =============================================================================
-- SECTION 2: RE-APPLY LOGMINER PERMISSIONS
-- Grants required for Debezium Oracle connector via LogMiner.
-- Safe to re-run; Oracle silently ignores already-held privileges.
-- =============================================================================

PROMPT
PROMPT ============================================================
PROMPT  SECTION 2: Applying LogMiner Permissions to &&DEBEZIUM_USER
PROMPT ============================================================

-- ---- Core session and container privileges --------------------------------

GRANT CREATE SESSION    TO &&DEBEZIUM_USER &&container_clause;
GRANT SET CONTAINER     TO &&DEBEZIUM_USER &&container_clause;

-- ---- LogMiner engine privileges ------------------------------------------

GRANT LOGMINING         TO &&DEBEZIUM_USER &&container_clause;

-- ---- Catalog and transaction visibility ----------------------------------

GRANT SELECT_CATALOG_ROLE    TO &&DEBEZIUM_USER &&container_clause;
GRANT EXECUTE_CATALOG_ROLE   TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ANY TRANSACTION TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ANY TABLE       TO &&DEBEZIUM_USER &&container_clause;
GRANT FLASHBACK ANY TABLE    TO &&DEBEZIUM_USER &&container_clause;
GRANT LOCK ANY TABLE         TO &&DEBEZIUM_USER &&container_clause;

-- ---- DDL capture support -------------------------------------------------

GRANT CREATE TABLE    TO &&DEBEZIUM_USER &&container_clause;
GRANT CREATE SEQUENCE TO &&DEBEZIUM_USER &&container_clause;

-- ---- DBMS_LOGMNR execution -----------------------------------------------

GRANT EXECUTE ON SYS.DBMS_LOGMNR    TO &&DEBEZIUM_USER &&container_clause;
GRANT EXECUTE ON SYS.DBMS_LOGMNR_D  TO &&DEBEZIUM_USER &&container_clause;

-- ---- V$ dynamic view access ----------------------------------------------

GRANT SELECT ON SYS.V_$DATABASE            TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$LOG                 TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$LOG_HISTORY         TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$LOGFILE             TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$ARCHIVED_LOG        TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$TRANSACTION         TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$LOGMNR_LOGS         TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS     TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$LOGMNR_PARAMETERS   TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$INSTANCE            TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$SESSION             TO &&DEBEZIUM_USER &&container_clause;
GRANT SELECT ON SYS.V_$PDBS               TO &&DEBEZIUM_USER &&container_clause;

PROMPT Permissions applied successfully.

-- =============================================================================
-- SECTION 3: HEARTBEAT TABLE - Create If Not Exists and Set Permissions
--
-- The heartbeat table is used by the Debezium connector heartbeat mechanism.
-- The connector config should include:
--   heartbeat.interval.ms = <interval>
--   heartbeat.action.query = UPDATE <HEARTBEAT_SCHEMA>.<HEARTBEAT_TABLE>
--                            SET TS_MS = SYSTIMESTAMP WHERE ID = 1
-- =============================================================================

PROMPT
PROMPT ============================================================
PROMPT  SECTION 3: Heartbeat Table Setup - &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE
PROMPT ============================================================

-- Create heartbeat table if it does not already exist.
-- The MERGE ensures the seed row is present without failing on re-runs.

DECLARE
    v_table_exists NUMBER;
BEGIN
    SELECT COUNT(*)
      INTO v_table_exists
      FROM dba_tables
     WHERE owner      = UPPER('&&HEARTBEAT_SCHEMA')
       AND table_name = UPPER('&&HEARTBEAT_TABLE');

    IF v_table_exists = 0 THEN
        EXECUTE IMMEDIATE '
            CREATE TABLE &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE (
                ID     NUMBER(1)  NOT NULL,
                TS_MS  TIMESTAMP  DEFAULT SYSTIMESTAMP NOT NULL,
                CONSTRAINT PK_&&HEARTBEAT_TABLE PRIMARY KEY (ID)
            )';
        DBMS_OUTPUT.PUT_LINE('Heartbeat table created: &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE');
    ELSE
        DBMS_OUTPUT.PUT_LINE('Heartbeat table already exists: &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE');
    END IF;

    -- Ensure the seed row (ID=1) exists; connector UPDATE targets this row
    MERGE INTO &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE tgt
    USING (SELECT 1 AS id FROM dual) src
       ON (tgt.id = src.id)
    WHEN NOT MATCHED THEN
        INSERT (id, ts_ms) VALUES (1, SYSTIMESTAMP);

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('Seed row verified in &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE');

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE;
END;
/

-- ---- Grant DML access on the heartbeat table to the connector user --------
-- If the heartbeat schema and connector user are different, both grants apply.

GRANT SELECT, INSERT, UPDATE ON &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE TO &&DEBEZIUM_USER;

-- Enable supplemental logging on the heartbeat table so changes are captured
ALTER TABLE &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

PROMPT Heartbeat table setup complete.

-- =============================================================================
-- SECTION 4: SUPPLEMENTAL LOGGING VERIFICATION / ENABLEMENT
-- Debezium requires at minimum minimal supplemental logging at the DB level.
-- Recommended: enable supplemental logging for PRIMARY KEY columns.
-- =============================================================================

PROMPT
PROMPT ============================================================
PROMPT  SECTION 4: Supplemental Logging Check
PROMPT ============================================================

SELECT
    supplemental_log_data_min AS min_supplemental,
    supplemental_log_data_pk  AS pk_supplemental,
    supplemental_log_data_all AS all_supplemental
FROM v$database;

-- Uncomment ONE of the following blocks if supplemental logging is not enabled:

-- Option A: Minimal supplemental logging (required minimum)
-- ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

-- Option B: Primary Key supplemental logging (recommended for most workloads)
-- ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS;

-- Option C: All columns supplemental logging (use only if required)
-- ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

-- =============================================================================
-- SECTION 5: POST-APPLY VERIFICATION
-- Re-run the diagnostic queries to confirm all permissions are in place.
-- =============================================================================

PROMPT
PROMPT ============================================================
PROMPT  SECTION 5: Post-Apply Verification
PROMPT ============================================================

PROMPT --- System Privileges ---
SELECT privilege, admin_option, common
FROM dba_sys_privs
WHERE grantee = UPPER('&&DEBEZIUM_USER')
ORDER BY privilege;

PROMPT
PROMPT --- Role Grants ---
SELECT granted_role, admin_option, default_role, common
FROM dba_role_privs
WHERE grantee = UPPER('&&DEBEZIUM_USER')
ORDER BY granted_role;

PROMPT
PROMPT --- Object Privileges (V$ Views + DBMS Packages) ---
SELECT owner, table_name, privilege, grantable
FROM dba_tab_privs
WHERE grantee = UPPER('&&DEBEZIUM_USER')
ORDER BY owner, table_name;

PROMPT
PROMPT --- Heartbeat Table Row Count ---
SELECT COUNT(*) AS heartbeat_rows
FROM &&HEARTBEAT_SCHEMA..&&HEARTBEAT_TABLE;

PROMPT
PROMPT ============================================================
PROMPT  Script complete. Review output above for any missing grants.
PROMPT ============================================================
