-- =============================================================================
-- Debezium Oracle Source Connector - Permission Review & Re-apply Script
-- Confluent Kafka / Debezium Oracle LogMiner Connector
--
-- Usage:
--   Edit the constants in the CONFIGURATION section at the top of the block,
--   then execute the entire script as a DBA user (SYSDBA or user with DBA role).
--   Enable DBMS_OUTPUT in your SQL client to see all output.
--
-- Compatible with Oracle 19c CDB (multitenant) and non-CDB deployments.
-- No SQL*Plus dependencies; runs in SQL Developer, DBeaver, SQLcl, or sqlplus.
-- =============================================================================

DECLARE
    -- =========================================================================
    -- CONFIGURATION - Edit these values before running
    -- =========================================================================

    -- Debezium connector user (C##DBZUSER for CDB, DBZUSER for non-CDB)
    c_debezium_user    CONSTANT VARCHAR2(128) := 'C##DBZUSER';

    -- Schema that owns the heartbeat table (often same as c_debezium_user)
    c_heartbeat_schema CONSTANT VARCHAR2(128) := 'C##DBZUSER';

    -- Heartbeat table name (must match heartbeat.action.query in connector config)
    c_heartbeat_table  CONSTANT VARCHAR2(128) := 'DEBEZIUM_HEARTBEAT';

    -- TRUE for CDB (multitenant), FALSE for non-CDB (single-tenant)
    c_is_cdb           CONSTANT BOOLEAN       := TRUE;

    -- =========================================================================
    -- Derived values (do not edit)
    -- =========================================================================
    v_cont  VARCHAR2(20);   -- CONTAINER = ALL for CDB; empty string for non-CDB

    -- =========================================================================
    -- Helper: print a labelled section header via DBMS_OUTPUT
    -- =========================================================================
    PROCEDURE section_header(p_title IN VARCHAR2) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('============================================================');
        DBMS_OUTPUT.PUT_LINE('  ' || p_title);
        DBMS_OUTPUT.PUT_LINE('============================================================');
    END section_header;

    -- =========================================================================
    -- Helper: execute a GRANT; logs success or a non-fatal warning on failure
    -- =========================================================================
    PROCEDURE do_grant(p_stmt IN VARCHAR2) IS
    BEGIN
        EXECUTE IMMEDIATE p_stmt;
        DBMS_OUTPUT.PUT_LINE('  OK  : ' || p_stmt);
    EXCEPTION
        WHEN OTHERS THEN
            DBMS_OUTPUT.PUT_LINE('  WARN: ' || p_stmt);
            DBMS_OUTPUT.PUT_LINE('        ' || SQLERRM);
    END do_grant;

BEGIN
    v_cont := CASE WHEN c_is_cdb THEN ' CONTAINER = ALL' ELSE '' END;

    -- =========================================================================
    -- SECTION 1: DIAGNOSTIC - Review Current Permissions
    -- =========================================================================

    section_header('SECTION 1: System Privileges for ' || c_debezium_user);
    FOR r IN (
        SELECT privilege, admin_option, common, inherited
          FROM dba_sys_privs
         WHERE grantee = UPPER(c_debezium_user)
         ORDER BY privilege
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  PRIVILEGE=' || RPAD(r.privilege, 40)
            || ' ADMIN='     || r.admin_option
            || ' COMMON='    || r.common
            || ' INHERITED=' || r.inherited);
    END LOOP;

    section_header('SECTION 1b: Role Grants for ' || c_debezium_user);
    FOR r IN (
        SELECT granted_role, admin_option, default_role, common, inherited
          FROM dba_role_privs
         WHERE grantee = UPPER(c_debezium_user)
         ORDER BY granted_role
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ROLE='    || RPAD(r.granted_role, 30)
            || ' ADMIN='   || r.admin_option
            || ' DEFAULT=' || r.default_role
            || ' COMMON='  || r.common);
    END LOOP;

    section_header('SECTION 1c: Object Privileges for ' || c_debezium_user);
    FOR r IN (
        SELECT owner, table_name, privilege, grantable, common, inherited
          FROM dba_tab_privs
         WHERE grantee = UPPER(c_debezium_user)
         ORDER BY owner, table_name, privilege
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || r.owner || '.' || RPAD(r.table_name, 35)
            || ' PRIV='      || RPAD(r.privilege, 10)
            || ' GRANTABLE=' || r.grantable
            || ' COMMON='    || r.common);
    END LOOP;

    section_header('SECTION 1d: Heartbeat Table Existence');
    FOR r IN (
        SELECT owner, table_name, status,
               NVL(TO_CHAR(num_rows),   'N/A')                    AS num_rows,
               NVL(TO_CHAR(last_analyzed, 'YYYY-MM-DD'), 'N/A')   AS last_analyzed
          FROM dba_tables
         WHERE owner      = UPPER(c_heartbeat_schema)
           AND table_name = UPPER(c_heartbeat_table)
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  TABLE='    || r.owner || '.' || r.table_name
            || ' STATUS='   || r.status
            || ' ROWS='     || r.num_rows
            || ' ANALYZED=' || r.last_analyzed);
    END LOOP;

    section_header('SECTION 1e: Supplemental Logging Status');
    FOR r IN (
        SELECT log_mode,
               supplemental_log_data_min AS supp_min,
               supplemental_log_data_pk  AS supp_pk,
               supplemental_log_data_ui  AS supp_ui,
               supplemental_log_data_fk  AS supp_fk,
               supplemental_log_data_all AS supp_all
          FROM v$database
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  LOG_MODE=' || r.log_mode
            || '  MIN=' || r.supp_min
            || '  PK='  || r.supp_pk
            || '  UI='  || r.supp_ui
            || '  FK='  || r.supp_fk
            || '  ALL=' || r.supp_all);
    END LOOP;

    section_header('SECTION 1f: Archived Log Destination Status');
    FOR r IN (
        SELECT dest_id, dest_name, status, target, archiver, schedule, destination
          FROM v$archive_dest_status
         WHERE status != 'INACTIVE'
         ORDER BY dest_id
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  DEST_ID=' || r.dest_id
            || '  NAME='   || RPAD(NVL(r.dest_name, '?'), 20)
            || '  STATUS=' || r.status
            || '  TARGET=' || r.target
            || '  DEST='   || NVL(r.destination, '(none)'));
    END LOOP;

    -- =========================================================================
    -- SECTION 2: RE-APPLY LOGMINER PERMISSIONS
    -- Grants required for Debezium Oracle connector via LogMiner.
    -- do_grant() logs a warning on failure and continues; re-runs are safe
    -- because Oracle does not error on already-held privileges.
    -- =========================================================================

    section_header('SECTION 2: Applying LogMiner Permissions to ' || c_debezium_user);

    -- Core session privilege
    do_grant('GRANT CREATE SESSION TO '         || c_debezium_user || v_cont);

    -- LogMiner engine
    do_grant('GRANT LOGMINING TO '              || c_debezium_user || v_cont);

    -- Catalog and transaction visibility
    do_grant('GRANT SELECT_CATALOG_ROLE TO '    || c_debezium_user || v_cont);
    do_grant('GRANT EXECUTE_CATALOG_ROLE TO '   || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ANY TRANSACTION TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ANY TABLE TO '       || c_debezium_user || v_cont);
    do_grant('GRANT FLASHBACK ANY TABLE TO '    || c_debezium_user || v_cont);
    do_grant('GRANT LOCK ANY TABLE TO '         || c_debezium_user || v_cont);

    -- DDL capture support
    do_grant('GRANT CREATE TABLE TO '           || c_debezium_user || v_cont);
    do_grant('GRANT CREATE SEQUENCE TO '        || c_debezium_user || v_cont);

    -- DBMS_LOGMNR execution
    do_grant('GRANT EXECUTE ON SYS.DBMS_LOGMNR   TO ' || c_debezium_user || v_cont);
    do_grant('GRANT EXECUTE ON SYS.DBMS_LOGMNR_D TO ' || c_debezium_user || v_cont);

    -- V$ dynamic view access
    do_grant('GRANT SELECT ON SYS.V_$DATABASE            TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$LOG                 TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$LOG_HISTORY         TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$LOGFILE             TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$ARCHIVED_LOG        TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$ARCHIVE_DEST_STATUS TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$TRANSACTION         TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$LOGMNR_LOGS         TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$LOGMNR_CONTENTS     TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$LOGMNR_PARAMETERS   TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$INSTANCE            TO ' || c_debezium_user || v_cont);
    do_grant('GRANT SELECT ON SYS.V_$SESSION             TO ' || c_debezium_user || v_cont);

    -- CDB-only: SET CONTAINER and V_$PDBS (V_$PDBS does not exist in non-CDB)
    IF c_is_cdb THEN
        do_grant('GRANT SET CONTAINER TO '          || c_debezium_user || ' CONTAINER = ALL');
        do_grant('GRANT SELECT ON SYS.V_$PDBS TO ' || c_debezium_user || ' CONTAINER = ALL');
        DBMS_OUTPUT.PUT_LINE('  CDB-specific grants applied.');
    ELSE
        DBMS_OUTPUT.PUT_LINE('  Non-CDB: SET CONTAINER and V_$PDBS grants skipped.');
    END IF;

    DBMS_OUTPUT.PUT_LINE('Permissions applied successfully.');

    -- =========================================================================
    -- SECTION 3: HEARTBEAT TABLE SETUP
    --
    -- The heartbeat table supports the Debezium heartbeat mechanism.
    -- Connector config should include:
    --   heartbeat.interval.ms  = <interval>
    --   heartbeat.action.query = UPDATE <schema>.<table>
    --                            SET TS_MS = SYSTIMESTAMP WHERE ID = 1
    -- =========================================================================

    section_header('SECTION 3: Heartbeat Table - '
        || c_heartbeat_schema || '.' || c_heartbeat_table);

    DECLARE
        v_table_exists NUMBER;
        v_fqn          VARCHAR2(261);
        v_pk_name      VARCHAR2(132);
    BEGIN
        v_fqn     := c_heartbeat_schema || '.' || c_heartbeat_table;
        v_pk_name := 'PK_' || c_heartbeat_table;

        SELECT COUNT(*)
          INTO v_table_exists
          FROM dba_tables
         WHERE owner      = UPPER(c_heartbeat_schema)
           AND table_name = UPPER(c_heartbeat_table);

        IF v_table_exists = 0 THEN
            EXECUTE IMMEDIATE
                'CREATE TABLE ' || v_fqn || ' ('
             || '    ID     NUMBER(1)  NOT NULL,'
             || '    TS_MS  TIMESTAMP  DEFAULT SYSTIMESTAMP NOT NULL,'
             || '    CONSTRAINT ' || v_pk_name || ' PRIMARY KEY (ID)'
             || ')';
            DBMS_OUTPUT.PUT_LINE('  Heartbeat table created: ' || v_fqn);
        ELSE
            DBMS_OUTPUT.PUT_LINE('  Heartbeat table already exists: ' || v_fqn);
        END IF;

        -- Ensure seed row (ID=1) exists; connector UPDATE targets this row
        EXECUTE IMMEDIATE
            'MERGE INTO ' || v_fqn || ' tgt '
         || 'USING (SELECT 1 AS id FROM dual) src ON (tgt.id = src.id) '
         || 'WHEN NOT MATCHED THEN INSERT (id, ts_ms) VALUES (1, SYSTIMESTAMP)';

        COMMIT;
        DBMS_OUTPUT.PUT_LINE('  Seed row verified in ' || v_fqn);

        -- Grant DML access to the connector user
        EXECUTE IMMEDIATE
            'GRANT SELECT, INSERT, UPDATE ON ' || v_fqn || ' TO ' || c_debezium_user;
        DBMS_OUTPUT.PUT_LINE('  DML grants applied on ' || v_fqn);

        -- Enable supplemental logging on the heartbeat table
        EXECUTE IMMEDIATE
            'ALTER TABLE ' || v_fqn || ' ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS';
        DBMS_OUTPUT.PUT_LINE('  Supplemental logging enabled on ' || v_fqn);

    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            DBMS_OUTPUT.PUT_LINE('ERROR in Section 3: ' || SQLERRM);
            RAISE;
    END;

    DBMS_OUTPUT.PUT_LINE('Heartbeat table setup complete.');

    -- =========================================================================
    -- SECTION 4: SUPPLEMENTAL LOGGING VERIFICATION
    -- Debezium requires at minimum minimal supplemental logging at the DB level.
    -- If any column below shows NO, execute the appropriate ALTER DATABASE
    -- statement against the database as SYSDBA (outside this script).
    -- =========================================================================

    section_header('SECTION 4: Supplemental Logging Status');

    FOR r IN (
        SELECT supplemental_log_data_min AS min_supp,
               supplemental_log_data_pk  AS pk_supp,
               supplemental_log_data_all AS all_supp
          FROM v$database
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  MIN=' || r.min_supp
            || '  PK='  || r.pk_supp
            || '  ALL=' || r.all_supp);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('  To enable (execute separately as SYSDBA if needed):');
    DBMS_OUTPUT.PUT_LINE('    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;                       -- minimal (required)');
    DBMS_OUTPUT.PUT_LINE('    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (PRIMARY KEY) COLUMNS; -- recommended');
    DBMS_OUTPUT.PUT_LINE('    ALTER DATABASE ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;         -- all columns');

    -- =========================================================================
    -- SECTION 5: POST-APPLY VERIFICATION
    -- =========================================================================

    section_header('SECTION 5: Post-Apply Verification');

    DBMS_OUTPUT.PUT_LINE('--- System Privileges ---');
    FOR r IN (
        SELECT privilege, admin_option, common
          FROM dba_sys_privs
         WHERE grantee = UPPER(c_debezium_user)
         ORDER BY privilege
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || RPAD(r.privilege, 40)
            || ' ADMIN='  || r.admin_option
            || ' COMMON=' || r.common);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('--- Role Grants ---');
    FOR r IN (
        SELECT granted_role, admin_option, default_role, common
          FROM dba_role_privs
         WHERE grantee = UPPER(c_debezium_user)
         ORDER BY granted_role
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || RPAD(r.granted_role, 30)
            || ' ADMIN='   || r.admin_option
            || ' DEFAULT=' || r.default_role
            || ' COMMON='  || r.common);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('--- Object Privileges (V$ Views + DBMS Packages) ---');
    FOR r IN (
        SELECT owner, table_name, privilege, grantable
          FROM dba_tab_privs
         WHERE grantee = UPPER(c_debezium_user)
         ORDER BY owner, table_name
    ) LOOP
        DBMS_OUTPUT.PUT_LINE('  ' || r.owner || '.' || RPAD(r.table_name, 35)
            || ' PRIV='      || RPAD(r.privilege, 10)
            || ' GRANTABLE=' || r.grantable);
    END LOOP;

    DBMS_OUTPUT.PUT_LINE('--- Heartbeat Table Row Count ---');
    DECLARE
        v_count NUMBER;
        v_fqn   VARCHAR2(261);
    BEGIN
        v_fqn := c_heartbeat_schema || '.' || c_heartbeat_table;
        EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || v_fqn INTO v_count;
        DBMS_OUTPUT.PUT_LINE('  HEARTBEAT_ROWS=' || v_count);
    END;

    section_header('Script complete. Review output above for any missing grants.');

EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('');
        DBMS_OUTPUT.PUT_LINE('FATAL ERROR: ' || SQLERRM);
        RAISE;
END;
/
