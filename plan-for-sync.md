# Building Data Systems Sync

- Build out the visualization / test harness to debug issues and ensure consistency.
- consistency
- adaptability
- sync
- determine sync.
- resync
- location aware
- dashboards
- reporting
- search
- extensions
- export
- import
- UI
- Build an application for managing fit testing devices.  These devices are essentially windows 11 computers that are connected to a central hub.  The fit tests are test designed for managing n95 masks associated with hospital patients. Every year every person at the medical facility must be tested for the mask fit.   This application should manage the employees along with the tests performed and dates for the next test.


- UI mockups help to visualize the outcome of the system.
- Start with a way to search and see all data / new data associated with the devices.
- Bootstrap device.  Build new database / download from central location
    - Data storage
    - event storage.
    - Manage and store
    - allow user to read through employees, create new employees, see tests.
    - Report on the overall scores of the tests, see fit status, etc.. 
    - Question, what is their deliverable?  Can we streamline delivery of the test results?
- Build Conenctor to connect Kafka to Postgres
- Device Management, register device, assign unique id, download config.  Hook that into the system to get the report URI for Kafka bootstrap.
- Work through offline mode, windows user capture.
- person / auth management.
- Multi-phased approach.
- Single-tenant, bootstrap, data management.  Augment functionality.  Extend reporting and features.
- Provide mockups, deploy prototype, align protoptype data.  Re-sync stored backup.
- Multi-tenant
```
-- auto-generated definition

create table customFieldRecord(
    id        INTEGER        primary key autoincrement,
    labelName CHAR(10) collate NOCASE not null,
    required  BOOLEAN default 0       not null,
    enabled   BOOLEAN default 1       not null,
    combo     BOOLEAN default 0       not null,
    active    BOOLEAN default 0       not null,
    option0   CHAR(25) collate NOCASE,
    option1   CHAR(25) collate NOCASE,
    option2   CHAR(25) collate NOCASE,
    option3   CHAR(25) collate NOCASE,
    option4   CHAR(25) collate NOCASE,
    option5   CHAR(25) collate NOCASE,
    option6   CHAR(25) collate NOCASE,
    option7   CHAR(25) collate NOCASE,
    option8   CHAR(25) collate NOCASE,
    option9   CHAR(25) collate NOCASE
);

-- auto-generated definition 
create table dailyCheckRecord(
    id           INTEGER        primary key autoincrement,
     serialNumber CHAR(64) not null,
     date         DATETIME not null,
     particle     INTEGER  not null,
     classifier   INTEGER,
     zero         INTEGER  not null,
      maxFf        INTEGER  not null,
      n95          BOOLEAN  not null,
      unique (serialNumber, date) on conflict ignore);


create table dbInfo(    dbVersion INTEGER not null);

create table main.exerciseRecord
(
    id         INTEGER
        primary key autoincrement,
        number     INTEGER                 not null,
        exercise   CHAR(64) collate NOCASE not null,
        maskSample INTEGER                 not null,
        exclude    BOOLEAN                 not null,
        protocolId INTEGER
        references main.protocolRecord
            on delete cascade
);

create table main.fitTestRecord
(
    id                          INTEGER
        primary key autoincrement,
    testDate                    DATETIME,
    dueDate                     DATE,
    operator                    CHAR(64) collate NOCASE,
    maskSize                    CHAR(64) collate NOCASE,
    description                 CHAR(255) collate NOCASE,
    avgAmbient                  INTEGER,
    overallFf                   INTEGER,
    overallPass                 INTEGER,
    serialNumber                CHAR(64) collate NOCASE,
    n95                         BOOLEAN not null,
    firstName                   CHAR(64) collate NOCASE,
    lastName                    CHAR(64) collate NOCASE,
    idNumber                    CHAR(64) collate NOCASE,
    company                     CHAR(64) collate NOCASE,
    location                    CHAR(64) collate NOCASE,
    note                        CHAR(128) collate NOCASE,
    custom1Label                CHAR(64) collate NOCASE,
    custom1Data                 CHAR(64) collate NOCASE,
    custom2Label                CHAR(64) collate NOCASE,
    custom2Data                 CHAR(64) collate NOCASE,
    custom3Label                CHAR(64) collate NOCASE,
    custom3Data                 CHAR(64) collate NOCASE,
    custom4Label                CHAR(64) collate NOCASE,
    custom4Data                 CHAR(64) collate NOCASE,
    maskManufacturer            CHAR(64) collate NOCASE,
    maskModel                   CHAR(64) collate NOCASE,
    maskStyle                   CHAR(64) collate NOCASE,
    approval                    CHAR(64) collate NOCASE,
    ffPassLevel                 INTEGER,
    maskDescription             CHAR(64) collate NOCASE,
    protocolName                CHAR(64) collate NOCASE,
    protocolModel               CHAR(64) collate NOCASE,
    ambientPurge                INTEGER,
    ambientSample               INTEGER,
    maskPurge                   INTEGER,
    period                      CHAR(64),
    endOnFail                   BOOLEAN,
    algorithm                   INTEGER,
    numExercises                INTEGER,
    exercise1                   CHAR(64) collate NOCASE,
    maskSample1                 INTEGER,
    exclude1                    BOOLEAN,
    fitFactor1                  INTEGER,
    pass1                       BOOLEAN,
    exercise2                   CHAR(64) collate NOCASE,
    maskSample2                 INTEGER,
    exclude2                    BOOLEAN,
    fitFactor2                  INTEGER,
    pass2                       BOOLEAN,
    exercise3                   CHAR(64) collate NOCASE,
    maskSample3                 INTEGER,
    exclude3                    BOOLEAN,
    fitFactor3                  INTEGER,
    pass3                       BOOLEAN,
    exercise4                   CHAR(64) collate NOCASE,
    maskSample4                 INTEGER,
    exclude4                    BOOLEAN,
    fitFactor4                  INTEGER,
    pass4                       BOOLEAN,
    exercise5                   CHAR(64) collate NOCASE,
    maskSample5                 INTEGER,
    exclude5                    BOOLEAN,
    fitFactor5                  INTEGER,
    pass5                       BOOLEAN,
    exercise6                   CHAR(64) collate NOCASE,
    maskSample6                 INTEGER,
    exclude6                    BOOLEAN,
    fitFactor6                  INTEGER,
    pass6                       BOOLEAN,
    exercise7                   CHAR(64) collate NOCASE,
    maskSample7                 INTEGER,
    exclude7                    BOOLEAN,
    fitFactor7                  INTEGER,
    pass7                       BOOLEAN,
    exercise8                   CHAR(64) collate NOCASE,
    maskSample8                 INTEGER,
    exclude8                    BOOLEAN,
    fitFactor8                  INTEGER,
    pass8                       BOOLEAN,
    exercise9                   CHAR(64) collate NOCASE,
    maskSample9                 INTEGER,
    exclude9                    BOOLEAN,
    fitFactor9                  INTEGER,
    pass9                       BOOLEAN,
    exercise10                  CHAR(64) collate NOCASE,
    maskSample10                INTEGER,
    exclude10                   BOOLEAN,
    fitFactor10                 INTEGER,
    pass10                      BOOLEAN,
    exercise11                  CHAR(64) collate NOCASE,
    maskSample11                INTEGER,
    exclude11                   BOOLEAN,
    fitFactor11                 INTEGER,
    pass11                      BOOLEAN,
    exercise12                  CHAR(64) collate NOCASE,
    maskSample12                INTEGER,
    exclude12                   BOOLEAN,
    fitFactor12                 INTEGER,
    pass12                      BOOLEAN,
    comfortScore                CHAR(16) collate NOCASE,
    userCompetency              CHAR(16) collate NOCASE,
    comfortValidation           CHAR(16) collate NOCASE,
    fitTestPerformed            BOOLEAN,
    qlftTestSolution            CHAR(32),
    qlftThresholdScreeningValue CHAR(32),
    unique (serialNumber, testDate)
);

create table main.maskRecord
(
        id               INTEGER
        primary key autoincrement,
        maskManufacturer CHAR(64) collate NOCASE not null,
        maskModel        CHAR(64) collate NOCASE not null,
        maskStyle        CHAR(64) collate NOCASE not null,
        approval         CHAR(64) collate NOCASE not null,
        ffPassLevel      INTEGER                 not null,
        n95              BOOLEAN                 not null,
        maskDescription  CHAR(64) collate NOCASE not null,
        autoDescription  BOOLEAN                 not null,
        maskFormFactor   INTEGER,
        unique (maskManufacturer, maskModel, maskStyle) on conflict replace
);

create table main.peopleRecord
(
    id           INTEGER
        primary key autoincrement,
    firstName    CHAR(64) collate NOCASE            not null,
    lastName     CHAR(64) collate NOCASE            not null,
    idNumber     CHAR(64) collate NOCASE            not null,
    company      CHAR(64) collate NOCASE,
    location     CHAR(64) collate NOCASE,
    note         CHAR(128) collate NOCASE,
    custom1Label CHAR(64) collate NOCASE,
    custom1Data  CHAR(64) collate NOCASE,
    custom2Label CHAR(64) collate NOCASE,
    custom2Data  CHAR(64) collate NOCASE,
    custom3Label CHAR(64) collate NOCASE,
    custom3Data  CHAR(64) collate NOCASE,
    custom4Label CHAR(64) collate NOCASE,
    custom4Data  CHAR(64) collate NOCASE,
    inactive     BOOLEAN                  default 0 not null,
    email        CHAR(384) collate NOCASE default NULL,
    unique (firstName, lastName, idNumber) on conflict replace
);

create table main.protocolRecord
(
    id            INTEGER
    primary key autoincrement,
    protocolName  CHAR(64) collate NOCASE not null,
    protocolModel CHAR(64) collate NOCASE not null,
    ambientPurge  INTEGER                 not null,
    ambientSample INTEGER                 not null,
    maskPurge     INTEGER                 not null,
    numExercises  INTEGER                 not null,
    period        CHAR(64)                not null,
    endOnFail     BOOLEAN                 not null,
    algorithm     INTEGER                 not null,
    n95           BOOLEAN                 not null,
    unique (protocolName, protocolModel, n95) on conflict replace
);

create table main.sqlite_master
(
    type     TEXT,
    name     TEXT,
    tbl_name TEXT,
    rootpage INT,
    sql      TEXT
);

create table main.sqlite_sequence
(
    name,
    seq
);

CREATE VIEW dueDateRecord as 
    SELECT 
        FT.lastName as lastName, 
        FT.firstName as firstName, 
        FT.idNumber as idNumber, 
        FT.company as company, 
        FT.maskDescription as maskDescription, 
        max(FT.dueDate) as dueDate, 
        PR.inactive as inactive 
    from fitTestRecord as FT LEFT JOIN peopleRecord as PR on PR.lastName = FT.lastName AND PR.firstName = FT.firstName and PR.idNumber = FT.idNumber 
    GROUP BY FT.lastName, FT.firstName, FT.idNumber, FT.company, FT.maskDescription;

```