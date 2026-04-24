/* =========================================================================
   OVB Analytics — Data Warehouse Schema (T-SQL, MS SQL Server)
   -------------------------------------------------------------------------
   Star schema: one central FACT table surrounded by DIMENSION tables.
     fact_contracts     -> "what happened" (a contract was signed)
     dim_adviser        -> "who sold it"
     dim_client         -> "to whom"
     dim_product        -> "what product"
     dim_date           -> "when"

   Why a star schema? In a warehouse you optimise for READING (reports,
   dashboards), not for writing. Denormalising the surrounding context into
   wide dimension tables means reports join the fact to each dimension
   exactly once — fast and easy for analysts to write.
   ========================================================================= */


/* -------------------------------------------------------------------------
   1. Create (and switch to) the database
   ------------------------------------------------------------------------- */
IF DB_ID('OVBAnalytics') IS NULL
    CREATE DATABASE OVBAnalytics;
GO

USE OVBAnalytics;
GO


/* -------------------------------------------------------------------------
   2. Drop existing tables (so this script is re-runnable).
      Drop the FACT first because it has foreign keys INTO the dimensions.
   ------------------------------------------------------------------------- */
IF OBJECT_ID('dbo.fact_contracts', 'U') IS NOT NULL DROP TABLE dbo.fact_contracts;
IF OBJECT_ID('dbo.dim_adviser',    'U') IS NOT NULL DROP TABLE dbo.dim_adviser;
IF OBJECT_ID('dbo.dim_client',     'U') IS NOT NULL DROP TABLE dbo.dim_client;
IF OBJECT_ID('dbo.dim_product',    'U') IS NOT NULL DROP TABLE dbo.dim_product;
IF OBJECT_ID('dbo.dim_date',       'U') IS NOT NULL DROP TABLE dbo.dim_date;
GO


/* -------------------------------------------------------------------------
   3. dim_adviser — the financial advisers (Vermögensberater*innen)
   -------------------------------------------------------------------------
   adviser_sk   : SURROGATE KEY — a meaningless auto-generated integer used
                  as the primary key. Warehouses prefer surrogate keys over
                  "natural" business keys because natural keys (like an
                  employee number) can change or be reused over time.
   adviser_id   : the BUSINESS KEY — how the source system identifies this
                  adviser. Kept for traceability back to the source.
   ------------------------------------------------------------------------- */
CREATE TABLE dbo.dim_adviser (
    adviser_sk        INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    adviser_id        NVARCHAR(20)      NOT NULL UNIQUE,
    first_name        NVARCHAR(50)      NOT NULL,
    last_name         NVARCHAR(50)      NOT NULL,
    region            NVARCHAR(50)      NOT NULL,   -- e.g. Köln, Hamburg, Berlin
    experience_level  NVARCHAR(20)      NOT NULL,   -- Junior / Mid / Senior
    hire_date         DATE              NOT NULL,
    is_active         BIT               NOT NULL DEFAULT 1
);
GO


/* -------------------------------------------------------------------------
   4. dim_client — OVB's customers
   -------------------------------------------------------------------------
   NVARCHAR vs VARCHAR: NVARCHAR stores Unicode (2 bytes/char), so it
   handles German umlauts (ä, ö, ü, ß) and international names safely.
   Use NVARCHAR for anything human-readable in a European business.
   ------------------------------------------------------------------------- */
CREATE TABLE dbo.dim_client (
    client_sk            INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    client_id            NVARCHAR(20)      NOT NULL UNIQUE,
    first_name           NVARCHAR(50)      NOT NULL,
    last_name            NVARCHAR(50)      NOT NULL,
    date_of_birth        DATE              NOT NULL,
    gender               NVARCHAR(10)      NULL,    -- nullable; not everyone discloses
    city                 NVARCHAR(80)      NOT NULL,
    country              NVARCHAR(50)      NOT NULL DEFAULT 'Germany',
    acquisition_channel  NVARCHAR(30)      NOT NULL -- Referral / Web / Partner / Event
);
GO


/* -------------------------------------------------------------------------
   5. dim_product — financial products sold by OVB advisers
   ------------------------------------------------------------------------- */
CREATE TABLE dbo.dim_product (
    product_sk        INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    product_id        NVARCHAR(20)      NOT NULL UNIQUE,
    product_name      NVARCHAR(100)     NOT NULL,
    product_category  NVARCHAR(40)      NOT NULL,   -- Insurance / Investment / Pension / Mortgage
    provider          NVARCHAR(80)      NOT NULL,   -- e.g. Allianz, DWS, Generali
    risk_level        NVARCHAR(20)      NOT NULL    -- Low / Medium / High
);
GO


/* -------------------------------------------------------------------------
   6. dim_date — the classic date dimension
   -------------------------------------------------------------------------
   Why a dedicated date dimension instead of just using a DATE column?
     - You can pre-compute attributes once (quarter, is_weekend, fiscal_year)
       and every report can just SELECT them — no repeated date maths.
     - Reports can easily filter / group by "last quarter", "weekends only",
       "month-ends only" etc. without every analyst re-deriving the logic.
     - date_sk is conventionally an INT in YYYYMMDD form (e.g. 20260423),
       which is both human-readable and sorts chronologically.
   ------------------------------------------------------------------------- */
CREATE TABLE dbo.dim_date (
    date_sk       INT         NOT NULL PRIMARY KEY,   -- YYYYMMDD
    full_date     DATE        NOT NULL UNIQUE,
    day_num       TINYINT     NOT NULL,               -- 1..31
    month_num     TINYINT     NOT NULL,               -- 1..12
    month_name    NVARCHAR(15) NOT NULL,
    quarter_num   TINYINT     NOT NULL,               -- 1..4
    year_num      SMALLINT    NOT NULL,
    day_of_week   TINYINT     NOT NULL,               -- 1=Mon .. 7=Sun
    day_name      NVARCHAR(15) NOT NULL,
    is_weekend    BIT         NOT NULL,
    is_month_end  BIT         NOT NULL
);
GO


/* -------------------------------------------------------------------------
   7. fact_contracts — the central fact table
   -------------------------------------------------------------------------
   Grain: one row per signed contract.
   Every fact table has two kinds of columns:
     - FOREIGN KEYS to the dimensions  ("the context")
     - MEASURES (numbers you aggregate) ("the numbers")

   DECIMAL(18,2) is the standard money type in T-SQL — 16 digits before the
   decimal, 2 after. Avoid FLOAT for money; floating-point rounding causes
   audit nightmares.
   ------------------------------------------------------------------------- */
CREATE TABLE dbo.fact_contracts (
    contract_sk        BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    contract_id        NVARCHAR(30)         NOT NULL UNIQUE,

    -- Foreign keys ("who / what / when")
    adviser_sk         INT                  NOT NULL,
    client_sk          INT                  NOT NULL,
    product_sk         INT                  NOT NULL,
    signing_date_sk    INT                  NOT NULL,

    -- Measures ("the numbers")
    contract_value     DECIMAL(18,2)        NOT NULL,   -- total notional / sum insured
    commission         DECIMAL(18,2)        NOT NULL,   -- adviser's commission
    premium_monthly    DECIMAL(18,2)        NULL,       -- NULL for one-off products
    duration_months    SMALLINT             NOT NULL,
    status             NVARCHAR(20)         NOT NULL,   -- Active / Cancelled / Completed

    CONSTRAINT fk_fact_adviser  FOREIGN KEY (adviser_sk)      REFERENCES dbo.dim_adviser(adviser_sk),
    CONSTRAINT fk_fact_client   FOREIGN KEY (client_sk)       REFERENCES dbo.dim_client(client_sk),
    CONSTRAINT fk_fact_product  FOREIGN KEY (product_sk)      REFERENCES dbo.dim_product(product_sk),
    CONSTRAINT fk_fact_date     FOREIGN KEY (signing_date_sk) REFERENCES dbo.dim_date(date_sk)
);
GO


/* -------------------------------------------------------------------------
   8. Indexes on the fact table's foreign keys
   -------------------------------------------------------------------------
   SQL Server automatically indexes PRIMARY KEYs and UNIQUE columns, but
   NOT foreign keys. In a warehouse, nearly every report joins the fact to
   the dimensions on these FK columns, so indexing them is a huge win.
   ------------------------------------------------------------------------- */
CREATE INDEX ix_fact_adviser  ON dbo.fact_contracts(adviser_sk);
CREATE INDEX ix_fact_client   ON dbo.fact_contracts(client_sk);
CREATE INDEX ix_fact_product  ON dbo.fact_contracts(product_sk);
CREATE INDEX ix_fact_date     ON dbo.fact_contracts(signing_date_sk);
GO

PRINT 'Schema created successfully.';
GO
