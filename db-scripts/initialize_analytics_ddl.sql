CREATE SCHEMA IF NOT EXISTS analytical;


CREATE TABLE IF NOT EXISTS analytical.dim_customer (
    customer_id         BIGINT          PRIMARY KEY,
    customer_name       VARCHAR(255)    NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT true,
    customer_address    VARCHAR(500),
    updated_at          TIMESTAMP,
    created_at          TIMESTAMP,
    created_by          BIGINT,
    updated_by          BIGINT,
    _additional_columns TEXT,
    _kafka_offset       BIGINT
);

CREATE TABLE IF NOT EXISTS analytical.dim_customer_history (
    history_id      BIGSERIAL       PRIMARY KEY,
    customer_id         BIGINT       ,
    customer_name       VARCHAR(255)    NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT true,
    customer_address    VARCHAR(500),
    updated_at          TIMESTAMP,
    created_at          TIMESTAMP,
    created_by          BIGINT,
    updated_by          BIGINT,
    recorded_at     TIMESTAMP       DEFAULT NOW(),
    _additional_columns TEXT,
    _kafka_offset       BIGINT
);


CREATE TABLE IF NOT EXISTS analytical.dim_product (
    product_id          BIGINT          PRIMARY KEY,
    product_name        VARCHAR(255)    NOT NULL,
    barcode             VARCHAR(100),
    unity_price         DECIMAL(10, 2)  NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT true,
    updated_at          TIMESTAMP,
    created_at          TIMESTAMP,
    created_by          BIGINT,
    updated_by          BIGINT,
    _additional_columns TEXT,
    _kafka_offset       BIGINT
);


CREATE TABLE IF NOT EXISTS analytical.dim_product_history (
    history_id      BIGSERIAL       PRIMARY KEY,
    product_id          BIGINT          ,
    product_name        VARCHAR(255)    NOT NULL,
    barcode             VARCHAR(100),
    unity_price         DECIMAL(10, 2)  NOT NULL,
    is_active           BOOLEAN         NOT NULL DEFAULT true,
    updated_at          TIMESTAMP,
    created_at          TIMESTAMP,
    created_by          BIGINT,
    updated_by          BIGINT,
    recorded_at     TIMESTAMP       DEFAULT NOW(),
    _additional_columns TEXT,
    _kafka_offset       BIGINT
);

CREATE TABLE IF NOT EXISTS analytical.fact_orders (
    order_id            BIGINT          PRIMARY KEY,
    customer_id         BIGINT          ,
    order_date          DATE            NOT NULL,
    delivery_date       DATE            NOT NULL,
    status              VARCHAR(50)     NOT NULL,
    updated_at          TIMESTAMP,
    created_at          TIMESTAMP,
    _additional_columns TEXT,
    _kafka_offset       BIGINT
);

CREATE TABLE IF NOT EXISTS analytical.fact_orders_history (
    history_id      BIGSERIAL       PRIMARY KEY,
    order_id        BIGINT          NOT NULL,
    customer_id     BIGINT          NOT NULL,
    order_date      DATE            NOT NULL,
    delivery_date   DATE            NOT NULL,
    status          VARCHAR(50)     NOT NULL,
    updated_at      TIMESTAMP,  -- when this change was captured
    created_at      TIMESTAMP,
    recorded_at     TIMESTAMP       DEFAULT NOW(),   -- when this change was captured
    _additional_columns TEXT,
    _kafka_offset   BIGINT
);


CREATE TABLE IF NOT EXISTS analytical.fact_order_items (
    order_item_id       BIGINT          PRIMARY KEY,
    order_id            BIGINT,          
    product_id          BIGINT   ,
    _additional_columns TEXT,
    _kafka_offset       BIGINT,
    quantity            INTEGER         NOT NULL,
    order_status        VARCHAR(50)     NOT NULL,   -- denormalized from fact_orders
    updated_at          TIMESTAMP,
    created_at          TIMESTAMP
);


CREATE TABLE IF NOT EXISTS analytical.dim_customer_staging    (LIKE analytical.dim_customer    INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS analytical.dim_product_staging     (LIKE analytical.dim_product     INCLUDING DEFAULTS);
CREATE TABLE IF NOT EXISTS analytical.fact_orders_staging     (LIKE analytical.fact_orders     INCLUDING DEFAULTS EXCLUDING CONSTRAINTS);
CREATE TABLE IF NOT EXISTS analytical.fact_order_items_staging(LIKE analytical.fact_order_items INCLUDING DEFAULTS EXCLUDING CONSTRAINTS);
