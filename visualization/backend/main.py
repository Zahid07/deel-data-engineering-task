import os
from datetime import datetime, date
from typing import List

import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


QUERY = """
SELECT
    delivery_date,
    status,
    COUNT(order_id) AS order_count
FROM analytical.fact_orders
WHERE status NOT IN ('COMPLETED')
GROUP BY delivery_date, status
ORDER BY delivery_date, status;
"""

TOP_DELIVERY_DATES_QUERY = """
SELECT
    delivery_date,
    COUNT(order_id) AS open_order_count
FROM analytical.fact_orders
WHERE status NOT IN ('COMPLETED')
GROUP BY delivery_date
ORDER BY open_order_count DESC
LIMIT 3;
"""

PENDING_ITEMS_BY_PRODUCT_QUERY = """
SELECT
    oi.product_id,
    p.product_name,
    SUM(
        COALESCE(
            (oi._additional_columns::json->>'quanity')::INTEGER,
            oi.quantity,
            0
        )
    ) AS pending_quantity,
    COUNT(oi.order_item_id) AS pending_item_count
FROM analytical.fact_order_items oi
LEFT JOIN analytical.fact_orders o ON o.order_id = oi.order_id
LEFT JOIN analytical.dim_product p ON p.product_id = oi.product_id
WHERE o.status = 'PENDING'
GROUP BY oi.product_id, p.product_name
ORDER BY pending_quantity DESC;
"""

TOP_CUSTOMERS_WITH_PENDING_ORDERS_QUERY = """
SELECT
    o.customer_id,
    c.customer_name,
    COUNT(o.order_id) AS pending_order_count
FROM analytical.fact_orders o
JOIN analytical.dim_customer c ON c.customer_id = o.customer_id
WHERE o.status = 'PENDING'
GROUP BY o.customer_id, c.customer_name
ORDER BY pending_order_count DESC
LIMIT 3;
"""


class OrderDeliverySummaryItem(BaseModel):
    delivery_date: date = Field(description="Delivery date")
    status: str = Field(description="Order status")
    order_count: int = Field(description="Number of orders for the date/status")


class OrderDeliverySummaryResponse(BaseModel):
    contract_version: str = Field(default="1.0.0")
    generated_at: datetime
    data: List[OrderDeliverySummaryItem]


class TopDeliveryDateItem(BaseModel):
    delivery_date: date = Field(description="Delivery date")
    open_order_count: int = Field(description="Number of open orders for the date")


class TopDeliveryDatesResponse(BaseModel):
    contract_version: str = Field(default="1.0.0")
    generated_at: datetime
    data: List[TopDeliveryDateItem]


class PendingItemsByProductItem(BaseModel):
    product_id: int = Field(description="Product ID")
    product_name: str | None = Field(description="Product name")
    pending_quantity: int = Field(description="Total pending quantity")
    pending_item_count: int = Field(description="Number of pending order items")


class PendingItemsByProductResponse(BaseModel):
    contract_version: str = Field(default="1.0.0")
    generated_at: datetime
    data: List[PendingItemsByProductItem]


class TopCustomerWithPendingOrdersItem(BaseModel):
    customer_id: int = Field(description="Customer ID")
    customer_name: str = Field(description="Customer name")
    pending_order_count: int = Field(description="Number of pending orders")


class TopCustomersWithPendingOrdersResponse(BaseModel):
    contract_version: str = Field(default="1.0.0")
    generated_at: datetime
    data: List[TopCustomerWithPendingOrdersItem]


app = FastAPI(title="Order Delivery Summary API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return psycopg2.connect(database_url)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/api/v1/order-delivery-summary", response_model=OrderDeliverySummaryResponse)
def get_order_delivery_summary():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(QUERY)
                rows = cur.fetchall()

        data = [
            OrderDeliverySummaryItem(
                delivery_date=row[0],
                status=row[1],
                order_count=row[2],
            )
            for row in rows
        ]

        return OrderDeliverySummaryResponse(
            generated_at=datetime.utcnow(),
            data=data,
        )

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load summary: {exc}")


@app.get("/api/v1/top-delivery-dates", response_model=TopDeliveryDatesResponse)
def get_top_delivery_dates():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(TOP_DELIVERY_DATES_QUERY)
                rows = cur.fetchall()

        data = [
            TopDeliveryDateItem(
                delivery_date=row[0],
                open_order_count=row[1],
            )
            for row in rows
        ]

        return TopDeliveryDatesResponse(
            generated_at=datetime.utcnow(),
            data=data,
        )

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load top delivery dates: {exc}")


@app.get("/api/v1/pending-items-by-product", response_model=PendingItemsByProductResponse)
def get_pending_items_by_product():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(PENDING_ITEMS_BY_PRODUCT_QUERY)
                rows = cur.fetchall()

        data = [
            PendingItemsByProductItem(
                product_id=row[0],
                product_name=row[1],
                pending_quantity=row[2],
                pending_item_count=row[3],
            )
            for row in rows
        ]

        return PendingItemsByProductResponse(
            generated_at=datetime.utcnow(),
            data=data,
        )

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load pending items: {exc}")


@app.get("/api/v1/top-customers-pending-orders", response_model=TopCustomersWithPendingOrdersResponse)
def get_top_customers_with_pending_orders():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(TOP_CUSTOMERS_WITH_PENDING_ORDERS_QUERY)
                rows = cur.fetchall()

        data = [
            TopCustomerWithPendingOrdersItem(
                customer_id=row[0],
                customer_name=row[1],
                pending_order_count=row[2],
            )
            for row in rows
        ]

        return TopCustomersWithPendingOrdersResponse(
            generated_at=datetime.utcnow(),
            data=data,
        )

    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load top customers: {exc}")
