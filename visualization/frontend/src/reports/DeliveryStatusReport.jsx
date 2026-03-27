import React from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { isValidOrderDeliverySummary } from "../contracts/orderDeliverySummary";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8001";

export default function DeliveryStatusReport() {
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const isFirstLoad = useRef(true);

  const totalOrders = useMemo(
    () => rows.reduce((sum, row) => sum + row.order_count, 0),
    [rows]
  );

  useEffect(() => {
    async function loadData() {
      try {
        if (isFirstLoad.current) setLoading(true);
        setError("");

        const response = await fetch(`${API_URL}/api/v1/order-delivery-summary`);
        if (!response.ok) {
          throw new Error(`Request failed with status ${response.status}`);
        }

        const payload = await response.json();
        if (!isValidOrderDeliverySummary(payload)) {
          throw new Error("API response does not match contract");
        }

        setRows(payload.data);
        setGeneratedAt(payload.generated_at);
        isFirstLoad.current = false;
      } catch (err) {
        setError(err.message || "Unexpected error");
      } finally {
        setLoading(false);
      }
    }

    loadData();
    const interval = setInterval(loadData, 10000);
    return () => clearInterval(interval);
  }, []);

  return (
    <section className="panel">
      <h2>Delivery Status Report</h2>
      <p className="subtext">
        Open orders by delivery date and status | Data contract: <code>/api/v1/order-delivery-summary</code>
      </p>

      <div className="meta">
        <span>Total open orders: {totalOrders}</span>
        <span>
          Generated at:{" "}
          {generatedAt ? new Date(generatedAt).toLocaleString() : "-"}
        </span>
      </div>

      {loading && <div className="state">Loading...</div>}
      {error && <div className="state error">{error}</div>}

      {!loading && !error && (
        <table>
          <thead>
            <tr>
              <th>Delivery Date</th>
              <th>Status</th>
              <th>Order Count</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={3} className="empty">
                  No open orders found.
                </td>
              </tr>
            ) : (
              rows.map((row, idx) => (
                <tr key={`${row.delivery_date}-${row.status}-${idx}`}>
                  <td>{row.delivery_date}</td>
                  <td>{row.status}</td>
                  <td>{row.order_count}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      )}
    </section>
  );
}
