import React from "react";
import { useEffect, useState } from "react";
import { isValidTopDeliveryDates } from "./contracts/topDeliveryDates";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8001";

export function TopDeliveryDates() {
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        setError("");

        console.log("Fetching from:", `${API_URL}/api/v1/top-delivery-dates`);
        const response = await fetch(`${API_URL}/api/v1/top-delivery-dates`);
        console.log("Response status:", response.status);
        if (!response.ok) {
          throw new Error(`Request failed with status ${response.status}`);
        }

        const payload = await response.json();
        console.log("Payload received:", payload);
        if (!isValidTopDeliveryDates(payload)) {
          throw new Error("API response does not match contract");
        }

        setRows(payload.data);
        setGeneratedAt(payload.generated_at);
      } catch (err) {
        console.error("Error loading data:", err);
        setError(err.message || "Unexpected error");
      } finally {
        setLoading(false);
      }
    }

    loadData();

    // Poll every 10 seconds — matches Spark micro-batch trigger interval
    const interval = setInterval(loadData, 10000);

    // Cleanup on unmount
    return () => clearInterval(interval);
  }, []);

  return (
    <section className="panel">
      <h2>Top 3 Delivery Dates with Most Open Orders</h2>
      <p className="subtext">
        Ranked by number of open orders | Data contract: <code>/api/v1/top-delivery-dates</code>
      </p>

      <div className="meta">
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
              <th>Rank</th>
              <th>Delivery Date</th>
              <th>Open Order Count</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={3} className="empty">
                  No data available.
                </td>
              </tr>
            ) : (
              rows.map((row, idx) => (
                <tr key={`${row.delivery_date}-${idx}`}>
                  <td>{idx + 1}</td>
                  <td>{row.delivery_date}</td>
                  <td>{row.open_order_count}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      )}
    </section>
  );
}
