import React from "react";
import { useEffect, useState } from "react";
import { isValidPendingItemsByProduct } from "./contracts/pendingItemsByProduct";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8001";

export function PendingItemsByProduct() {
  const [rows, setRows] = useState([]);
  const [generatedAt, setGeneratedAt] = useState("");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");

  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        setError("");

        console.log("Fetching from:", `${API_URL}/api/v1/pending-items-by-product`);
        const response = await fetch(`${API_URL}/api/v1/pending-items-by-product`);
        console.log("Response status:", response.status);
        if (!response.ok) {
          throw new Error(`Request failed with status ${response.status}`);
        }

        const payload = await response.json();
        console.log("Payload received:", payload);
        if (!isValidPendingItemsByProduct(payload)) {
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
      <h2>Pending Items by Product</h2>
      <p className="subtext">
        Number of items pending fulfillment | Data contract: <code>/api/v1/pending-items-by-product</code>
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
              <th>Product ID</th>
              <th>Product Name</th>
              <th>Pending Quantity</th>
              <th>Item Count</th>
            </tr>
          </thead>
          <tbody>
            {rows.length === 0 ? (
              <tr>
                <td colSpan={4} className="empty">
                  No pending items found.
                </td>
              </tr>
            ) : (
              rows.map((row, idx) => (
                <tr key={`${row.product_id}-${idx}`}>
                  <td>{row.product_id}</td>
                  <td>{row.product_name || "-"}</td>
                  <td>{row.pending_quantity}</td>
                  <td>{row.pending_item_count}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      )}
    </section>
  );
}
