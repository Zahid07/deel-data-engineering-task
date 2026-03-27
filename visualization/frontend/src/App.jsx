import React from "react";
import { useState } from "react";
import { isValidOrderDeliverySummary } from "./contracts/orderDeliverySummary";
import { TopDeliveryDates } from "./TopDeliveryDates";
import { PendingItemsByProduct } from "./PendingItemsByProduct";
import { TopCustomersWithPendingOrders } from "./TopCustomersWithPendingOrders";
import DeliveryStatusReport from "./reports/DeliveryStatusReport";

function App() {
  const [selectedReport, setSelectedReport] = useState("delivery-status");

  const reports = [
    {
      id: "delivery-status",
      label: "📦 Delivery Status",
      component: <DeliveryStatusReport key="delivery-status" />,
    },
    {
      id: "top-delivery-dates",
      label: "📍 Top Delivery Dates",
      component: <TopDeliveryDates key="top-delivery-dates" />,
    },
    {
      id: "pending-items",
      label: "⏳ Pending Items",
      component: <PendingItemsByProduct key="pending-items" />,
    },
    {
      id: "top-customers",
      label: "👥 Top Customers",
      component: <TopCustomersWithPendingOrders key="top-customers" />,
    },
  ];

  return (
    <main className="page">
      <div className="navbar">
        <h1 className="navbar-title">📊 Order Analytics Dashboard</h1>
        <div className="button-group">
          {reports.map((report) => (
            <button
              key={report.id}
              className={`nav-button ${selectedReport === report.id ? "active" : ""}`}
              onClick={() => setSelectedReport(report.id)}
            >
              {report.label}
            </button>
          ))}
        </div>
      </div>

      <div className="report-container">
        {reports.find((r) => r.id === selectedReport)?.component}
      </div>
    </main>
  );
}

export default App;