export const TopCustomersWithPendingOrdersContract = {
  contract_version: "string",
  generated_at: "datetime",
  data: [
    {
      customer_id: "number",
      customer_name: "string",
      pending_order_count: "number"
    }
  ]
};

export function isValidTopCustomersWithPendingOrders(payload) {
  if (!payload || typeof payload !== "object") return false;
  if (typeof payload.contract_version !== "string") return false;
  if (typeof payload.generated_at !== "string") return false;
  if (!Array.isArray(payload.data)) return false;

  return payload.data.every((row) => {
    return (
      row &&
      typeof row.customer_id === "number" &&
      typeof row.customer_name === "string" &&
      typeof row.pending_order_count === "number"
    );
  });
}
