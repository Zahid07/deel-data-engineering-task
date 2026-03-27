export const PendingItemsByProductContract = {
  contract_version: "string",
  generated_at: "datetime",
  data: [
    {
      product_id: "number",
      product_name: "string | null",
      pending_quantity: "number",
      pending_item_count: "number"
    }
  ]
};

export function isValidPendingItemsByProduct(payload) {
  if (!payload || typeof payload !== "object") return false;
  if (typeof payload.contract_version !== "string") return false;
  if (typeof payload.generated_at !== "string") return false;
  if (!Array.isArray(payload.data)) return false;

  return payload.data.every((row) => {
    return (
      row &&
      typeof row.product_id === "number" &&
      (row.product_name === null || typeof row.product_name === "string") &&
      typeof row.pending_quantity === "number" &&
      typeof row.pending_item_count === "number"
    );
  });
}
