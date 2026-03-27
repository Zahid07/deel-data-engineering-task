export const TopDeliveryDatesContract = {
  contract_version: "string",
  generated_at: "datetime",
  data: [
    {
      delivery_date: "date",
      open_order_count: "number"
    }
  ]
};

export function isValidTopDeliveryDates(payload) {
  if (!payload || typeof payload !== "object") return false;
  if (typeof payload.contract_version !== "string") return false;
  if (typeof payload.generated_at !== "string") return false;
  if (!Array.isArray(payload.data)) return false;

  return payload.data.every((row) => {
    return (
      row &&
      typeof row.delivery_date === "string" &&
      typeof row.open_order_count === "number"
    );
  });
}
