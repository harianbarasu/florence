export function privateReviewSummaryAad(input: {
  householdId: string;
  adultId: string;
  itemKey: string;
}): string {
  return ["florence.private-review-summary.v1", input.householdId, input.adultId, input.itemKey].join("\0");
}
