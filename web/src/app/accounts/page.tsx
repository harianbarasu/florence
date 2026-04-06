import { redirect } from "next/navigation";
import { withToken } from "@/lib/routes";

type SearchParams = Record<string, string | string[] | undefined>;

export default async function AccountsPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const resolved = (await searchParams) || {};
  const token = typeof resolved.token === "string" ? resolved.token : undefined;
  redirect(withToken("/connections", token));
}
