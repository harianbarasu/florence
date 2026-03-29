import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";

type SearchParams = Record<string, string | string[] | undefined>;

export default async function LoginPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const resolved = (await searchParams) || {};
  const next = typeof resolved.next === "string" ? resolved.next : "/setup";
  return <GoogleSignInCard redirectTo={next} title="Sign in to Florence web" />;
}
