import { auth } from "@/auth";
import { AppShell } from "@/components/app-shell";
import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";
import { AccountsScreen } from "@/components/accounts/accounts-screen";

type SearchParams = Record<string, string | string[] | undefined>;

export default async function AccountsPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const resolved = (await searchParams) || {};
  const token = typeof resolved.token === "string" ? resolved.token : undefined;
  const session = await auth();

  if (!session?.user?.email) {
    return <GoogleSignInCard redirectTo="/accounts" title="Sign in to manage Florence accounts" />;
  }

  return (
    <AppShell
      currentPath="/accounts"
      userName={session.user.name || session.user.email}
      userEmail={session.user.email}
    >
      <AccountsScreen token={token} />
    </AppShell>
  );
}
