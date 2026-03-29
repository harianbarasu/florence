import { auth } from "@/auth";
import { AppShell } from "@/components/app-shell";
import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";
import { SettingsScreen } from "@/components/settings/settings-screen";

type SearchParams = Record<string, string | string[] | undefined>;

export default async function SettingsPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const resolved = (await searchParams) || {};
  const token = typeof resolved.token === "string" ? resolved.token : undefined;
  const session = await auth();

  if (!session?.user?.email) {
    return <GoogleSignInCard redirectTo="/settings" title="Sign in to manage Florence settings" />;
  }

  return (
    <AppShell
      currentPath="/settings"
      userName={session.user.name || session.user.email}
      userEmail={session.user.email}
    >
      <SettingsScreen token={token} />
    </AppShell>
  );
}
