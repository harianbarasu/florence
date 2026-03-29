import { auth } from "@/auth";
import { AppShell } from "@/components/app-shell";
import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";
import { SetupScreen } from "@/components/setup/setup-screen";

type SearchParams = Record<string, string | string[] | undefined>;

export default async function SetupPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const resolved = (await searchParams) || {};
  const token = typeof resolved.token === "string" ? resolved.token : undefined;
  const session = await auth();

  if (!session?.user?.email) {
    const redirectTo = token ? `/setup?token=${encodeURIComponent(token)}` : "/setup";
    return <GoogleSignInCard redirectTo={redirectTo} title="Finish Florence setup" />;
  }

  return (
    <AppShell
      currentPath="/setup"
      userName={session.user.name || session.user.email}
      userEmail={session.user.email}
    >
      <SetupScreen token={token} />
    </AppShell>
  );
}
