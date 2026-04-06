import { auth } from "@/auth";
import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";
import { AppShell } from "@/components/app-shell";
import { SetupScreen } from "@/components/setup/setup-screen";
import { withToken } from "@/lib/routes";

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
    const redirectTo = withToken("/setup", token);
    return <GoogleSignInCard redirectTo={redirectTo} />;
  }

  return (
    <AppShell
      currentPath="/setup"
      userName={session.user.name || session.user.email}
      userEmail={session.user.email}
      token={token}
    >
      <SetupScreen token={token} />
    </AppShell>
  );
}
