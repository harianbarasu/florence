import { auth } from "@/auth";
import { AppShell } from "@/components/app-shell";
import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";
import { CalendarScreen } from "@/components/calendar/calendar-screen";
import { withToken } from "@/lib/routes";

type SearchParams = Record<string, string | string[] | undefined>;

export default async function CalendarPage({
  searchParams,
}: {
  searchParams?: Promise<SearchParams>;
}) {
  const resolved = (await searchParams) || {};
  const token = typeof resolved.token === "string" ? resolved.token : undefined;
  const session = await auth();

  if (!session?.user?.email) {
    return <GoogleSignInCard redirectTo={withToken("/calendar", token)} />;
  }

  return (
    <AppShell
      currentPath="/calendar"
      userName={session.user.name || session.user.email}
      userEmail={session.user.email}
      token={token}
    >
      <CalendarScreen token={token} />
    </AppShell>
  );
}
