import { auth } from "@/auth";
import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";
import { OnboardingWizard } from "@/components/setup/onboarding-wizard";

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
    return <GoogleSignInCard redirectTo={redirectTo} />;
  }

  return (
    <OnboardingWizard
      token={token}
      userName={session.user.name || undefined}
    />
  );
}
