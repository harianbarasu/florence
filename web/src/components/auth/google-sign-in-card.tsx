import { ArrowRight, Sparkles } from "lucide-react";
import { signIn } from "@/auth";
import { Button } from "@/components/ui/button";

export function GoogleSignInCard({
  redirectTo,
}: {
  redirectTo: string;
}) {
  async function startGoogleSignIn() {
    "use server";
    await signIn("google", { redirectTo });
  }

  return (
    <div className="flex min-h-screen flex-col items-center px-4 py-12 sm:py-16">
      <div className="mb-10 flex items-center gap-2.5">
        <div className="flex h-9 w-9 items-center justify-center rounded-xl bg-primary text-primary-foreground">
          <Sparkles className="h-4.5 w-4.5" />
        </div>
        <span className="text-lg font-semibold">Florence</span>
      </div>
      <div className="w-full max-w-md">
        <div className="flex flex-col items-center gap-6 text-center">
          <div>
            <h1 className="text-xl font-semibold">Sign in to get started</h1>
            <p className="mt-2 text-sm leading-relaxed text-muted-foreground">
              Florence needs your Google account to connect Gmail and Calendar
              for your household.
            </p>
          </div>
          <form action={startGoogleSignIn} className="w-full">
            <Button size="lg" className="w-full">
              Continue with Google
              <ArrowRight className="h-4 w-4" />
            </Button>
          </form>
        </div>
      </div>
    </div>
  );
}
