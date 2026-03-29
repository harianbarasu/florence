import { ArrowRight, ShieldCheck } from "lucide-react";
import { signIn } from "@/auth";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

export function GoogleSignInCard({
  redirectTo,
  title = "Continue with Google",
  description = "Florence uses Google sign-in for the web control plane. Your first signed-in Google account also becomes your first synced Florence account.",
}: {
  redirectTo: string;
  title?: string;
  description?: string;
}) {
  async function startGoogleSignIn() {
    "use server";
    await signIn("google", { redirectTo });
  }

  return (
    <div className="mx-auto flex min-h-screen w-full max-w-5xl items-center px-4 py-10 sm:px-6">
      <Card className="grid w-full overflow-hidden border-border/70 lg:grid-cols-[1.1fr_0.9fr]">
        <div className="border-b border-border/70 bg-[linear-gradient(180deg,rgba(28,91,122,0.1),rgba(28,91,122,0.03))] lg:border-b-0 lg:border-r">
          <CardHeader className="gap-4 p-8 sm:p-10">
            <div className="inline-flex w-fit items-center gap-2 rounded-full border border-primary/15 bg-white/80 px-3 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-primary">
              Florence setup
            </div>
            <CardTitle className="max-w-lg text-3xl leading-tight sm:text-4xl">
              The chat stays in iMessage. Setup and account management happen here.
            </CardTitle>
            <CardDescription className="max-w-xl text-base leading-7">
              Connect the first Google account, let Florence finish the initial sync, then fill in kids, schools, and
              activities so household matching gets sharper immediately.
            </CardDescription>
          </CardHeader>
          <CardContent className="grid gap-4 px-8 pb-10 sm:px-10">
            <div className="rounded-[1.25rem] border border-border/70 bg-white/70 p-4 text-sm leading-6 text-muted-foreground">
              Florence will send you back to iMessage when setup is complete. If you save this page, you can come back
              later for accounts, settings, and billing.
            </div>
          </CardContent>
        </div>
        <div className="flex items-center">
          <div className="w-full p-8 sm:p-10">
            <div className="mb-6 inline-flex items-center gap-2 rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-800">
              <ShieldCheck className="h-3.5 w-3.5" />
              Secure Google session
            </div>
            <h1 className="mb-3 text-2xl font-semibold">{title}</h1>
            <p className="mb-8 text-sm leading-6 text-muted-foreground">{description}</p>
            <form action={startGoogleSignIn}>
              <Button className="w-full" size="lg">
                Continue with Google
                <ArrowRight className="h-4 w-4" />
              </Button>
            </form>
          </div>
        </div>
      </Card>
    </div>
  );
}
