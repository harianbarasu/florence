import { ArrowRight, ShieldCheck, Sparkles } from "lucide-react";
import { signIn } from "@/auth";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";

export function GoogleSignInCard({
  redirectTo,
  title = "Finish Florence setup",
  description = "Sign in once here, then Florence will continue directly into the Google account connection flow for the same household.",
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
    <div className="min-h-screen px-4 py-6 sm:px-6 sm:py-8">
      <div className="mx-auto flex min-h-[calc(100vh-3rem)] max-w-6xl items-center">
        <Card className="grid w-full overflow-hidden border-border/70 lg:grid-cols-[1.08fr_0.92fr]">
          <div className="border-b border-border/70 bg-[linear-gradient(160deg,rgba(28,91,122,0.14),rgba(255,255,255,0.55)_42%,rgba(176,106,51,0.06))] lg:border-b-0 lg:border-r">
            <CardHeader className="gap-5 p-8 sm:p-10">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant="secondary">Florence</Badge>
                <Badge variant="outline">Web control plane</Badge>
              </div>
              <div className="inline-flex w-fit items-center gap-2 rounded-full bg-white/80 px-3 py-1 text-[11px] font-semibold uppercase tracking-[0.18em] text-primary">
                <Sparkles className="h-3.5 w-3.5" />
                Setup flow
              </div>
              <CardTitle className="max-w-xl text-3xl leading-tight sm:text-4xl">
                Sign in once, then Florence carries the rest of onboarding forward.
              </CardTitle>
              <CardDescription className="max-w-2xl text-base leading-7">
                The control plane handles sign-in, Google account consent, and household setup in sequence. iMessage
                remains the primary product once the initial sync is done.
              </CardDescription>
            </CardHeader>
            <CardContent className="grid gap-4 px-8 pb-10 sm:px-10">
              <div className="rounded-[1.5rem] border border-border/70 bg-white/75 p-5">
                <div className="mb-3 text-sm font-semibold">What happens next</div>
                <div className="grid gap-3 text-sm leading-6 text-muted-foreground">
                  <div>1. Secure Google sign-in for the web app.</div>
                  <div>2. Immediate handoff into Gmail and Calendar consent.</div>
                  <div>3. Back to Florence for sync progress and household details.</div>
                </div>
              </div>
            </CardContent>
          </div>

          <div className="flex items-center bg-[linear-gradient(180deg,rgba(255,255,255,0.76),rgba(255,255,255,0.92))]">
            <div className="w-full p-8 sm:p-10">
              <div className="mb-6 inline-flex items-center gap-2 rounded-full bg-emerald-50 px-3 py-1 text-xs font-semibold text-emerald-800">
                <ShieldCheck className="h-3.5 w-3.5" />
                Secure Google session
              </div>
              <h1 className="mb-3 text-2xl font-semibold">{title}</h1>
              <p className="mb-8 text-sm leading-6 text-muted-foreground">{description}</p>
              <form action={startGoogleSignIn} className="grid gap-3">
                <Button className="w-full" size="lg">
                  Continue with Google
                  <ArrowRight className="h-4 w-4" />
                </Button>
                <p className="text-center text-xs leading-5 text-muted-foreground">
                  Florence uses this identity for web access, then immediately continues into the Google account
                  connection needed for Gmail and Calendar sync.
                </p>
              </form>
            </div>
          </div>
        </Card>
      </div>
    </div>
  );
}
