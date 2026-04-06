import Link from "next/link";
import {
  ArrowRight,
  CalendarClock,
  CheckCheck,
  HeartHandshake,
  Mail,
  NotebookPen,
  ShoppingBasket,
  Sparkles,
  Users,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";

const scenarioGroups = [
  {
    title: "School and schedule chaos",
    eyebrow: "Inbox to action",
    icon: Mail,
    summary:
      "Florence reads the school noise, turns it into a plan, and keeps both adults aligned without making anyone forward emails all night.",
    messages: [
      {
        from: "parent",
        text: "Anything important from school today?",
      },
      {
        from: "florence",
        text: "Yes. Early dismissal Friday at 12:30, class photo money due Monday, and baseball registration closes tonight. I added the dismissal to the family plan and can remind both of you tomorrow morning.",
      },
    ],
  },
  {
    title: "Morning coordination",
    eyebrow: "Who is doing what",
    icon: Users,
    summary:
      "Florence keeps pickups, dropoffs, practices, and calendar conflicts from living entirely in one person’s head.",
    messages: [
      {
        from: "parent",
        text: "Can you sanity check tomorrow morning?",
      },
      {
        from: "florence",
        text: "Tomorrow is tight. Violet has dentist at 8:15, Jackson has a 9am work block, and pickup coverage is still missing for soccer. I’d have Kendall handle dropoff and I can text a reminder at 7.",
      },
    ],
  },
  {
    title: "Lists, meals, and home admin",
    eyebrow: "The invisible work",
    icon: ShoppingBasket,
    summary:
      "Florence is not just a calendar bot. It can remember household needs, prep lists, and keep the day-to-day admin from disappearing.",
    messages: [
      {
        from: "parent",
        text: "Add what we need for class snacks and next week’s dinners.",
      },
      {
        from: "florence",
        text: "Added clementines, crackers, yogurt tubes, taco fixings, pasta, and fruit for lunches. I also noted that snack sign-up is Wednesday so I can remind you before the store run.",
      },
    ],
  },
  {
    title: "Travel, forms, and deadlines",
    eyebrow: "Nothing slips",
    icon: NotebookPen,
    summary:
      "Florence remembers the paperwork, reservations, camp windows, and little deadlines that usually get rediscovered too late.",
    messages: [
      {
        from: "parent",
        text: "What’s hanging over us this week?",
      },
      {
        from: "florence",
        text: "Camp deposit due Thursday, passport photo appointment still needs booking, and the birthday RSVP is unanswered. Want me to make a short plan for tonight after dinner?",
      },
    ],
  },
  {
    title: "Weekly family planning",
    eyebrow: "Proactive check-ins",
    icon: CalendarClock,
    summary:
      "Florence should not just answer questions. It should proactively surface what is coming, what is risky, and where coverage is missing.",
    messages: [
      {
        from: "florence",
        text: "Heads up for next week: 3 school events, 2 practice conflicts, one doctor appointment, and Thursday still has no pickup owner. The busiest day is Wednesday. Want the short version or the full plan?",
      },
      {
        from: "parent",
        text: "Short version.",
      },
    ],
  },
  {
    title: "Emotional load and memory",
    eyebrow: "Second brain",
    icon: HeartHandshake,
    summary:
      "The product promise is that Florence remembers the household context over time, not just the current thread.",
    messages: [
      {
        from: "parent",
        text: "I’m overloaded. What am I forgetting?",
      },
      {
        from: "florence",
        text: "A few things are still open: swim waiver, teacher gift idea for Friday, and rescheduling the HVAC visit. The most time-sensitive is the swim waiver. Want me to break tonight into a 20-minute plan?",
      },
    ],
  },
] as const;

const capabilityPillars = [
  {
    title: "Text-first household manager",
    description:
      "Daily use should happen in text. You dump in what you are carrying, and Florence captures, organizes, remembers, and follows through.",
    icon: Sparkles,
  },
  {
    title: "Memory across the week",
    description:
      "Not a stateless chat. Florence should remember your kids, schools, routines, pending tasks, and the household context that makes future replies better.",
    icon: CheckCheck,
  },
  {
    title: "Web as support surface",
    description:
      "Calendar, review, and connections live on the web, but the web app supports the text product instead of replacing it.",
    icon: CalendarClock,
  },
] as const;

function Bubble({ from, text }: { from: "parent" | "florence"; text: string }) {
  const mine = from === "parent";
  return (
    <div className={`flex ${mine ? "justify-end" : "justify-start"}`}>
      <div
        className={[
          "max-w-[82%] rounded-[1.4rem] px-4 py-3 text-sm leading-6 shadow-sm",
          mine
            ? "bg-primary text-primary-foreground"
            : "border border-border/80 bg-card text-card-foreground",
        ].join(" ")}
      >
        {text}
      </div>
    </div>
  );
}

export function ExplorePage() {
  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(28,91,122,0.14),_transparent_34%),radial-gradient(circle_at_bottom_right,_rgba(176,106,51,0.14),_transparent_28%),linear-gradient(180deg,_#f8f4ec_0%,_#f4ede1_100%)] text-foreground">
      <div className="mx-auto flex w-full max-w-7xl flex-col px-5 py-6 sm:px-8 lg:px-10">
        <header className="flex items-center justify-between gap-4 py-4">
          <Link href="/" className="flex items-center gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary text-primary-foreground shadow-sm">
              <Sparkles className="h-5 w-5" />
            </div>
            <div>
              <div className="text-lg font-semibold">Florence</div>
              <div className="text-xs text-muted-foreground">Household manager over text</div>
            </div>
          </Link>
          <div className="flex items-center gap-3">
            <Button asChild variant="ghost">
              <Link href="/login">Sign in</Link>
            </Button>
            <Button asChild>
              <Link href="/login">
                Start with Google
                <ArrowRight className="h-4 w-4" />
              </Link>
            </Button>
          </div>
        </header>

        <main className="grid gap-10 py-8 sm:py-12">
          <section className="grid gap-8 lg:grid-cols-[minmax(0,1.05fr)_minmax(340px,0.95fr)] lg:items-center">
            <div className="space-y-6">
              <Badge variant="secondary" className="w-fit">
                Text-first household management
              </Badge>
              <div className="space-y-4">
                <h1 className="max-w-4xl text-5xl font-semibold tracking-tight sm:text-6xl">
                  The family&apos;s second brain for everything that keeps a household running.
                </h1>
                <p className="max-w-2xl text-lg leading-8 text-muted-foreground">
                  Florence reads the school noise, remembers the household context, keeps the calendar honest, helps with lists and planning, and follows through over text so less of the family operation lives in one person&apos;s head.
                </p>
              </div>
              <div className="flex flex-wrap gap-3">
                <Button asChild size="lg">
                  <Link href="/login">
                    Start with Google
                    <ArrowRight className="h-4 w-4" />
                  </Link>
                </Button>
                <Button asChild size="lg" variant="outline">
                  <Link href="#explore">Explore real scenarios</Link>
                </Button>
              </div>
              <div className="grid gap-4 sm:grid-cols-3">
                {capabilityPillars.map((pillar) => {
                  const Icon = pillar.icon;
                  return (
                    <Card key={pillar.title} className="bg-card/85">
                      <CardContent className="grid gap-3 pt-6">
                        <div className="flex h-11 w-11 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                          <Icon className="h-5 w-5" />
                        </div>
                        <div className="space-y-1.5">
                          <div className="font-semibold">{pillar.title}</div>
                          <div className="text-sm leading-6 text-muted-foreground">{pillar.description}</div>
                        </div>
                      </CardContent>
                    </Card>
                  );
                })}
              </div>
            </div>

            <Card className="overflow-hidden bg-[linear-gradient(180deg,rgba(255,253,248,0.95),rgba(248,243,235,0.96))]">
              <CardHeader className="border-b border-border/70">
                <CardTitle className="text-lg">A household-manager conversation</CardTitle>
                <CardDescription>
                  Not just calendar imports. The product has to feel useful across the real family load.
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-3 pt-6">
                <Bubble from="parent" text="Can you get me caught up on what matters this week?" />
                <Bubble
                  from="florence"
                  text="This week has 2 school deadlines, 1 early dismissal, soccer runs long on Thursday, and nobody owns Friday pickup yet. I can text Kendall the short version and make you a store + forms list for tonight."
                />
                <Bubble from="parent" text="Yes, and what should we buy?" />
                <Bubble
                  from="florence"
                  text="For the week: snack sign-up items, sandwich bread, fruit, taco fixings, and poster board for Violet’s project. I added the list and I’ll remind you about the form before bedtime."
                />
              </CardContent>
            </Card>
          </section>

          <section id="explore" className="grid gap-5">
            <div className="space-y-2">
              <Badge variant="outline" className="w-fit">
                Explore scenarios
              </Badge>
              <h2 className="text-3xl font-semibold tracking-tight sm:text-4xl">
                Florence should feel useful across the whole household.
              </h2>
              <p className="max-w-3xl text-base leading-7 text-muted-foreground">
                These are the kinds of moments Florence should be great at: inbox triage, schedule sanity checks, invisible work, weekly planning, and carrying the family context forward.
              </p>
            </div>

            <div className="grid gap-5 lg:grid-cols-2">
              {scenarioGroups.map((group) => {
                const Icon = group.icon;
                return (
                  <Card key={group.title} className="overflow-hidden bg-card/90">
                    <CardHeader className="gap-4 border-b border-border/70 bg-[linear-gradient(180deg,rgba(28,91,122,0.04),rgba(255,255,255,0))]">
                      <div className="flex items-start gap-4">
                        <div className="flex h-12 w-12 items-center justify-center rounded-2xl bg-primary/10 text-primary">
                          <Icon className="h-5 w-5" />
                        </div>
                        <div className="space-y-1.5">
                          <Badge variant="outline" className="w-fit">
                            {group.eyebrow}
                          </Badge>
                          <CardTitle className="text-2xl">{group.title}</CardTitle>
                          <CardDescription className="max-w-xl leading-7">{group.summary}</CardDescription>
                        </div>
                      </div>
                    </CardHeader>
                    <CardContent className="grid gap-3 pt-6">
                      {group.messages.map((message, index) => (
                        <Bubble key={`${group.title}-${index}`} from={message.from} text={message.text} />
                      ))}
                    </CardContent>
                  </Card>
                );
              })}
            </div>
          </section>

          <section className="grid gap-5 lg:grid-cols-[minmax(0,1.1fr)_minmax(320px,0.9fr)]">
            <Card className="bg-card/92">
              <CardHeader>
                <CardTitle className="text-3xl">What Florence should be</CardTitle>
                <CardDescription className="max-w-3xl text-base leading-7">
                  A text-native household manager that catches what matters, remembers the family context, and helps coordinate the work without forcing the family into another heavy app.
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-4 text-sm leading-7 text-muted-foreground">
                <div>School and activity logistics</div>
                <div>Calendar grounding and conflict detection</div>
                <div>Shared lists, reminders, and follow-through</div>
                <div>Travel, forms, returns, gifts, and household admin</div>
                <div>Weekly planning, role clarity, and proactive check-ins</div>
                <div>Memory that compounds over time instead of resetting every session</div>
              </CardContent>
            </Card>

            <Card className="bg-primary text-primary-foreground">
              <CardHeader>
                <CardTitle className="text-3xl">Start with the household you already have.</CardTitle>
                <CardDescription className="text-primary-foreground/82">
                  Connect Google, ground the family, and let Florence learn through real usage instead of one giant setup form.
                </CardDescription>
              </CardHeader>
              <CardContent className="grid gap-4">
                <Button asChild size="lg" variant="secondary" className="justify-between">
                  <Link href="/login">
                    Start with Google
                    <ArrowRight className="h-4 w-4" />
                  </Link>
                </Button>
                <div className="text-sm leading-7 text-primary-foreground/82">
                  The web app exists to review, connect accounts, and see the household plan. The real product lives in the conversation.
                </div>
              </CardContent>
            </Card>
          </section>
        </main>
      </div>
    </div>
  );
}
