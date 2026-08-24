import type {
  CompleteFamilyOnboardingInput,
  FamilyCalendarEvent,
  FamilyMemberProfile,
  GoogleProviderRevocation,
  PatchFactInput,
  PatchWatchInput,
  PreferencesInput,
  SetupSessionInput,
  VaultFact,
  VaultWatch,
  WorkspaceView,
} from "@florence/contracts";
import { useQueryClient } from "@tanstack/react-query";
import { Link, Outlet } from "@tanstack/react-router";
import {
  CalendarDays,
  Check,
  ChevronLeft,
  ChevronRight,
  MapPin,
  MessageCircle,
  Pause,
  Play,
  Plus,
  RefreshCw,
  Trash2,
} from "lucide-react";
import { type FormEvent, useEffect, useState } from "react";
import { createSession, FlorenceRequestError } from "./api";
import { MemberEditor } from "./components/MemberEditor";
import {
  queryKeys,
  useCompleteFamilyOnboarding,
  useCreateSession,
  useDeleteFact,
  useDeleteGoogleDerivedData,
  useDeleteSession,
  useDeleteWatch,
  useDisconnectGoogleConnection,
  useFamilyCalendarMonth,
  usePatchFact,
  usePatchWatch,
  usePutMember,
  usePutPreferences,
  useSession,
  useStartGoogleConnection,
  useWorkspace,
} from "./queries";

const onboardingEntry = consumeOnboardingEntry();
let accessSessionRequest: ReturnType<typeof createSession> | null = null;
const CALENDAR_WEEKDAYS = [
  { short: "S", long: "Sunday" },
  { short: "M", long: "Monday" },
  { short: "T", long: "Tuesday" },
  { short: "W", long: "Wednesday" },
  { short: "T", long: "Thursday" },
  { short: "F", long: "Friday" },
  { short: "S", long: "Saturday" },
] as const;
const CALENDAR_SKELETON_CELLS = Array.from({ length: 35 }, (_, index) => `calendar-skeleton-${index + 1}`);

type CalendarMonthCell = {
  key: string;
  date: string | null;
};

export function AppShell() {
  const directEntry = onboardingEntry.setupToken !== null || onboardingEntry.accessToken !== null;
  const session = useSession(!directEntry);
  const workspace = useWorkspace(!directEntry && session.isSuccess);
  if (onboardingEntry.setupToken) return <SetupPage setupToken={onboardingEntry.setupToken} />;
  if (onboardingEntry.accessToken) return <AccessPage accessToken={onboardingEntry.accessToken} />;
  if (session.isLoading) return <PageLoader />;
  if (session.error instanceof FlorenceRequestError && session.error.status === 401) {
    return <StartInMessagesPage />;
  }
  if (session.isError) return <LoadError error={session.error} />;
  if (workspace.isLoading) return <PageLoader />;
  if (workspace.isError) return <LoadError error={workspace.error} />;
  if (!workspace.data) return <PageLoader />;
  const googleConnected = workspace.data.workspace.googleConnections.length > 0;
  if (!googleConnected && !workspace.data.workspace.setup.ownOnboardingComplete) {
    return <GoogleSetupGate status={onboardingEntry.googleStatus} />;
  }
  if (!workspace.data.workspace.setup.ownOnboardingComplete) {
    return <FamilySetupPage view={workspace.data} />;
  }
  if (onboardingEntry.setupComplete || onboardingEntry.googleStatus === "connected") {
    return <GoogleSetupSuccess view={workspace.data} />;
  }

  return (
    <div className="shell">
      <DesktopSidebar />
      <div className="content-shell">
        <MobileHeader />
        <main className="content">
          <Outlet />
        </main>
      </div>
    </div>
  );
}

function AccessPage({ accessToken }: { accessToken: string }) {
  const [error, setError] = useState<Error | null>(null);

  useEffect(() => {
    let active = true;
    accessSessionRequest ??= createSession({ accessToken });
    void accessSessionRequest.then(
      (session) => {
        if (active) window.location.replace(session.accessPath ?? "/");
      },
      (cause: unknown) => {
        if (!active) return;
        setError(
          new Error(
            cause instanceof FlorenceRequestError && cause.status === 401
              ? "This private link is no longer valid. Ask Florence for a fresh link in your private Messages conversation."
              : cause instanceof Error
                ? cause.message
                : "Florence could not open this private link.",
          ),
        );
      },
    );
    return () => {
      active = false;
    };
  }, [accessToken]);

  return error ? <LoadError error={error} /> : <PageLoader />;
}

function SetupPage({ setupToken }: { setupToken: string }) {
  const createSession = useCreateSession();
  const startGoogle = useStartGoogleConnection();
  const [step, setStep] = useState<"profile" | "permission" | "google">("profile");
  const [firstNameValue, setFirstNameValue] = useState("");
  const [lastNameValue, setLastNameValue] = useState("");
  const [privateConflictBusySharingEnabled, setPrivateConflictBusySharingEnabled] = useState(false);
  const [token, setToken] = useState<string | null>(setupToken);
  const [error, setError] = useState<string | null>(null);

  function continueFromName(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFirstNameValue((value) => value.trim());
    setLastNameValue((value) => value.trim());
    setStep("permission");
  }

  async function submitProfile(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    if (!token) return;
    const timeZone = detectedTimeZone();
    if (!isTimeZone(timeZone)) {
      setError("Use an IANA time zone such as America/Los_Angeles.");
      return;
    }
    const profile: SetupSessionInput["profile"] = {
      firstName: firstNameValue,
      lastName: lastNameValue,
      timeZone,
      guardianAttested: true,
      proactiveUseAccepted: true,
      privateConflictBusySharingEnabled,
    };
    try {
      await createSession.mutateAsync({ setupToken: token, profile });
      setToken(null);
      setStep("google");
    } catch (cause) {
      setError(setupError(cause));
    }
  }

  async function connectGoogle() {
    setError(null);
    try {
      const result = await startGoogle.mutateAsync();
      window.location.assign(result.authorizationUrl);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not open Google.");
    }
  }

  if (step === "google") {
    return (
      <SetupFrame>
        <GoogleSetupStep
          error={error}
          isPending={startGoogle.isPending}
          onConnect={() => void connectGoogle()}
        />
      </SetupFrame>
    );
  }

  if (step === "permission") {
    return (
      <SetupFrame>
        <form className="setup-form" onSubmit={(event) => void submitProfile(event)}>
          <SetupHeading
            title="Can Florence stay ahead of things for you?"
            detail="She’ll look for family things in Gmail and Calendar, remember useful details, add clear official dates to your family calendar, and text when something needs attention."
          />
          <label className="setup-attestation">
            <input name="proactiveUseAccepted" type="checkbox" required />
            <span>Yes, Florence can do this. I can turn it off later in settings.</span>
          </label>
          <label className="setup-attestation">
            <input name="guardianAttested" type="checkbox" required />
            <span>I’m a parent, guardian, or caregiver for the children I add.</span>
          </label>
          <label className="setup-attestation">
            <input
              name="privateConflictBusySharingEnabled"
              type="checkbox"
              checked={privateConflictBusySharingEnabled}
              onChange={(event) => setPrivateConflictBusySharingEnabled(event.target.checked)}
            />
            <span>
              Florence may tell our family chat when I’m busy, without sharing the event name or personal
              details.
            </span>
          </label>
          <p className="setup-footnote">
            Personal details stay private unless you ask Florence to share them. Once both parents finish
            setup, Florence will create a new shared family calendar for you.
          </p>
          {error && <SetupError>{error}</SetupError>}
          <button className="button primary wide" type="submit" disabled={createSession.isPending}>
            {createSession.isPending ? "Saving…" : "Continue"}
          </button>
        </form>
      </SetupFrame>
    );
  }

  return (
    <SetupFrame>
      <form className="setup-form" onSubmit={continueFromName}>
        <SetupHeading
          title="What’s your name?"
          detail="Florence will use it when she texts you and your family."
        />
        <label className="field">
          <span>First name</span>
          <input
            value={firstNameValue}
            onChange={(event) => setFirstNameValue(event.target.value)}
            autoComplete="given-name"
            placeholder="First name"
            required
          />
        </label>
        <label className="field">
          <span>Last name</span>
          <input
            value={lastNameValue}
            onChange={(event) => setLastNameValue(event.target.value)}
            autoComplete="family-name"
            placeholder="Last name"
            required
          />
        </label>
        {error && (
          <p className="form-error" role="alert">
            {error}
          </p>
        )}
        <button className="button primary wide" type="submit">
          Continue
        </button>
      </form>
    </SetupFrame>
  );
}

function StartInMessagesPage() {
  return (
    <SetupFrame>
      <SetupHeading
        title="Open Florence from Messages"
        detail="In your private conversation, ask Florence for a fresh web link, then open the link she sends there."
      />
      <p className="setup-footnote">
        Florence will confirm it’s really your conversation before you sign in.
      </p>
    </SetupFrame>
  );
}

function GoogleSetupGate({ status }: { status: string | null }) {
  const startGoogle = useStartGoogleConnection();
  const [error, setError] = useState<string | null>(null);

  async function connectGoogle() {
    setError(null);
    try {
      const result = await startGoogle.mutateAsync();
      window.location.assign(result.authorizationUrl);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not open Google.");
    }
  }

  return (
    <SetupFrame>
      <GoogleSetupStep
        error={error ?? (status ? googleSetupError(status) : null)}
        isPending={startGoogle.isPending}
        onConnect={() => void connectGoogle()}
      />
      <GoogleDataDeletionControl />
    </SetupFrame>
  );
}

function GoogleSetupSuccess({ view }: { view: WorkspaceView }) {
  return (
    <SetupFrame>
      <div className="setup-success-mark" aria-hidden="true">
        <Check size={18} />
      </div>
      <SetupHeading
        title="Your side is ready"
        detail="Head back to Messages—Florence will follow up there shortly."
      />
      {view.workspace.messagesUrl ? (
        <a className="button primary wide" href={view.workspace.messagesUrl}>
          Back to Messages
        </a>
      ) : (
        <p className="setup-footnote">You can close this page and return to Messages.</p>
      )}
      <a className="setup-secondary-action" href="/preferences">
        Open Florence settings
      </a>
    </SetupFrame>
  );
}

type ChildDraft = {
  id: string;
  firstName: string;
  lastName: string;
  school: string;
  activities: string;
};

type FamilySetupScreen =
  | { kind: "partner" }
  | { kind: "partner-phone" }
  | { kind: "child-name"; childId: string; returnToReview?: boolean }
  | { kind: "child-school"; childId: string; returnToReview?: boolean }
  | { kind: "child-activities"; childId: string; returnToReview?: boolean }
  | { kind: "more-children" }
  | { kind: "postal-code" }
  | { kind: "review" };

function FamilySetupPage({ view }: { view: WorkspaceView }) {
  const complete = useCompleteFamilyOnboarding();
  const [partnerFirstName, setPartnerFirstName] = useState("");
  const [partnerLastName, setPartnerLastName] = useState("");
  const [partnerPhone, setPartnerPhone] = useState("");
  const [postalCode, setPostalCode] = useState("");
  const viewerLastName = view.viewer.lastName ?? "Family";
  const familyLabel = familyLabelFromSurnames(viewerLastName, partnerLastName);
  const [children, setChildren] = useState<ChildDraft[]>(() => [newChildDraft()]);
  const [screen, setScreen] = useState<FamilySetupScreen>({ kind: "partner" });
  const [error, setError] = useState<string | null>(null);

  function updateChild(id: string, patch: Partial<ChildDraft>) {
    setChildren((current) => current.map((child) => (child.id === id ? { ...child, ...patch } : child)));
  }

  function showScreen(next: FamilySetupScreen) {
    setError(null);
    setScreen(next);
  }

  function continueFromPartner(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setPartnerFirstName((current) => current.trim());
    setPartnerLastName((current) => current.trim());
    showScreen({ kind: "partner-phone" });
  }

  function continueFromChildName(event: FormEvent<HTMLFormElement>, child: ChildDraft) {
    event.preventDefault();
    if (!child.firstName.trim()) {
      setError("Add your child’s first name.");
      return;
    }
    updateChild(child.id, { firstName: child.firstName.trim(), lastName: child.lastName.trim() });
    showScreen({
      kind: "child-school",
      childId: child.id,
      ...(screen.kind === "child-name" && screen.returnToReview ? { returnToReview: true } : {}),
    });
  }

  function continueFromChildSchool(event: FormEvent<HTMLFormElement>, child: ChildDraft) {
    event.preventDefault();
    updateChild(child.id, { school: child.school.trim() });
    showScreen({
      kind: "child-activities",
      childId: child.id,
      ...(screen.kind === "child-school" && screen.returnToReview ? { returnToReview: true } : {}),
    });
  }

  function continueFromChildActivities(event: FormEvent<HTMLFormElement>, child: ChildDraft) {
    event.preventDefault();
    updateChild(child.id, { activities: child.activities.trim() });
    showScreen(
      screen.kind === "child-activities" && screen.returnToReview
        ? { kind: "review" }
        : { kind: "more-children" },
    );
  }

  function addChild() {
    const child = newChildDraft();
    setChildren((current) => [...current, child]);
    showScreen({ kind: "child-name", childId: child.id });
  }

  function discardChild(id: string) {
    setChildren((current) => current.filter((child) => child.id !== id));
    showScreen({ kind: "more-children" });
  }

  async function submit() {
    setError(null);
    const family = {
      postalCode: postalCode.trim(),
      children: children.map((child) => {
        const activities = listValues(child.activities);
        return {
          firstName: child.firstName.trim(),
          ...(child.lastName.trim() ? { lastName: child.lastName.trim() } : {}),
          ...(child.school.trim() ? { school: child.school.trim() } : {}),
          ...(activities.length ? { activities } : {}),
        };
      }),
    };
    const input: CompleteFamilyOnboardingInput = {
      ...family,
      mode: "two_adult",
      partner: {
        firstName: partnerFirstName.trim(),
        lastName: partnerLastName.trim(),
        phoneNumber: partnerPhone,
      },
    };
    if (input.children.some((child) => !child.firstName)) {
      setError("Add each child’s first name.");
      return;
    }
    try {
      await complete.mutateAsync(input);
      window.location.replace("/?setup=complete");
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not save your family setup.");
    }
  }

  const activeChild = "childId" in screen ? children.find((child) => child.id === screen.childId) : undefined;

  if (screen.kind === "partner") {
    return (
      <SetupFrame>
        <form className="setup-form" onSubmit={continueFromPartner}>
          <SetupHeading
            title={`Who runs family life with you, ${firstName(view.viewer.displayName ?? "there")}?`}
            detail="Florence will ask before texting them. They’ll set up their own account."
          />
          <label className="field">
            <span>First name</span>
            <input
              value={partnerFirstName}
              onChange={(event) => setPartnerFirstName(event.target.value)}
              autoComplete="given-name"
              placeholder="First name"
              required
            />
          </label>
          <label className="field">
            <span>Last name</span>
            <input
              value={partnerLastName}
              onChange={(event) => setPartnerLastName(event.target.value)}
              autoComplete="family-name"
              placeholder="Last name"
              required
            />
          </label>
          <button className="button primary wide" type="submit">
            Continue
          </button>
        </form>
      </SetupFrame>
    );
  }

  if (screen.kind === "partner-phone") {
    return (
      <SetupFrame>
        <form
          className="setup-form"
          onSubmit={(event) => {
            event.preventDefault();
            showScreen({ kind: "child-name", childId: children[0]?.id ?? "" });
          }}
        >
          <SetupHeading
            title={`What’s ${partnerFirstName}’s number?`}
            detail="Florence won’t text them until she asks you in Messages."
          />
          <label className="field">
            <span>US mobile number</span>
            <input
              value={formatUsPhoneNumber(partnerPhone)}
              onChange={(event) => setPartnerPhone(usPhoneDigits(event.target.value))}
              autoComplete="tel"
              inputMode="numeric"
              placeholder="415 555 0123"
              pattern="[0-9]{3} [0-9]{3} [0-9]{4}"
              maxLength={12}
              required
            />
          </label>
          <p className="setup-footnote">No country code needed.</p>
          <button className="button primary wide" type="submit">
            Continue
          </button>
        </form>
      </SetupFrame>
    );
  }

  if (screen.kind === "child-name" && activeChild) {
    return (
      <SetupFrame>
        <form className="setup-form" onSubmit={(event) => continueFromChildName(event, activeChild)}>
          <SetupHeading
            title="Who should Florence know about?"
            detail="Add one child at a time. Florence uses this to recognize the family details that reach your inbox."
          />
          <label className="field">
            <span>Child’s first name</span>
            <input
              value={activeChild.firstName}
              onChange={(event) => updateChild(activeChild.id, { firstName: event.target.value })}
              autoComplete="given-name"
              placeholder="First name"
              required
            />
          </label>
          <label className="field">
            <span>Last name (optional)</span>
            <input
              value={activeChild.lastName}
              onChange={(event) => updateChild(activeChild.id, { lastName: event.target.value })}
              autoComplete="family-name"
              placeholder="Last name"
            />
          </label>
          {error && <SetupError>{error}</SetupError>}
          <button className="button primary wide" type="submit">
            Continue
          </button>
          {children.length > 1 && !screen.returnToReview && (
            <button
              className="setup-secondary-action"
              type="button"
              onClick={() => discardChild(activeChild.id)}
            >
              Never mind
            </button>
          )}
        </form>
      </SetupFrame>
    );
  }

  if (screen.kind === "child-school" && activeChild) {
    return (
      <SetupFrame>
        <form className="setup-form" onSubmit={(event) => continueFromChildSchool(event, activeChild)}>
          <SetupHeading
            title={`Where does ${firstName(activeChild.firstName)} go during the day?`}
            detail="A school, daycare, or preschool helps Florence recognize schedules and messages."
          />
          <label className="field">
            <span>School or daycare (optional)</span>
            <input
              value={activeChild.school}
              onChange={(event) => updateChild(activeChild.id, { school: event.target.value })}
              autoComplete="off"
              placeholder="School or daycare"
            />
          </label>
          <button className="button primary wide" type="submit">
            {activeChild.school.trim() ? "Continue" : "Skip for now"}
          </button>
        </form>
      </SetupFrame>
    );
  }

  if (screen.kind === "child-activities" && activeChild) {
    return (
      <SetupFrame>
        <form className="setup-form" onSubmit={(event) => continueFromChildActivities(event, activeChild)}>
          <SetupHeading
            title={`What is ${firstName(activeChild.firstName)} into?`}
            detail="Add any recurring activities that tend to create practices, pickups, or calendar events."
          />
          <label className="field">
            <span>Activities (optional)</span>
            <input
              value={activeChild.activities}
              onChange={(event) => updateChild(activeChild.id, { activities: event.target.value })}
              autoComplete="off"
              placeholder="Soccer, piano, robotics"
            />
          </label>
          <button className="button primary wide" type="submit">
            {activeChild.activities.trim() ? "Continue" : "Skip for now"}
          </button>
        </form>
      </SetupFrame>
    );
  }

  if (screen.kind === "more-children") {
    const names = children.map((child) => firstName(child.firstName)).join(", ");
    return (
      <SetupFrame>
        <div className="setup-form">
          <SetupHeading
            title="Anyone else?"
            detail={`${names} ${children.length === 1 ? "is" : "are"} in. You can add another child or keep going.`}
          />
          <button
            className="button primary wide"
            type="button"
            onClick={() => showScreen({ kind: "postal-code" })}
          >
            Review family
          </button>
          {children.length < 20 && (
            <button className="setup-secondary-action" type="button" onClick={addChild}>
              Add another child
            </button>
          )}
        </div>
      </SetupFrame>
    );
  }

  if (screen.kind === "postal-code") {
    return (
      <SetupFrame>
        <form
          className="setup-form"
          onSubmit={(event) => {
            event.preventDefault();
            showScreen({ kind: "review" });
          }}
        >
          <SetupHeading
            title="What’s your ZIP code?"
            detail="This helps Florence find nearby school and family activities."
          />
          <label className="field">
            <span>ZIP code</span>
            <input
              value={postalCode}
              onChange={(event) => setPostalCode(event.target.value)}
              inputMode="numeric"
              autoComplete="postal-code"
              placeholder="94110"
              pattern="[0-9]{5}(-[0-9]{4})?"
              required
            />
          </label>
          <button className="button primary wide" type="submit">
            Continue
          </button>
        </form>
      </SetupFrame>
    );
  }

  if (screen.kind === "review") {
    return (
      <SetupFrame>
        <div className="setup-form">
          <SetupHeading
            title="Does this look right?"
            detail={`Florence will call you the ${familyLabel}. After both parents finish, she’ll automatically create your shared calendar.`}
          />
          <div className="setup-review-list">
            <button
              className="setup-review-row"
              type="button"
              onClick={() => showScreen({ kind: "partner" })}
            >
              <span>Partner or co-parent</span>
              <strong>{`${partnerFirstName} ${partnerLastName} · ${formatUsPhoneNumber(partnerPhone)}`}</strong>
            </button>
            {children.map((child) => (
              <button
                className="setup-review-row"
                type="button"
                key={child.id}
                onClick={() => showScreen({ kind: "child-name", childId: child.id, returnToReview: true })}
              >
                <span>{`${child.firstName} ${child.lastName}`.trim()}</span>
                <strong>
                  {[child.school, child.activities].filter(Boolean).join(" · ") || "No details added"}
                </strong>
              </button>
            ))}
            <button
              className="setup-review-row"
              type="button"
              onClick={() => showScreen({ kind: "postal-code" })}
            >
              <span>Family</span>
              <strong>{`${familyLabel} · ZIP ${postalCode}`}</strong>
            </button>
          </div>
          {error && <SetupError>{error}</SetupError>}
          <button
            className="button primary wide"
            type="button"
            onClick={() => void submit()}
            disabled={complete.isPending}
          >
            {complete.isPending ? "Saving your family…" : "Finish setup"}
          </button>
          <button
            className="setup-secondary-action"
            type="button"
            onClick={() => showScreen({ kind: "more-children" })}
          >
            Add another child
          </button>
        </div>
      </SetupFrame>
    );
  }

  return <PageLoader />;
}

function GoogleSetupStep({
  error,
  isPending,
  onConnect,
}: {
  error: string | null;
  isPending: boolean;
  onConnect: () => void;
}) {
  return (
    <div className="setup-form">
      <SetupHeading
        title="Connect your Google account"
        detail="Use the account where school emails and family plans usually land. Florence will start sorting through them while you finish setup."
      />
      <div className="setup-google-card">
        <span className="google-mark" aria-hidden="true">
          G
        </span>
        <div>
          <strong>Google Workspace</strong>
          <p>Gmail and Calendar</p>
        </div>
      </div>
      <p className="setup-trust">
        When Google asks, leave both Gmail and Calendar enabled—Florence can’t connect if either is turned
        off. Florence keeps your personal email private. With your permission, she may add a clear official
        family date to the shared calendar. If something is uncertain, she’ll ask first.
      </p>
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      <button className="button primary wide" type="button" onClick={onConnect} disabled={isPending}>
        {isPending ? "Opening Google…" : "Connect Google Workspace"}
      </button>
    </div>
  );
}

function SetupFrame({ children }: { children: React.ReactNode }) {
  return (
    <main className="setup-page">
      <section className="setup-flow">
        <div className="setup-brand" aria-hidden="true">
          F
        </div>
        <div className="setup-content">{children}</div>
      </section>
    </main>
  );
}

function SetupHeading({ title, detail }: { title: string; detail: string }) {
  return (
    <header className="setup-heading-copy">
      <h1>{title}</h1>
      <p>{detail}</p>
    </header>
  );
}

function SetupError({ children }: { children: React.ReactNode }) {
  return (
    <p className="form-error" role="alert">
      {children}
    </p>
  );
}

export function WorkspacePage() {
  const query = useWorkspace();
  if (query.isLoading) return <PageLoader />;
  if (query.isError) return <LoadError error={query.error} />;
  const view = query.data;
  if (!view) return <PageLoader />;

  const { setup } = view.workspace;
  const partnerState =
    setup.partnerInvitation === "connected"
      ? "Private Messages connected"
      : setup.partnerInvitation === "invited"
        ? "Private invitation sent"
        : setup.partnerInvitation === "approved"
          ? "Invitation approved"
          : setup.partnerInvitation === "ready"
            ? "Ready to invite"
            : "Not added yet";
  const googleState = setup.bothAdultsGoogleConnected
    ? "Both parents connected"
    : view.workspace.googleConnections.length
      ? "Your account is connected"
      : "Not connected";

  let currentTitle = "Bring your partner into Florence";
  let currentDetail =
    "Continue in your private Messages conversation. Florence will ask before sending a private invitation.";
  if (setup.partnerInvitation === "ready") {
    currentTitle = "Invite your partner in Messages";
    currentDetail =
      "Florence is waiting for your okay in your private conversation. Your partner will complete their own setup.";
  } else if (setup.partnerInvitation === "approved") {
    currentTitle = "Florence has your okay";
    currentDetail =
      "She’ll confirm in your private conversation once your partner’s invitation is delivered.";
  } else if (setup.partnerInvitation === "invited") {
    currentTitle = "Your partner has the next step";
    currentDetail =
      "They’ll finish in their own private Messages conversation and connect their own Google account.";
  } else if (!setup.bothAdultsMessagesConnected) {
    currentTitle = "Connect both parents in Messages";
    currentDetail =
      "Each parent needs a separate private conversation before Florence creates the family group.";
  } else if (!setup.bothAdultsGoogleConnected) {
    currentTitle = "One Google connection to go";
    currentDetail =
      "Your partner completes this privately. Florence keeps each parent’s Gmail, Calendar, and personal details separate.";
  } else if (!setup.familyGroupConnected) {
    currentTitle = "Next, Florence creates your family group";
    currentDetail =
      "The exact three-person Messages group will be the main place for family plans, follow-ups, and decisions.";
  } else if (!setup.familyCalendarConnected) {
    currentTitle = "Next, Florence creates your family calendar";
    currentDetail = "Both parents will have equal Florence authority over the new shared family calendar.";
  } else if (setup.initialBriefing === "preparing") {
    currentTitle = "Florence is preparing your first family briefing";
    currentDetail =
      "She’s reviewing each parent’s side privately and will put only the useful shared picture in your family group.";
  } else if (setup.initialBriefing === "not_ready") {
    currentTitle = "Florence is starting your first family briefing";
    currentDetail =
      "Your household is connected. Florence will review each parent’s side before she writes in the family group.";
  } else {
    currentTitle = "Florence is ready in your family group";
    currentDetail =
      "Both parents, the exact family group, and the shared calendar are ready. Your first family briefing was sent in Messages.";
  }

  return (
    <Page title="Workspace" intro="Family life happens in Messages. This is a quiet check on what’s ready.">
      <section className="workspace-card" aria-labelledby="workspace-current-title">
        <div className="workspace-current">
          <div className="workspace-current-copy">
            <span className="workspace-eyebrow">Current</span>
            <h2 id="workspace-current-title">{currentTitle}</h2>
            <p>{currentDetail}</p>
          </div>
          {view.workspace.messagesUrl && (
            <a className="button primary workspace-message-action" href={view.workspace.messagesUrl}>
              Open Messages
            </a>
          )}
        </div>
        <dl className="workspace-state-list" aria-label="Household readiness">
          <div className="workspace-state-row">
            <dt>Partner</dt>
            <dd>{partnerState}</dd>
          </div>
          <div className="workspace-state-row">
            <dt>Google</dt>
            <dd>{googleState}</dd>
          </div>
          <div className="workspace-state-row">
            <dt>Family group</dt>
            <dd>{setup.familyGroupConnected ? "Exact group ready" : "Not created yet"}</dd>
          </div>
          <div className="workspace-state-row">
            <dt>Family calendar</dt>
            <dd>{setup.familyCalendarConnected ? "Shared calendar ready" : "Not created yet"}</dd>
          </div>
          <div className="workspace-state-row">
            <dt>First family briefing</dt>
            <dd>
              {setup.initialBriefing === "sent"
                ? "Sent in Messages"
                : setup.initialBriefing === "preparing"
                  ? "Preparing"
                  : "Waiting for household setup"}
            </dd>
          </div>
        </dl>
      </section>
    </Page>
  );
}

export function CalendarPage() {
  const queryClient = useQueryClient();
  const householdTimeZone = queryClient.getQueryData<WorkspaceView>(queryKeys.workspace)?.vault?.timeZone;
  const [month, setMonth] = useState(() => currentCalendarMonth(householdTimeZone));
  const [selectedDate, setSelectedDate] = useState(() => currentCalendarDate(householdTimeZone));
  const query = useFamilyCalendarMonth(month);

  function showMonth(offset: number) {
    const nextMonth = shiftCalendarMonth(month, offset);
    setMonth(nextMonth);
    setSelectedDate(`${nextMonth}-01`);
  }

  function showToday() {
    const today = currentCalendarDate(query.data?.timeZone ?? householdTimeZone);
    setMonth(today.slice(0, 7));
    setSelectedDate(today);
  }

  const calendarName = query.data?.calendarName ?? null;

  return (
    <Page
      title="Calendar"
      intro="Your family group is where plans happen. This is the shared calendar Florence keeps for both parents."
    >
      <section className="calendar-section" aria-live="polite">
        <CalendarToolbar
          month={month}
          calendarName={calendarName}
          onPrevious={() => showMonth(-1)}
          onNext={() => showMonth(1)}
          onToday={showToday}
        />

        {query.isLoading ? (
          <CalendarSkeleton />
        ) : query.isError ? (
          <CalendarState
            title="The calendar couldn’t load"
            detail="Nothing changed. Try loading it again in a moment."
            action={
              <button className="button pill" type="button" onClick={() => void query.refetch()}>
                <RefreshCw size={14} /> Try again
              </button>
            }
          />
        ) : query.data?.status === "not_ready" ? (
          <CalendarState
            title="Your family calendar is on the way"
            detail="Florence will make it automatically after both parents finish setup. She’ll tell you in your family group when it’s ready."
          />
        ) : query.data?.status === "temporarily_unavailable" ? (
          <CalendarState
            title="Google Calendar isn’t available right now"
            detail="Nothing changed. Try loading your family calendar again in a moment."
            action={
              <button className="button pill" type="button" onClick={() => void query.refetch()}>
                <RefreshCw size={14} /> Try again
              </button>
            }
          />
        ) : query.data?.status === "ready" ? (
          <CalendarMonth
            month={month}
            selectedDate={selectedDate}
            timeZone={query.data.timeZone}
            events={query.data.events}
            truncated={query.data.truncated}
            onSelectDate={setSelectedDate}
          />
        ) : (
          <CalendarSkeleton />
        )}
      </section>
    </Page>
  );
}

function CalendarToolbar({
  month,
  calendarName,
  onPrevious,
  onNext,
  onToday,
}: {
  month: string;
  calendarName: string | null;
  onPrevious: () => void;
  onNext: () => void;
  onToday: () => void;
}) {
  return (
    <div className="calendar-toolbar">
      <div className="calendar-title">
        <h2>{calendarMonthLabel(month)}</h2>
        {calendarName && <p>{calendarName}</p>}
      </div>
      <div className="calendar-controls">
        <button className="calendar-today" type="button" onClick={onToday}>
          Today
        </button>
        <button className="calendar-arrow" type="button" onClick={onPrevious} aria-label="Previous month">
          <ChevronLeft size={16} />
        </button>
        <button className="calendar-arrow" type="button" onClick={onNext} aria-label="Next month">
          <ChevronRight size={16} />
        </button>
      </div>
    </div>
  );
}

function CalendarMonth({
  month,
  selectedDate,
  timeZone,
  events,
  truncated,
  onSelectDate,
}: {
  month: string;
  selectedDate: string;
  timeZone: string;
  events: FamilyCalendarEvent[];
  truncated: boolean;
  onSelectDate: (date: string) => void;
}) {
  const weeks = calendarMonthWeeks(month);
  const today = currentCalendarDate(timeZone);
  const selectedEvents = eventsForCalendarDate(events, selectedDate, timeZone);

  return (
    <>
      {!events.length && (
        <div className="calendar-empty-month">
          <strong>Nothing on the family calendar this month</strong>
          <p>When Florence adds a family plan, it’ll appear here for both parents.</p>
        </div>
      )}
      <div className="calendar-layout">
        <div className="calendar-grid-wrap">
          <table className="calendar-grid">
            <caption className="visually-hidden">{calendarMonthLabel(month)}</caption>
            <thead>
              <tr>
                {CALENDAR_WEEKDAYS.map((day) => (
                  <th scope="col" key={day.long} aria-label={day.long}>
                    {day.short}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {weeks.map((week) => (
                <tr key={week[0]?.key ?? month}>
                  {week.map((cell) => {
                    const { date } = cell;
                    if (!date) {
                      return <td className="calendar-blank" key={cell.key} />;
                    }
                    const dayEvents = eventsForCalendarDate(events, date, timeZone);
                    const isSelected = date === selectedDate;
                    const isToday = date === today;
                    return (
                      <td key={date}>
                        <button
                          className={`calendar-day${isSelected ? " selected" : ""}${isToday ? " today" : ""}`}
                          type="button"
                          onClick={() => onSelectDate(date)}
                          aria-label={calendarDayAriaLabel(date, dayEvents.length)}
                          aria-pressed={isSelected}
                          aria-current={isToday ? "date" : undefined}
                        >
                          <span className="calendar-day-number">{Number(date.slice(-2))}</span>
                          <span className="calendar-dots" aria-hidden="true">
                            {dayEvents.slice(0, 3).map((event, eventIndex) => (
                              <span
                                className={`calendar-dot${event.status === "tentative" ? " tentative" : ""}`}
                                key={calendarEventKey(event, eventIndex)}
                              />
                            ))}
                            {dayEvents.length > 3 && (
                              <span className="calendar-more">+{dayEvents.length - 3}</span>
                            )}
                          </span>
                        </button>
                      </td>
                    );
                  })}
                </tr>
              ))}
            </tbody>
          </table>
          {truncated && (
            <p className="calendar-truncated">
              This is an especially busy month, so a few plans may not appear here. Check Google Calendar for
              the complete list.
            </p>
          )}
        </div>
        <CalendarAgenda date={selectedDate} events={selectedEvents} timeZone={timeZone} />
      </div>
      <div className="calendar-message-note">
        <MessageCircle size={16} aria-hidden="true" />
        <p>
          To add or change a plan, text Florence in your family group. She’ll keep this calendar up to date
          for both parents.
        </p>
      </div>
    </>
  );
}

function CalendarAgenda({
  date,
  events,
  timeZone,
}: {
  date: string;
  events: FamilyCalendarEvent[];
  timeZone: string;
}) {
  return (
    <aside className="calendar-agenda" aria-label={`Plans for ${calendarDayLabel(date)}`}>
      <header>
        <span className="calendar-agenda-weekday">{calendarDayWeekday(date)}</span>
        <h3>{calendarDayLabel(date)}</h3>
      </header>
      {events.length ? (
        <ol className="calendar-event-list">
          {events.map((event, index) => (
            <li className="calendar-event" key={calendarEventKey(event, index)}>
              <div className="calendar-event-heading">
                <strong>{event.title ?? "Family plan"}</strong>
                {event.status === "tentative" && <span className="calendar-event-status">Tentative</span>}
              </div>
              <p>{calendarEventTime(event, timeZone)}</p>
              {event.location && (
                <p className="calendar-event-location">
                  <MapPin size={12} aria-hidden="true" /> {event.location}
                </p>
              )}
            </li>
          ))}
        </ol>
      ) : (
        <p className="calendar-agenda-empty">Nothing planned for this day.</p>
      )}
    </aside>
  );
}

function CalendarState({
  title,
  detail,
  action,
}: {
  title: string;
  detail: string;
  action?: React.ReactNode;
}) {
  return (
    <div className="calendar-state">
      <span className="calendar-state-icon" aria-hidden="true">
        <CalendarDays size={20} />
      </span>
      <strong>{title}</strong>
      <p>{detail}</p>
      {action && <div className="calendar-state-action">{action}</div>}
    </div>
  );
}

function CalendarSkeleton() {
  return (
    <div className="calendar-skeleton" role="status" aria-label="Loading family calendar">
      <div className="calendar-skeleton-grid" aria-hidden="true">
        {CALENDAR_SKELETON_CELLS.map((cell) => (
          <span className="calendar-skeleton-cell" key={cell} />
        ))}
      </div>
      <div className="calendar-skeleton-agenda" aria-hidden="true">
        <span className="calendar-skeleton-line calendar-skeleton-line-heading" />
        <span className="calendar-skeleton-line" />
        <span className="calendar-skeleton-line" />
      </div>
    </div>
  );
}

export function VaultPage() {
  const query = useWorkspace();
  const putMember = usePutMember();
  const patchFact = usePatchFact();
  const deleteFact = useDeleteFact();
  const patchWatch = usePatchWatch();
  const deleteWatch = useDeleteWatch();
  const [editing, setEditing] = useState<FamilyMemberProfile | "new" | null>(null);

  if (query.isLoading) return <PageLoader />;
  if (query.isError) return <LoadError error={query.error} />;
  const view = query.data;
  if (!view) return <PageLoader />;
  const vault = view.vault;
  if (!vault) {
    return (
      <LoadError
        error={new Error("Your household setup is incomplete. Return to the private link Florence sent.")}
      />
    );
  }
  const adults = vault.members.filter((member) => member.kind === "adult");
  const children = vault.members.filter((member) => member.kind === "child");
  const foundingAdult = adults.find((member) => member.postalCode !== undefined) ?? null;

  return (
    <Page title="Vault" intro="The family knowledge Florence may use, with its source and visibility.">
      {editing && (
        <MemberEditor
          {...(editing === "new" ? {} : { member: editing })}
          isSaving={putMember.isPending}
          onCancel={() => setEditing(null)}
          onSave={async (memberId, input) => {
            await putMember.mutateAsync({ memberId, input });
            setEditing(null);
          }}
        />
      )}

      <VaultSection label="Home">
        <HomePostalCode
          postalCode={vault.postalCode}
          isSaving={putMember.isPending}
          onSave={async (postalCode) => {
            if (!foundingAdult) throw new Error("Florence could not find your household profile.");
            await putMember.mutateAsync({
              memberId: foundingAdult.id,
              input: { postalCode },
            });
          }}
        />
      </VaultSection>

      <VaultSection label="Florence is watching">
        <WatchList
          watches={vault.watches}
          timeZone={vault.timeZone}
          isSaving={patchWatch.isPending || deleteWatch.isPending}
          onUpdate={(workId, input) => patchWatch.mutateAsync({ workId, input })}
          onDelete={(workId) => deleteWatch.mutateAsync(workId)}
        />
      </VaultSection>

      <VaultSection label="Adults">
        <PeopleList members={adults} onEdit={setEditing} />
      </VaultSection>

      <VaultSection
        label="Children"
        action={
          <button className="text-button" type="button" onClick={() => setEditing("new")}>
            <Plus size={14} /> Add child
          </button>
        }
      >
        <PeopleList members={children} onEdit={setEditing} />
      </VaultSection>

      <VaultSection label="Facts">
        <FactList
          facts={vault.facts}
          isSaving={patchFact.isPending || deleteFact.isPending}
          onCorrect={(factId, input) => patchFact.mutateAsync({ factId, input })}
          onDelete={(factId) => deleteFact.mutateAsync(factId)}
        />
      </VaultSection>
    </Page>
  );
}

function HomePostalCode({
  postalCode,
  isSaving,
  onSave,
}: {
  postalCode: string | null;
  isSaving: boolean;
  onSave: (postalCode: string) => Promise<unknown>;
}) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(postalCode ?? "");
  const [error, setError] = useState<string | null>(null);

  useEffect(() => setDraft(postalCode ?? ""), [postalCode]);

  async function save(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const next = draft.trim();
    if (!/^\d{5}(?:-\d{4})?$/.test(next)) {
      setError("Enter a five-digit ZIP code.");
      return;
    }
    setError(null);
    try {
      await onSave(next);
      setEditing(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not update your home ZIP.");
    }
  }

  return (
    <div className="vault-data-list">
      <div className="vault-data-row">
        {editing ? (
          <form className="fact-editor" onSubmit={(event) => void save(event)}>
            <label className="field">
              <span>Home ZIP</span>
              <input
                value={draft}
                onChange={(event) => setDraft(event.target.value)}
                inputMode="numeric"
                autoComplete="postal-code"
                placeholder="94110"
                required
              />
            </label>
            <div className="row-actions">
              <button
                className="text-button"
                type="button"
                onClick={() => {
                  setDraft(postalCode ?? "");
                  setError(null);
                  setEditing(false);
                }}
              >
                Cancel
              </button>
              <button className="button pill" type="submit" disabled={isSaving}>
                {isSaving ? "Saving…" : "Save"}
              </button>
            </div>
          </form>
        ) : (
          <>
            <span className="initials" aria-hidden="true">
              <MapPin size={15} />
            </span>
            <div className="vault-data-copy">
              <strong>{postalCode ?? "Add your home ZIP"}</strong>
              <p>Florence uses this to find useful things near your family.</p>
            </div>
            <div className="row-actions">
              <button
                className="text-button"
                type="button"
                onClick={() => {
                  setError(null);
                  setEditing(true);
                }}
              >
                {postalCode ? "Correct" : "Add"}
              </button>
            </div>
          </>
        )}
        {error && <p className="form-error">{error}</p>}
      </div>
    </div>
  );
}

export function PreferencesPage() {
  const query = useWorkspace();

  if (query.isLoading) return <PageLoader />;
  if (query.isError) return <LoadError error={query.error} />;
  if (!query.data) return <PageLoader />;

  return (
    <Page title="Preferences" intro="Choose what Florence can do for you. You can change these anytime.">
      <PreferencesEditor initial={query.data.preferences} />
      <section className="preference-group">
        <SectionLabel>Your Google account</SectionLabel>
        <GoogleConnector view={query.data} />
      </section>
    </Page>
  );
}

function PreferencesEditor({ initial }: { initial: PreferencesInput }) {
  const save = usePutPreferences();
  const [draft, setDraft] = useState(initial);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => setDraft(initial), [initial]);

  function changePermission(
    permission: keyof Pick<
      PreferencesInput,
      "proactiveGoogleEnabled" | "automaticFamilyCalendarEnabled" | "privateConflictBusySharingEnabled"
    >,
    enabled: boolean,
  ) {
    setDraft((current) => ({ ...current, [permission]: enabled }));
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      await save.mutateAsync(draft);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not save your preferences.");
    }
  }

  return (
    <form onSubmit={(event) => void submit(event)}>
      <section className="preference-group">
        <SectionLabel>What Florence does automatically</SectionLabel>
        <div className="preference-card permission-options">
          <PermissionSetting
            title="Keep an eye on Gmail and Calendar"
            detail="Florence looks for school updates, deadlines, schedule changes, and loose ends, then texts when something needs attention. Turn this off to stop automatic checks; you can still ask Florence for help."
            checked={draft.proactiveGoogleEnabled}
            onChange={(enabled) => changePermission("proactiveGoogleEnabled", enabled)}
          />
          <PermissionSetting
            title="Add clear dates to the family calendar"
            detail="Florence may automatically add a school, activity, appointment, or family-travel date when an official source is completely clear. If anything is uncertain, she asks in the family chat first. This works only while both parents leave it on."
            checked={draft.automaticFamilyCalendarEnabled}
            onChange={(enabled) => changePermission("automaticFamilyCalendarEnabled", enabled)}
          />
          <PermissionSetting
            title="Mention when I have a private conflict"
            detail="When a family plan clashes with your personal calendar, Florence may tell the family only that you’re busy. She sends the event name and other private details to you, not the group."
            checked={draft.privateConflictBusySharingEnabled}
            onChange={(enabled) => changePermission("privateConflictBusySharingEnabled", enabled)}
          />
        </div>
      </section>

      <section className="preference-group preference-save-row">
        {error && <p className="form-error">{error}</p>}
        <button className="button primary" type="submit" disabled={save.isPending}>
          {save.isPending ? "Saving…" : "Save preferences"}
        </button>
      </section>
    </form>
  );
}

function PermissionSetting({
  title,
  detail,
  checked,
  onChange,
}: {
  title: string;
  detail: string;
  checked: boolean;
  onChange: (enabled: boolean) => void;
}) {
  return (
    <label className="permission-setting">
      <span className="permission-setting-copy">
        <strong>{title}</strong>
        <span>{detail}</span>
      </span>
      <input type="checkbox" checked={checked} onChange={(event) => onChange(event.target.checked)} />
    </label>
  );
}

function GoogleConnector({ view }: { view: WorkspaceView }) {
  const start = useStartGoogleConnection();
  const disconnect = useDisconnectGoogleConnection();
  const deleteGoogleData = useDeleteGoogleDerivedData();
  const accounts = view.workspace.googleConnections;
  const [confirmation, setConfirmation] = useState<
    { kind: "disconnect"; connectionId: string; emailLabel: string } | { kind: "delete" } | null
  >(null);
  const [resultNotice, setResultNotice] = useState<string | null>(null);
  const error = start.error ?? disconnect.error ?? deleteGoogleData.error;
  const isPending = disconnect.isPending || deleteGoogleData.isPending;

  async function connect() {
    try {
      const result = await start.mutateAsync();
      window.location.assign(result.authorizationUrl);
    } catch {
      return;
    }
  }

  async function confirmAction() {
    if (!confirmation) return;
    try {
      if (confirmation.kind === "disconnect") {
        const result = await disconnect.mutateAsync(confirmation.connectionId);
        setConfirmation(null);
        setResultNotice(googleActionResult("disconnect", result.providerRevocation));
        return;
      }
      const result = await deleteGoogleData.mutateAsync();
      setConfirmation(null);
      setResultNotice(googleActionResult("delete", result.providerRevocation));
    } catch {
      return;
    }
  }

  return (
    <article className="connector-card">
      <div className="google-mark" aria-hidden="true">
        G
      </div>
      <div className="connector-copy">
        <strong>Your Google Workspace</strong>
        <p>
          {accounts.length
            ? accounts.map((account) => account.emailLabel).join(", ")
            : "Connect the Gmail and Calendar account you want Florence to use for you."}
        </p>
        {error && <p className="form-error">{error.message}</p>}
      </div>
      <div className="connector-actions">
        {!accounts.length && (
          <button
            className="button pill"
            type="button"
            onClick={() => void connect()}
            disabled={start.isPending}
          >
            {start.isPending ? "Opening…" : "Connect"}
          </button>
        )}
        {accounts.map((account) => (
          <button
            className="text-button danger"
            type="button"
            key={account.connectionId}
            onClick={() => {
              setResultNotice(null);
              setConfirmation({
                kind: "disconnect",
                connectionId: account.connectionId,
                emailLabel: account.emailLabel,
              });
            }}
            disabled={isPending}
          >
            Disconnect
          </button>
        ))}
        <button
          className="text-button danger google-delete-trigger"
          type="button"
          onClick={() => {
            setResultNotice(null);
            setConfirmation({ kind: "delete" });
          }}
          disabled={isPending}
        >
          Delete Google-derived data
        </button>
      </div>
      {confirmation && (
        <GoogleActionConfirmation
          kind={confirmation.kind}
          emailLabel={confirmation.kind === "disconnect" ? confirmation.emailLabel : undefined}
          isPending={isPending}
          onCancel={() => setConfirmation(null)}
          onConfirm={() => void confirmAction()}
        />
      )}
      {resultNotice && (
        <p className="google-action-success connector-result" role="status">
          {resultNotice}
        </p>
      )}
    </article>
  );
}

function GoogleDataDeletionControl() {
  const deleteGoogleData = useDeleteGoogleDerivedData();
  const [isConfirming, setIsConfirming] = useState(false);
  const [resultNotice, setResultNotice] = useState<string | null>(null);

  async function confirmDelete() {
    try {
      const result = await deleteGoogleData.mutateAsync();
      setIsConfirming(false);
      setResultNotice(googleActionResult("delete", result.providerRevocation));
    } catch {
      return;
    }
  }

  return (
    <div className="google-data-control google-data-control-setup">
      {resultNotice ? (
        <p className="google-action-success" role="status">
          {resultNotice}
        </p>
      ) : isConfirming ? (
        <GoogleActionConfirmation
          kind="delete"
          isPending={deleteGoogleData.isPending}
          onCancel={() => setIsConfirming(false)}
          onConfirm={() => void confirmDelete()}
        />
      ) : (
        <button
          className="setup-secondary-action google-delete-trigger"
          type="button"
          onClick={() => {
            setResultNotice(null);
            setIsConfirming(true);
          }}
        >
          Delete Google-derived data
        </button>
      )}
      {deleteGoogleData.error && (
        <p className="form-error" role="alert">
          {deleteGoogleData.error.message}
        </p>
      )}
    </div>
  );
}

function googleActionResult(
  kind: "disconnect" | "delete",
  providerRevocation: GoogleProviderRevocation,
): string {
  const result =
    kind === "delete" ? "Florence’s retained Google-derived data was deleted." : "Google disconnected.";
  const localAccess =
    kind === "delete" ? " Any local Google access was removed" : " Florence’s local access was removed";
  const providerResult =
    providerRevocation === "confirmed"
      ? `${localAccess}, and Google confirmed the separate revoke.`
      : providerRevocation === "unconfirmed"
        ? `${localAccess}, though Google did not confirm the separate revoke.`
        : `${localAccess}; there was no Google token left to revoke.`;
  const unchanged = kind === "delete" ? " Messages already sent and shared-calendar events remain." : "";
  return `${result}${providerResult}${unchanged}`;
}

function GoogleActionConfirmation({
  kind,
  emailLabel,
  isPending,
  onCancel,
  onConfirm,
}: {
  kind: "disconnect" | "delete";
  emailLabel?: string | undefined;
  isPending: boolean;
  onCancel: () => void;
  onConfirm: () => void;
}) {
  const isDelete = kind === "delete";
  const titleId = `google-${kind}-title`;
  const detailId = `google-${kind}-detail`;

  return (
    <section
      className={`google-action-confirmation${isDelete ? " is-destructive" : ""}`}
      role="alertdialog"
      aria-modal="false"
      aria-labelledby={titleId}
      aria-describedby={detailId}
    >
      <div className="google-action-copy">
        <strong id={titleId}>
          {isDelete
            ? "Delete Google-derived data?"
            : emailLabel
              ? `Disconnect ${emailLabel}?`
              : "Disconnect Google?"}
        </strong>
        {isDelete ? (
          <p id={detailId}>
            This disconnects Google and permanently deletes Florence’s retained facts, watches, and source
            details from Gmail and Calendar, plus queued updates and actions based on that Google data.
            Messages already sent and events already added to the shared calendar remain.
          </p>
        ) : (
          <div id={detailId}>
            <p>
              Florence will immediately stop reading new Gmail and Calendar information and cancel queued
              updates based on Google.
            </p>
            <p>
              Previously retained facts and source details remain, along with Messages already sent and
              changes already made to the shared calendar. Florence’s local access is removed even if Google
              does not confirm its separate revocation request.
            </p>
          </div>
        )}
      </div>
      <div className="google-confirmation-actions">
        <button className="button" type="button" onClick={onCancel} disabled={isPending}>
          Cancel
        </button>
        <button
          className={`button${isDelete ? " danger" : " primary"}`}
          type="button"
          onClick={onConfirm}
          disabled={isPending}
        >
          {isPending
            ? isDelete
              ? "Deleting…"
              : "Disconnecting…"
            : isDelete
              ? "Delete permanently"
              : "Disconnect Google"}
        </button>
      </div>
    </section>
  );
}

function WatchList({
  watches,
  timeZone,
  isSaving,
  onUpdate,
  onDelete,
}: {
  watches: VaultWatch[];
  timeZone: string;
  isSaving: boolean;
  onUpdate: (workId: string, input: PatchWatchInput) => Promise<unknown>;
  onDelete: (workId: string) => Promise<unknown>;
}) {
  if (!watches.length) {
    return (
      <EmptyVaultRow
        title="Nothing being watched right now"
        detail="When Florence keeps an eye on a deadline or family interest, it’ll appear here."
      />
    );
  }
  return (
    <div className="watch-list">
      {watches.map((watch) => (
        <WatchRow
          key={watch.workId}
          watch={watch}
          timeZone={timeZone}
          isSaving={isSaving}
          onUpdate={onUpdate}
          onDelete={onDelete}
        />
      ))}
    </div>
  );
}

function WatchRow({
  watch,
  timeZone,
  isSaving,
  onUpdate,
  onDelete,
}: {
  watch: VaultWatch;
  timeZone: string;
  isSaving: boolean;
  onUpdate: (workId: string, input: PatchWatchInput) => Promise<unknown>;
  onDelete: (workId: string) => Promise<unknown>;
}) {
  const [error, setError] = useState<string | null>(null);

  async function toggleStatus() {
    setError(null);
    try {
      await onUpdate(watch.workId, {
        kind: watch.kind,
        status: watch.status === "active" ? "paused" : "active",
      });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not change this watch.");
    }
  }

  async function stop() {
    if (!window.confirm("Stop watching this? Florence will remove it from the Vault and stop checking.")) {
      return;
    }
    setError(null);
    try {
      await onDelete(watch.workId);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not stop watching this.");
    }
  }

  return (
    <article className="watch-card">
      <div className="watch-card-heading">
        <div className="watch-badges">
          <span className="watch-badge">
            {watch.kind === "monitor" ? "One-time watch" : "Family interest"}
          </span>
          <span className="watch-badge">
            {watch.visibility === "private" ? "Private to you" : "Shared with the household"}
          </span>
          <span className={`watch-badge${watch.status === "paused" ? " paused" : ""}`}>
            {watch.status === "paused" ? "Paused" : "Active"}
          </span>
        </div>
      </div>

      <strong className="watch-objective">{watch.objective}</strong>
      <p className="watch-conclusion">{watch.currentConclusion ?? "Florence is keeping an eye on this."}</p>
      <p className="watch-source">
        {watch.source
          ? `Based on ${watch.source.label} · ${watchTime(watch.source.occurredAt, timeZone)}`
          : "The original source is no longer available."}
      </p>
      <div className="watch-actions">
        <button className="text-button" type="button" onClick={() => void toggleStatus()} disabled={isSaving}>
          {watch.status === "active" ? <Pause size={13} /> : <Play size={13} />}
          {watch.status === "active" ? "Pause" : "Resume"}
        </button>
        <button className="text-button danger" type="button" onClick={() => void stop()} disabled={isSaving}>
          <Trash2 size={13} /> Stop
        </button>
      </div>
      {error && <p className="form-error">{error}</p>}
    </article>
  );
}

function FactList({
  facts,
  isSaving,
  onCorrect,
  onDelete,
}: {
  facts: VaultFact[];
  isSaving: boolean;
  onCorrect: (factId: string, input: PatchFactInput) => Promise<unknown>;
  onDelete: (factId: string) => Promise<unknown>;
}) {
  if (!facts.length) {
    return (
      <EmptyVaultRow
        title="No retained facts yet"
        detail="Useful facts Florence remembers will appear here."
      />
    );
  }
  return (
    <div className="vault-data-list">
      {facts.map((fact) => (
        <FactRow key={fact.id} fact={fact} isSaving={isSaving} onCorrect={onCorrect} onDelete={onDelete} />
      ))}
    </div>
  );
}

function FactRow({
  fact,
  isSaving,
  onCorrect,
  onDelete,
}: {
  fact: VaultFact;
  isSaving: boolean;
  onCorrect: (factId: string, input: PatchFactInput) => Promise<unknown>;
  onDelete: (factId: string) => Promise<unknown>;
}) {
  const [editing, setEditing] = useState(false);
  const [statement, setStatement] = useState(fact.statement);
  const [error, setError] = useState<string | null>(null);

  async function correct(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      await onCorrect(fact.id, { statement: statement.trim() });
      setEditing(false);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not correct this fact.");
    }
  }

  async function remove() {
    if (!window.confirm("Delete this fact from Florence's memory?")) return;
    setError(null);
    try {
      await onDelete(fact.id);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not delete this fact.");
    }
  }

  return (
    <div className="vault-data-row">
      {editing ? (
        <form className="fact-editor" onSubmit={(event) => void correct(event)}>
          <label className="field">
            <span>What Florence should remember</span>
            <input value={statement} onChange={(event) => setStatement(event.target.value)} required />
          </label>
          <div className="row-actions">
            <button className="text-button" type="button" onClick={() => setEditing(false)}>
              Cancel
            </button>
            <button className="button pill" type="submit" disabled={isSaving || !statement.trim()}>
              Save correction
            </button>
          </div>
        </form>
      ) : (
        <>
          <div className="vault-data-copy">
            <strong>{fact.statement}</strong>
            <p>{sourceSummary(fact.visibility, fact.source.label)}</p>
          </div>
          <div className="row-actions">
            {fact.editable && (
              <button className="text-button" type="button" onClick={() => setEditing(true)}>
                Correct
              </button>
            )}
            {fact.deletable && (
              <button
                className="text-button danger"
                type="button"
                onClick={() => void remove()}
                disabled={isSaving}
              >
                <Trash2 size={13} /> Delete
              </button>
            )}
          </div>
        </>
      )}
      {error && <p className="form-error">{error}</p>}
    </div>
  );
}

function PeopleList({
  members,
  onEdit,
}: {
  members: FamilyMemberProfile[];
  onEdit: (member: FamilyMemberProfile) => void;
}) {
  if (!members.length)
    return <EmptyVaultRow title="None added yet" detail="Add the people Florence should know." />;
  return (
    <div className="people-list">
      {members.map((member) => (
        <button className="person-row" type="button" key={member.id} onClick={() => onEdit(member)}>
          <span className="initials">{initials(member.displayName)}</span>
          <span>
            <strong>{member.displayName}</strong>
            <small>{memberSummary(member)}</small>
          </span>
        </button>
      ))}
    </div>
  );
}

function Page({ title, intro, children }: { title: string; intro: string; children: React.ReactNode }) {
  return (
    <div className="page">
      <header className="page-heading">
        <h1>{title}</h1>
        <p>{intro}</p>
      </header>
      {children}
    </div>
  );
}

function VaultSection({
  id,
  label,
  action,
  children,
}: {
  id?: string;
  label: string;
  action?: React.ReactNode;
  children: React.ReactNode;
}) {
  return (
    <section className="vault-section" id={id}>
      <div className="vault-label-row">
        <SectionLabel>{label}</SectionLabel>
        {action}
      </div>
      {children}
    </section>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return <h2 className="section-label">{children}</h2>;
}

function EmptyVaultRow({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="empty-row">
      <div>
        <strong>{title}</strong>
        <p>{detail}</p>
      </div>
    </div>
  );
}

function DesktopSidebar() {
  return (
    <aside className="sidebar">
      <Brand />
      <nav aria-label="Primary navigation">
        <NavItem to="/" label="Messages" />
        <NavItem to="/calendar" label="Calendar" />
        <NavItem to="/vault" label="Vault" />
      </nav>
      <AccountMenu />
    </aside>
  );
}

function MobileHeader() {
  return (
    <header className="mobile-header">
      <Brand />
      <nav aria-label="Primary navigation">
        <NavItem to="/" label="Messages" />
        <NavItem to="/calendar" label="Calendar" />
        <NavItem to="/vault" label="Vault" />
      </nav>
      <AccountMenu compact />
    </header>
  );
}

function NavItem({ to, label }: { to: "/" | "/calendar" | "/vault" | "/preferences"; label: string }) {
  return (
    <Link
      className="nav-link"
      activeProps={{ className: "nav-link active" }}
      activeOptions={{ exact: to === "/" }}
      to={to}
    >
      {label}
    </Link>
  );
}

function AccountMenu({ compact = false }: { compact?: boolean }) {
  const query = useWorkspace();
  const signOut = useDeleteSession();
  const displayName = query.data?.viewer.displayName ?? "Account";

  return (
    <details className="account-menu">
      <summary aria-label={`${displayName} account menu`}>
        <span className="account-avatar">{initials(displayName)}</span>
        {!compact && <span className="account-name">{displayName}</span>}
      </summary>
      <div className="account-popover">
        <span className="account-popover-name">{displayName}</span>
        <Link className="account-menu-item" to="/preferences">
          Preferences
        </Link>
        <button
          className="account-menu-item"
          type="button"
          onClick={() => void signOut.mutateAsync()}
          disabled={signOut.isPending}
        >
          {signOut.isPending ? "Signing out…" : "Sign out"}
        </button>
      </div>
    </details>
  );
}

function Brand() {
  return (
    <Link className="brand" to="/">
      <span className="brand-mark">F</span>
      <span>Florence</span>
    </Link>
  );
}

function PageLoader() {
  return (
    <main className="state-page">
      <Brand />
      <p>Opening Florence…</p>
    </main>
  );
}

function LoadError({ error }: { error: Error }) {
  return (
    <main className="state-page error-state">
      <Brand />
      <strong>Florence could not open this page.</strong>
      <p>{error.message}</p>
    </main>
  );
}

function sourceSummary(visibility: "private" | "household", source: string) {
  return `${visibility === "private" ? "Private to you" : "Shared with the household"} · ${source}`;
}

function watchTime(value: string | null, timeZone: string) {
  if (!value) return "not yet";
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
    timeZone,
  }).format(new Date(value));
}

function currentCalendarMonth(timeZone?: string): string {
  return currentCalendarDate(timeZone).slice(0, 7);
}

function currentCalendarDate(timeZone = detectedTimeZone()): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    timeZone,
  }).formatToParts(new Date());
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  return year && month && day ? `${year}-${month}-${day}` : new Date().toISOString().slice(0, 10);
}

function shiftCalendarMonth(month: string, offset: number): string {
  const { year, monthIndex } = calendarMonthParts(month);
  const next = new Date(Date.UTC(year, monthIndex + offset, 1));
  return `${next.getUTCFullYear()}-${String(next.getUTCMonth() + 1).padStart(2, "0")}`;
}

function calendarMonthLabel(month: string): string {
  const { year, monthIndex } = calendarMonthParts(month);
  return new Intl.DateTimeFormat(undefined, {
    month: "long",
    year: "numeric",
    timeZone: "UTC",
  }).format(new Date(Date.UTC(year, monthIndex, 1)));
}

function calendarMonthWeeks(month: string): CalendarMonthCell[][] {
  const { year, monthIndex } = calendarMonthParts(month);
  const leadingDays = new Date(Date.UTC(year, monthIndex, 1)).getUTCDay();
  const daysInMonth = new Date(Date.UTC(year, monthIndex + 1, 0)).getUTCDate();
  const cells: CalendarMonthCell[] = Array.from({ length: leadingDays }, (_, index) => ({
    key: `${month}-before-${index + 1}`,
    date: null,
  }));
  for (let day = 1; day <= daysInMonth; day += 1) {
    const date = `${month}-${String(day).padStart(2, "0")}`;
    cells.push({ key: date, date });
  }
  while (cells.length % 7 !== 0) {
    cells.push({ key: `${month}-after-${cells.length + 1}`, date: null });
  }

  const weeks: CalendarMonthCell[][] = [];
  for (let index = 0; index < cells.length; index += 7) weeks.push(cells.slice(index, index + 7));
  return weeks;
}

function calendarMonthParts(month: string): { year: number; monthIndex: number } {
  const [yearText, monthText] = month.split("-");
  return {
    year: Number(yearText),
    monthIndex: Number(monthText) - 1,
  };
}

function calendarDayLabel(date: string): string {
  return new Intl.DateTimeFormat(undefined, {
    month: "long",
    day: "numeric",
    timeZone: "UTC",
  }).format(calendarDate(date));
}

function calendarDayWeekday(date: string): string {
  return new Intl.DateTimeFormat(undefined, {
    weekday: "long",
    timeZone: "UTC",
  }).format(calendarDate(date));
}

function calendarDayAriaLabel(date: string, eventCount: number): string {
  const plans = eventCount === 1 ? "1 plan" : `${eventCount} plans`;
  return `${calendarDayWeekday(date)}, ${calendarDayLabel(date)}, ${plans}`;
}

function calendarDate(date: string): Date {
  const [yearText, monthText, dayText] = date.split("-");
  return new Date(Date.UTC(Number(yearText), Number(monthText) - 1, Number(dayText)));
}

function eventsForCalendarDate(
  events: FamilyCalendarEvent[],
  date: string,
  timeZone: string,
): FamilyCalendarEvent[] {
  return events
    .filter((event) => calendarEventIncludesDate(event, date, timeZone))
    .sort((left, right) => {
      if (left.intervalKind !== right.intervalKind) return left.intervalKind === "all_day" ? -1 : 1;
      const leftStart = left.intervalKind === "timed" ? left.startsAt : left.startDate;
      const rightStart = right.intervalKind === "timed" ? right.startsAt : right.startDate;
      return leftStart.localeCompare(rightStart) || (left.title ?? "").localeCompare(right.title ?? "");
    });
}

function calendarEventIncludesDate(event: FamilyCalendarEvent, date: string, timeZone: string): boolean {
  if (event.intervalKind === "all_day") return date >= event.startDate && date < event.endDate;
  const startsOn = dateKeyInTimeZone(new Date(event.startsAt), timeZone);
  const exclusiveEnd = new Date(event.endsAt).getTime();
  const endsOn = dateKeyInTimeZone(new Date(Math.max(exclusiveEnd - 1, 0)), timeZone);
  return date >= startsOn && date <= endsOn;
}

function dateKeyInTimeZone(value: Date, timeZone: string): string {
  const parts = new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    timeZone,
  }).formatToParts(value);
  const year = parts.find((part) => part.type === "year")?.value;
  const month = parts.find((part) => part.type === "month")?.value;
  const day = parts.find((part) => part.type === "day")?.value;
  return year && month && day ? `${year}-${month}-${day}` : value.toISOString().slice(0, 10);
}

function calendarEventKey(event: FamilyCalendarEvent, index: number): string {
  const interval =
    event.intervalKind === "timed"
      ? `${event.startsAt}:${event.endsAt}`
      : `${event.startDate}:${event.endDate}`;
  return `${event.intervalKind}:${interval}:${event.title ?? ""}:${event.location ?? ""}:${index}`;
}

function calendarEventTime(event: FamilyCalendarEvent, timeZone: string): string {
  if (event.intervalKind === "all_day") return "All day";
  const startsAt = new Date(event.startsAt);
  const endsAt = new Date(event.endsAt);
  const time = new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
    timeZone,
  });
  if (dateKeyInTimeZone(startsAt, timeZone) === dateKeyInTimeZone(endsAt, timeZone)) {
    return `${time.format(startsAt)}–${time.format(endsAt)}`;
  }
  const dateAndTime = new Intl.DateTimeFormat(undefined, {
    weekday: "short",
    hour: "numeric",
    minute: "2-digit",
    timeZone,
  });
  return `${dateAndTime.format(startsAt)}–${dateAndTime.format(endsAt)}`;
}

function memberSummary(member: FamilyMemberProfile) {
  return (
    [member.relationship, member.school].filter(Boolean).join(" · ") ||
    (member.kind === "adult" ? "Adult" : "Child")
  );
}

function familyLabelFromSurnames(founderLastName: string, partnerLastName: string): string {
  const surnames = [founderLastName, partnerLastName]
    .map((surname) => surname.trim())
    .filter((surname, index, all) => {
      if (!surname) return false;
      return (
        all.findIndex((candidate) => candidate.toLocaleLowerCase() === surname.toLocaleLowerCase()) === index
      );
    });
  return `${surnames.join("–") || "Family"}${surnames.length ? " Family" : ""}`;
}

function initials(name: string) {
  return name
    .split(/\s+/)
    .slice(0, 2)
    .map((part) => part[0])
    .join("")
    .toUpperCase();
}

function newChildDraft(): ChildDraft {
  return { id: crypto.randomUUID(), firstName: "", lastName: "", school: "", activities: "" };
}

function listValues(value: string): string[] {
  return value
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function usPhoneDigits(value: string): string {
  return value.replace(/\D/g, "").slice(0, 10);
}

function formatUsPhoneNumber(value: string): string {
  const digits = usPhoneDigits(value);
  if (digits.length <= 3) return digits;
  if (digits.length <= 6) return `${digits.slice(0, 3)} ${digits.slice(3)}`;
  return `${digits.slice(0, 3)} ${digits.slice(3, 6)} ${digits.slice(6)}`;
}

function consumeOnboardingEntry(): {
  setupToken: string | null;
  accessToken: string | null;
  googleStatus: string | null;
  setupComplete: boolean;
} {
  const url = new URL(window.location.href);
  const fragment = new URLSearchParams(url.hash.startsWith("#") ? url.hash.slice(1) : url.hash);
  const hasSetupFragment = fragment.has("s");
  const hasAccessFragment = fragment.has("a");
  const setupToken = fragment.get("s")?.trim() || null;
  const accessToken = setupToken === null ? fragment.get("a")?.trim() || null : null;
  const googleStatus = url.searchParams.get("google")?.trim() || null;
  const setupComplete = url.searchParams.get("setup") === "complete";
  if (hasSetupFragment || hasAccessFragment) url.hash = "";
  if (url.searchParams.has("google")) url.searchParams.delete("google");
  if (url.searchParams.has("setup")) url.searchParams.delete("setup");
  if (hasSetupFragment || hasAccessFragment || googleStatus !== null || setupComplete) {
    window.history.replaceState(window.history.state, "", `${url.pathname}${url.search}${url.hash}`);
  }
  return { setupToken, accessToken, googleStatus, setupComplete };
}

function setupError(cause: unknown): string {
  if (cause instanceof FlorenceRequestError && (cause.status === 401 || cause.status === 410)) {
    return "This setup link is no longer valid. If someone invited you, ask them to have Florence send a fresh invitation. Otherwise, return to the Messages conversation where you started and ask Florence for a new link.";
  }
  if (cause instanceof FlorenceRequestError && cause.status === 409) {
    return "Florence is already set up. Return to the Messages conversation you started.";
  }
  return cause instanceof Error ? cause.message : "Florence could not finish this setup.";
}

function googleSetupError(status: string): string {
  if (status === "missing_permissions") {
    return "Google gave Florence only some of the access she needs, so Florence did not save this connection. Google may still list the partial grant: open your Google Account’s third-party connections, select Florence, and remove access there. Then try again and allow both Gmail and Calendar, or choose another account where both are available.";
  }
  if (status === "authorization_cancelled" || status === "provider_rejected") {
    return "Google wasn’t connected. Nothing changed—try again when you’re ready.";
  }
  if (status === "connected") {
    return "Florence couldn’t confirm the Google connection. Nothing changed—please try again.";
  }
  return "Florence couldn’t connect Google. Nothing changed—please try again.";
}

function detectedTimeZone(): string {
  return Intl.DateTimeFormat().resolvedOptions().timeZone || "America/Los_Angeles";
}

function isTimeZone(value: string): boolean {
  try {
    new Intl.DateTimeFormat("en-US", { timeZone: value }).format();
    return true;
  } catch {
    return false;
  }
}

function firstName(value: string): string {
  return value.trim().split(/\s+/, 1)[0] ?? value;
}
