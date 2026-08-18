import type {
  CompleteFamilyOnboardingInput,
  FamilyMemberProfile,
  PreferencesInput,
  SetupSessionInput,
  VaultContact,
  VaultFact,
  WorkspaceView,
} from "@florence/contracts";
import { Link, Outlet } from "@tanstack/react-router";
import { Check, ExternalLink, MessageCircle, Plus, Trash2 } from "lucide-react";
import { type FormEvent, useEffect, useState } from "react";
import { FlorenceRequestError } from "./api";
import { MemberEditor } from "./components/MemberEditor";
import {
  useCompleteFamilyOnboarding,
  useCreateSession,
  useDeleteFact,
  useDeleteSession,
  useDisconnectGoogleConnection,
  usePatchFact,
  usePutMember,
  usePutPreferences,
  useSession,
  useStartGoogleConnection,
  useWorkspace,
} from "./queries";

const onboardingEntry = consumeOnboardingEntry();

export function AppShell() {
  const session = useSession(onboardingEntry.setupToken === null);
  const workspace = useWorkspace(onboardingEntry.setupToken === null && session.isSuccess);
  if (onboardingEntry.setupToken) return <SetupPage setupToken={onboardingEntry.setupToken} />;
  if (session.isLoading) return <PageLoader />;
  if (session.error instanceof FlorenceRequestError && session.error.status === 401) {
    return <StartInMessagesPage />;
  }
  if (session.isError) return <LoadError error={session.error} />;
  if (workspace.isLoading) return <PageLoader />;
  if (workspace.isError) return <LoadError error={workspace.error} />;
  if (!workspace.data) return <PageLoader />;
  const googleConnected = workspace.data.workspace.googleConnections.length > 0;
  if (!googleConnected) return <GoogleSetupGate status={onboardingEntry.googleStatus} />;
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

function SetupPage({ setupToken }: { setupToken: string }) {
  const createSession = useCreateSession();
  const startGoogle = useStartGoogleConnection();
  const [step, setStep] = useState<"profile" | "permission" | "google">("profile");
  const [firstNameValue, setFirstNameValue] = useState("");
  const [lastNameValue, setLastNameValue] = useState("");
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
            title="Can Florence get ahead of things for you?"
            detail="She’ll look for school dates, schedule changes, and loose ends, then text when something needs attention."
          />
          <label className="setup-attestation">
            <input name="proactiveUseAccepted" type="checkbox" required />
            <span>Yes, Florence can keep an eye on the family details I connect.</span>
          </label>
          <label className="setup-attestation">
            <input name="guardianAttested" type="checkbox" required />
            <span>I’m a parent, guardian, or caregiver for the children I add.</span>
          </label>
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
        title="Start in Messages"
        detail="Text Florence “Hi,” then open the private setup link she sends back in that conversation."
      />
      <p className="setup-footnote">There’s no password or access code to keep track of.</p>
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
        detail="Florence just texted you. Head back to Messages to keep going."
      />
      {view.workspace.messagesUrl ? (
        <a className="button primary wide" href={view.workspace.messagesUrl}>
          Back to Messages
        </a>
      ) : (
        <p className="setup-footnote">You can close this page and return to Messages.</p>
      )}
      <a className="setup-secondary-action" href="/workspace">
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
  | { kind: "family-label" }
  | { kind: "review" };

function FamilySetupPage({ view }: { view: WorkspaceView }) {
  const complete = useCompleteFamilyOnboarding();
  const [mode, setMode] = useState<"two_adult" | "solo">("two_adult");
  const [partnerFirstName, setPartnerFirstName] = useState("");
  const [partnerLastName, setPartnerLastName] = useState("");
  const [partnerPhone, setPartnerPhone] = useState("");
  const [postalCode, setPostalCode] = useState("");
  const viewerLastName = view.viewer.displayName?.trim().split(/\s+/).at(-1) ?? "Family";
  const [familyLabel, setFamilyLabel] = useState(`${viewerLastName} Family`);
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
    const suggestedSurnames = [viewerLastName, partnerLastName.trim()].filter(Boolean);
    setFamilyLabel(`${[...new Set(suggestedSurnames)].join("–")} Family`);
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
      familyLabel: familyLabel.trim(),
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
    const input: CompleteFamilyOnboardingInput =
      mode === "two_adult"
        ? {
            ...family,
            mode: "two_adult",
            partner: {
              firstName: partnerFirstName.trim(),
              lastName: partnerLastName.trim(),
              phoneNumber: partnerPhone.trim(),
            },
          }
        : { ...family, mode: "solo", partner: null };
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
          <button
            className="setup-secondary-action"
            type="button"
            onClick={() => {
              setMode("solo");
              showScreen({ kind: "child-name", childId: children[0]?.id ?? "" });
            }}
          >
            It’s just me
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
            setMode("two_adult");
            showScreen({ kind: "child-name", childId: children[0]?.id ?? "" });
          }}
        >
          <SetupHeading
            title={`What’s ${partnerFirstName}’s number?`}
            detail="Florence won’t text them until she asks you in Messages."
          />
          <label className="field">
            <span>Mobile number</span>
            <input
              value={partnerPhone}
              onChange={(event) => setPartnerPhone(normalizePhone(event.target.value))}
              autoComplete="tel"
              inputMode="tel"
              placeholder="+1 415 555 0123"
              pattern="\+[1-9][0-9]{7,14}"
              required
            />
          </label>
          <p className="setup-footnote">Include the country code, like +1.</p>
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
            showScreen({ kind: "family-label" });
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

  if (screen.kind === "family-label") {
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
            title="What should Florence call your family?"
            detail="We suggested a name. Change it if another one feels more like you."
          />
          <label className="field">
            <span>Family name</span>
            <input
              value={familyLabel}
              onChange={(event) => setFamilyLabel(event.target.value)}
              placeholder="Anbarasu Family"
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
            detail="Florence will use these names to make sense of the family logistics you share."
          />
          <div className="setup-review-list">
            <button
              className="setup-review-row"
              type="button"
              onClick={() => showScreen({ kind: "partner" })}
            >
              <span>Partner or co-parent</span>
              <strong>
                {mode === "two_adult"
                  ? `${partnerFirstName} ${partnerLastName} · ${partnerPhone}`
                  : "It’s just me"}
              </strong>
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
              <span>{familyLabel}</span>
              <strong>{postalCode}</strong>
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
        Florence won’t share your private email. She’ll bring the useful family details to you, and ask before
        changing anything.
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
  useWorkspaceAppearance(query.data?.preferences.appearance);
  if (query.isLoading) return <PageLoader />;
  if (query.isError) return <LoadError error={query.error} />;
  const view = query.data;
  if (!view) return <PageLoader />;

  return (
    <Page title="Workspace" intro="The places you can reach Florence and the sources connected to you.">
      <section className="section">
        <SectionLabel>Contact Florence</SectionLabel>
        <div className="contact-grid">
          <ContactAction
            icon={<MessageCircle size={20} />}
            title="Messages"
            detail={
              view.workspace.messagesUrl
                ? "Open your conversation with Florence"
                : "Available after Messages is configured"
            }
            href={view.workspace.messagesUrl ?? undefined}
          />
        </div>
      </section>

      <section className="section" id="google-connections">
        <SectionLabel>Connections</SectionLabel>
        <GoogleConnector view={view} />
      </section>
    </Page>
  );
}

export function VaultPage() {
  const query = useWorkspace();
  const putMember = usePutMember();
  const patchFact = usePatchFact();
  const deleteFact = useDeleteFact();
  const [editing, setEditing] = useState<FamilyMemberProfile | "new" | null>(null);
  useWorkspaceAppearance(query.data?.preferences.appearance);

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

      <VaultSection
        label="Adults"
        action={
          adults.length < 2 ? (
            <button className="text-button" type="button" onClick={() => setEditing("new")}>
              <Plus size={14} /> Add
            </button>
          ) : null
        }
      >
        <PeopleList members={adults} onEdit={setEditing} />
      </VaultSection>

      <VaultSection
        label="Children"
        action={
          <button className="text-button" type="button" onClick={() => setEditing("new")}>
            <Plus size={14} /> Add
          </button>
        }
      >
        <PeopleList members={children} onEdit={setEditing} />
      </VaultSection>

      <VaultSection label="Facts">
        <FactList
          facts={vault.facts}
          isSaving={patchFact.isPending || deleteFact.isPending}
          onCorrect={(factId, statement) => patchFact.mutateAsync({ factId, input: { statement } })}
          onDelete={(factId) => deleteFact.mutateAsync(factId)}
        />
      </VaultSection>

      <VaultSection label="Addresses & phones">
        <ContactList contacts={vault.contacts} />
      </VaultSection>
    </Page>
  );
}

export function PreferencesPage() {
  const query = useWorkspace();
  useWorkspaceAppearance(query.data?.preferences.appearance);

  if (query.isLoading) return <PageLoader />;
  if (query.isError) return <LoadError error={query.error} />;
  if (!query.data) return <PageLoader />;

  return (
    <Page title="Preferences" intro="How Florence looks to you.">
      <PreferencesEditor initial={query.data.preferences} />
    </Page>
  );
}

function PreferencesEditor({ initial }: { initial: PreferencesInput }) {
  const save = usePutPreferences();
  const [draft, setDraft] = useState(initial);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => setDraft(initial), [initial]);

  function changeAppearance(appearance: PreferencesInput["appearance"]) {
    setDraft((current) => ({ ...current, appearance }));
    document.documentElement.dataset.appearance = appearance;
  }

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      await save.mutateAsync(draft);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not save your appearance.");
    }
  }

  return (
    <form onSubmit={(event) => void submit(event)}>
      <section className="preference-group">
        <SectionLabel>Appearance</SectionLabel>
        <div className="preference-card appearance-options">
          {(["light", "dark", "system"] as const).map((option) => (
            <button
              className={draft.appearance === option ? "selected" : ""}
              type="button"
              key={option}
              onClick={() => changeAppearance(option)}
            >
              <span>{capitalize(option)}</span>
              {draft.appearance === option && <Check size={15} />}
            </button>
          ))}
        </div>
      </section>

      <section className="preference-group preference-save-row">
        {error && <p className="form-error">{error}</p>}
        <button className="button primary" type="submit" disabled={save.isPending}>
          {save.isPending ? "Saving…" : "Save appearance"}
        </button>
      </section>
    </form>
  );
}

function GoogleConnector({ view }: { view: WorkspaceView }) {
  const start = useStartGoogleConnection();
  const disconnect = useDisconnectGoogleConnection();
  const accounts = view.workspace.googleConnections;
  const error = start.error ?? disconnect.error;

  async function connect() {
    const result = await start.mutateAsync();
    window.location.assign(result.authorizationUrl);
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
            onClick={() => void disconnect.mutateAsync(account.connectionId)}
            disabled={disconnect.isPending}
          >
            Disconnect
          </button>
        ))}
      </div>
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
  onCorrect: (factId: string, statement: string) => Promise<unknown>;
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
  onCorrect: (factId: string, statement: string) => Promise<unknown>;
  onDelete: (factId: string) => Promise<unknown>;
}) {
  const [editing, setEditing] = useState(false);
  const [statement, setStatement] = useState(fact.statement);
  const [error, setError] = useState<string | null>(null);

  async function correct(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      await onCorrect(fact.id, statement.trim());
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

function ContactList({ contacts }: { contacts: VaultContact[] }) {
  if (!contacts.length) {
    return (
      <EmptyVaultRow
        title="No household contact details"
        detail="Addresses and phone numbers Florence retains will stay source-linked here."
      />
    );
  }
  return (
    <div className="vault-data-list">
      {contacts.map((contact) => (
        <div className="vault-data-row" key={contact.id}>
          <div className="vault-data-copy">
            <strong>{contact.label}</strong>
            <p>{contact.value}</p>
            <small>{sourceSummary(contact.visibility, contact.source.label)}</small>
          </div>
        </div>
      ))}
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

function ContactAction({
  icon,
  title,
  detail,
  href,
}: {
  icon: React.ReactNode;
  title: string;
  detail: string;
  href?: string | undefined;
}) {
  const action = href ? (
    <a className="contact-button" href={href}>
      Open <ExternalLink size={14} />
    </a>
  ) : (
    <span className="unavailable">Not configured</span>
  );

  return (
    <article className="contact-card">
      <span className="contact-icon">{icon}</span>
      <div>
        <strong>{title}</strong>
        <p>{detail}</p>
      </div>
      {action}
    </article>
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
        <NavItem to="/" label="Workspace" />
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
        <NavItem to="/" label="Workspace" />
        <NavItem to="/vault" label="Vault" />
      </nav>
      <AccountMenu compact />
    </header>
  );
}

function NavItem({ to, label }: { to: "/" | "/vault" | "/preferences"; label: string }) {
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

function useWorkspaceAppearance(appearance: PreferencesInput["appearance"] | undefined) {
  useEffect(() => {
    if (appearance) document.documentElement.dataset.appearance = appearance;
  }, [appearance]);
}

function sourceSummary(visibility: "private" | "household", source: string) {
  return `${visibility === "private" ? "Private to you" : "Shared with the household"} · ${source}`;
}

function memberSummary(member: FamilyMemberProfile) {
  return (
    [member.relationship, member.school, member.currentGrade].filter(Boolean).join(" · ") ||
    (member.kind === "adult" ? "Adult" : "Child")
  );
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

function normalizePhone(value: string): string {
  const digits = value.replace(/\D/g, "");
  return digits ? `+${digits.slice(0, 15)}` : "";
}

function consumeOnboardingEntry(): {
  setupToken: string | null;
  googleStatus: string | null;
  setupComplete: boolean;
} {
  const url = new URL(window.location.href);
  const fragment = new URLSearchParams(url.hash.startsWith("#") ? url.hash.slice(1) : url.hash);
  const hasSetupFragment = fragment.has("s") || fragment.has("setup");
  const compactSetupToken = fragment.get("s")?.trim() || null;
  const legacySetupToken = fragment.get("setup")?.trim() || null;
  const setupToken = compactSetupToken && legacySetupToken ? null : (compactSetupToken ?? legacySetupToken);
  const googleStatus = url.searchParams.get("google")?.trim() || null;
  const setupComplete = url.searchParams.get("setup") === "complete";
  if (hasSetupFragment) url.hash = "";
  if (url.searchParams.has("google")) url.searchParams.delete("google");
  if (url.searchParams.has("setup")) url.searchParams.delete("setup");
  if (hasSetupFragment || googleStatus !== null || setupComplete) {
    window.history.replaceState(window.history.state, "", `${url.pathname}${url.search}${url.hash}`);
  }
  return { setupToken, googleStatus, setupComplete };
}

function setupError(cause: unknown): string {
  if (cause instanceof FlorenceRequestError && (cause.status === 401 || cause.status === 410)) {
    return "This setup link is no longer valid. If setup has not started, text Florence “new link” in the same thread. If you already submitted your name, continue in the same browser.";
  }
  if (cause instanceof FlorenceRequestError && cause.status === 409) {
    return "Florence is already set up. Return to the Messages conversation you started.";
  }
  return cause instanceof Error ? cause.message : "Florence could not finish this setup.";
}

function googleSetupError(status: string): string {
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

function capitalize(value: string) {
  return value[0]?.toUpperCase() + value.slice(1);
}
