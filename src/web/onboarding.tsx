import { type ReactNode, useCallback, useEffect, useState } from "react";
import { NavLink, useLocation } from "react-router-dom";
import { ApiError, getJson, type OnboardingStep, type OnboardingView, postJson, type Viewer } from "./api.js";

export function OnboardingPage({ viewer, onCompleted }: { viewer: Viewer; onCompleted: () => void }) {
  const [data, setData] = useState<OnboardingView | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState(false);
  const [sessionExpired, setSessionExpired] = useState(false);
  const [busy, setBusy] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [previewStep, setPreviewStep] = useState<OnboardingStep | null>(null);
  const location = useLocation();

  const load = useCallback(async (showLoading = true) => {
    if (showLoading) setLoading(true);
    try {
      const next = await getJson<OnboardingView>("/api/onboarding");
      setData(next);
      setLoadError(false);
      setSessionExpired(false);
      return next;
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401) setSessionExpired(true);
      else setLoadError(true);
      return null;
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  useEffect(() => {
    if (data?.completed) onCompleted();
  }, [data?.completed, onCompleted]);

  useEffect(() => {
    const query = new URLSearchParams(location.search);
    if (query.get("connected") === "1") {
      setNotice("Google is connected. Florence will begin with your newest mail and calendars.");
      window.history.replaceState({}, "", "/onboarding");
    } else if (query.get("google") === "cancelled") {
      setNotice("Google was not connected. You can try again or choose not now.");
      window.history.replaceState({}, "", "/onboarding");
    }
  }, [location.search]);

  async function save(path: string, body: unknown, success?: string) {
    setBusy(path);
    setActionError(null);
    setNotice(null);
    try {
      await postJson(path, viewer.csrfToken, body);
      const next = await load(false);
      setPreviewStep(null);
      if (success) setNotice(success);
      return next !== null;
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401) {
        setSessionExpired(true);
      } else if (reason instanceof ApiError && reason.status === 409) {
        await load(false);
        setActionError("Something changed while you were here. Florence refreshed the latest details.");
      } else {
        setActionError("That didn’t save. Nothing changed—please try again.");
      }
      return false;
    } finally {
      setBusy(null);
    }
  }

  async function inviteCoordinator(
    invitee: OnboardingView["eligibleInvitees"][number],
    proposedDisplayName: string,
    role: "steward" | "caregiver",
  ) {
    if (!data?.household) return false;
    const path = `/api/households/${data.household.id}/invitations`;
    const body = {
      conversationId: invitee.conversationId,
      expectedParticipantEpochId: invitee.participantEpochId,
      expectedParticipantDigest: invitee.participantDigest,
      inviteeIdentityId: invitee.identityId,
      inviteePersonId: invitee.personId,
      proposedDisplayName,
      role,
    };
    setBusy(path);
    setActionError(null);
    setNotice(null);
    try {
      await postJson(path, viewer.csrfToken, body);
      await load(false);
      setNotice(`Florence sent ${proposedDisplayName} a private invitation.`);
      return true;
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401 && role === "steward") {
        try {
          await postJson("/api/safety/request-step-up", viewer.csrfToken, {
            purpose: "household_invitation",
            context: {
              action: "invite",
              householdId: data.household.id,
              conversationId: invitee.conversationId,
              expectedParticipantEpochId: invitee.participantEpochId,
              expectedParticipantDigest: invitee.participantDigest,
              inviteeIdentityId: invitee.identityId,
              inviteePersonId: invitee.personId,
              proposedDisplayName,
              role: "steward",
            },
          });
          setNotice(
            "Florence sent you a private confirmation. Open it, review the exact invitation, and confirm it there.",
          );
        } catch (stepUpReason) {
          if (stepUpReason instanceof ApiError && stepUpReason.status === 401) setSessionExpired(true);
          else setActionError("Florence couldn’t send the private confirmation. Please try again.");
        }
      } else if (reason instanceof ApiError && reason.status === 401) {
        setSessionExpired(true);
      } else if (reason instanceof ApiError && reason.status === 409) {
        await load(false);
        setActionError("That group changed. Florence refreshed the people who are available to invite.");
      } else {
        setActionError("The invitation didn’t send. Nothing changed—please try again.");
      }
      return false;
    } finally {
      setBusy(null);
    }
  }

  if (loading) return <OnboardingLoading />;
  if (sessionExpired) return <OnboardingExpired />;
  if (loadError || !data) {
    return (
      <OnboardingProblem
        title="Florence couldn’t open setup"
        copy="Your information is safe. Try loading this page again."
        onRetry={() => void load()}
      />
    );
  }

  const step = previewStep ?? data.step;
  const previous = previousOnboardingStep(data, step);
  const household = data.household;
  const isEditingEarlierStep = previewStep !== null;
  let content: ReactNode;

  if (step === "confirm_profile") {
    content = (
      <ProfileOnboardingStep
        person={data.person}
        busy={busy !== null}
        onContinue={(profile) =>
          save("/api/onboarding/profile", {
            displayName: profile.displayName,
            timeZone: profile.timeZone,
          })
        }
      />
    );
  } else if (step === "create_household") {
    content = <HouseholdStartStep busy={busy !== null} onContinue={() => save("/api/households", {})} />;
  } else if (step === "choose_household") {
    content = (
      <HouseholdChoiceStep
        choices={data.householdChoices}
        busy={busy !== null}
        onContinue={(householdId) => save("/api/onboarding/select-household", { householdId })}
      />
    );
  } else if (step === "coordinator" && household) {
    content = (
      <CoordinatorStep
        coordinator={household.coordinator}
        busy={busy !== null}
        onContinue={(choice) =>
          save("/api/onboarding/coordinator", {
            householdId: household.id,
            expectedMembershipVersion: household.versions.membership,
            expectedIntakeVersion: household.versions.intake,
            ...choice,
          })
        }
      />
    );
  } else if (step === "children" && household) {
    content = (
      <ChildrenStep
        key={`${household.id}:${household.versions.intake}`}
        household={household}
        busy={busy !== null}
        onAdd={(draft) => save(`/api/households/${household.id}/dependents`, dependentPayload(draft))}
        onUpdate={(childId, draft) =>
          save(`/api/households/${household.id}/dependents/${childId}`, {
            ...dependentPayload(draft),
            expectedRosterVersion: household.versions.roster,
            expectedIntakeVersion: household.versions.intake,
          })
        }
        onFinished={() =>
          isEditingEarlierStep
            ? Promise.resolve(setPreviewStep(null)).then(() => true)
            : save("/api/onboarding/children-reviewed", {
                householdId: household.id,
                expectedMembershipVersion: household.versions.membership,
                expectedIntakeVersion: household.versions.intake,
              })
        }
      />
    );
  } else if (step === "coordinator_invite" && household) {
    content = (
      <CoordinatorInviteStep
        household={household}
        invitees={data.eligibleInvitees}
        busy={busy !== null}
        onRefresh={() => load(false).then((next) => next !== null)}
        onInvite={inviteCoordinator}
        onLater={() =>
          save("/api/onboarding/coordinator-defer", {
            householdId: household.id,
            expectedMembershipVersion: household.versions.membership,
            expectedIntakeVersion: household.versions.intake,
          })
        }
      />
    );
  } else if (step === "review_shared_context" && household) {
    content = (
      <SharedContextStep
        household={household}
        busy={busy !== null}
        onContinue={() =>
          isEditingEarlierStep
            ? Promise.resolve(setPreviewStep(null)).then(() => true)
            : save("/api/onboarding/shared-review", {
                householdId: household.id,
                expectedMembershipVersion: household.versions.membership,
                expectedIntakeVersion: household.versions.intake,
                expectedMembershipOnboardingVersion: household.versions.membershipOnboarding,
              })
        }
      />
    );
  } else if (step === "google") {
    content = (
      <GoogleOnboardingStep
        viewer={viewer}
        google={data.google}
        busy={busy !== null}
        editing={isEditingEarlierStep}
        onContinue={() => {
          if (isEditingEarlierStep) setPreviewStep(null);
          else void load(false);
        }}
        onRequestLink={() =>
          save(
            "/api/safety/request-step-up",
            {
              purpose: "google_connect",
              context: { profile: "personal_family", returnPath: "/onboarding" },
            },
            "Florence sent a private, secure continuation link in iMessage.",
          )
        }
        onSkip={() => save("/api/onboarding/google-skip", {})}
      />
    );
  } else if (step === "review" && household) {
    content = (
      <OnboardingReviewStep
        view={data}
        busy={busy !== null}
        onComplete={() =>
          save("/api/onboarding/complete", {
            householdId: household.id,
            expectedMembershipVersion: household.versions.membership,
            expectedIntakeVersion: household.versions.intake,
            expectedMembershipOnboardingVersion: household.versions.membershipOnboarding,
            expectedProfileReviewVersion: data.person.profileReviewVersion,
          })
        }
      />
    );
  } else if (step === "complete") {
    content = <OnboardingCompleteStep onContinue={onCompleted} />;
  } else {
    content = (
      <OnboardingProblem
        embedded
        title="Florence is catching up"
        copy="Your setup is saved. Refresh to continue from the right place."
        onRetry={() => void load()}
      />
    );
  }

  return (
    <OnboardingFrame progress={data.progress}>
      {previous ? (
        <button type="button" className="onboarding-back" onClick={() => setPreviewStep(previous)}>
          <span aria-hidden="true">←</span> Back
        </button>
      ) : null}
      {notice ? (
        <div className="onboarding-notice" role="status">
          {notice}
        </div>
      ) : null}
      {actionError ? (
        <div className="onboarding-error" role="alert">
          {actionError}
        </div>
      ) : null}
      <section className="onboarding-card-main">{content}</section>
    </OnboardingFrame>
  );
}

function OnboardingFrame({
  progress,
  children,
}: {
  progress: OnboardingView["progress"];
  children: ReactNode;
}) {
  const current = Math.min(Math.max(progress.current, 1), Math.max(progress.total, 1));
  const percent = Math.round((current / Math.max(progress.total, 1)) * 100);
  return (
    <div className="onboarding-shell">
      <header className="onboarding-topbar">
        <Brand />
        <NavLink to="/safety">Privacy & account</NavLink>
      </header>
      <main className="onboarding-main">
        <div className="onboarding-progress-header">
          <span>
            Step {current} of {Math.max(progress.total, 1)}
          </span>
          <div
            className="onboarding-progress-track"
            role="progressbar"
            aria-label="Setup progress"
            aria-valuemin={1}
            aria-valuemax={Math.max(progress.total, 1)}
            aria-valuenow={current}
          >
            <span style={{ width: `${percent}%` }} />
          </div>
        </div>
        {children}
        <p className="onboarding-save-note">Florence saves each answer as you go.</p>
      </main>
    </div>
  );
}

export function OnboardingSafetyPage({ children }: { children: ReactNode }) {
  return (
    <div className="onboarding-safety-shell">
      <header className="onboarding-topbar">
        <Brand />
        <NavLink to="/onboarding">Back to setup</NavLink>
      </header>
      <main className="onboarding-safety-main">
        <header>
          <span className="section-kicker">Your account</span>
          <h1>Privacy & account</h1>
          <p>You can always reach your private data and account controls, even before setup is complete.</p>
        </header>
        {children}
      </main>
    </div>
  );
}

function ProfileOnboardingStep({
  person,
  busy,
  onContinue,
}: {
  person: OnboardingView["person"];
  busy: boolean;
  onContinue: (profile: { displayName: string; timeZone: string }) => Promise<boolean>;
}) {
  const [name, setName] = useState(person.name);
  const [timeZone, setTimeZone] = useState(person.timeZone);
  const timeZones = timeZoneChoices(person.timeZone);
  return (
    <form
      className="onboarding-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (name.trim()) void onContinue({ displayName: name.trim(), timeZone });
      }}
    >
      <OnboardingHeading
        eyebrow="First, you"
        title={`Hi${person.name ? `, ${person.name}` : ""}. Let’s get the basics right.`}
        copy="Florence will use this in private messages and when coordinating with your family."
      />
      <label className="onboarding-field">
        <span>What should Florence call you?</span>
        <input
          autoComplete="name"
          maxLength={80}
          value={name}
          onChange={(event) => setName(event.target.value)}
        />
      </label>
      <label className="onboarding-field">
        <span>Your time zone</span>
        <select value={timeZone} onChange={(event) => setTimeZone(event.target.value)}>
          {timeZones.map((zone) => (
            <option value={zone.value} key={zone.value}>
              {zone.label}
            </option>
          ))}
        </select>
        <small>This helps Florence understand when school, pickups, and reminders actually happen.</small>
      </label>
      <OnboardingPrimaryButton disabled={busy || !name.trim()} busy={busy}>
        Confirm and continue
      </OnboardingPrimaryButton>
    </form>
  );
}

function HouseholdStartStep({ busy, onContinue }: { busy: boolean; onContinue: () => Promise<boolean> }) {
  return (
    <div className="onboarding-form">
      <OnboardingHeading
        eyebrow="Your family"
        title="One place for the details everyone relies on"
        copy="Florence keeps your family’s shared basics together, while each adult’s email and account stay private."
      />
      <div className="onboarding-explainer-list">
        <span>Children, schools, and activities</span>
        <span>Partners, co-parents, and caregivers</span>
        <span>Private sources connected separately by each adult</span>
      </div>
      <OnboardingPrimaryButton disabled={busy} busy={busy} onClick={() => void onContinue()}>
        Set up my family
      </OnboardingPrimaryButton>
    </div>
  );
}

function HouseholdChoiceStep({
  choices,
  busy,
  onContinue,
}: {
  choices: OnboardingView["householdChoices"];
  busy: boolean;
  onContinue: (householdId: string) => Promise<boolean>;
}) {
  const [selected, setSelected] = useState(choices[0]?.id ?? "");
  return (
    <form
      className="onboarding-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (selected) void onContinue(selected);
      }}
    >
      <OnboardingHeading
        eyebrow="Choose a family"
        title="Which family are you setting up?"
        copy="Your Florence identity can belong to more than one family. Each family keeps its own people and context."
      />
      <div className="onboarding-choice-list">
        {choices.map((choice) => (
          <label className="onboarding-choice" key={choice.id}>
            <input
              type="radio"
              name="household"
              value={choice.id}
              checked={selected === choice.id}
              onChange={() => setSelected(choice.id)}
            />
            <span>
              <strong>{choice.name}</strong>
              <small>
                {choice.sharedIntakeComplete ? "Family details already added" : "Setup in progress"}
              </small>
            </span>
          </label>
        ))}
      </div>
      <OnboardingPrimaryButton disabled={busy || !selected} busy={busy}>
        Continue
      </OnboardingPrimaryButton>
    </form>
  );
}

function CoordinatorStep({
  coordinator,
  busy,
  onContinue,
}: {
  coordinator: NonNullable<OnboardingView["household"]>["coordinator"];
  busy: boolean;
  onContinue: (choice: {
    disposition: "solo" | "deferred" | "proposed";
    proposedName?: string;
  }) => Promise<boolean>;
}) {
  type Choice = "partner" | "caregiver" | "solo" | "later";
  const existingChoice: Choice | null =
    coordinator.disposition === "solo"
      ? "solo"
      : coordinator.disposition === "deferred"
        ? "later"
        : coordinator.disposition === "proposed" || coordinator.disposition === "pending"
          ? "partner"
          : null;
  const [choice, setChoice] = useState<Choice | null>(existingChoice);
  const [name, setName] = useState(coordinator.proposedName ?? "");
  const needsName = choice === "partner" || choice === "caregiver";
  return (
    <form
      className="onboarding-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (choice === null) return;
        if (needsName && !name.trim()) return;
        if (choice === "solo") void onContinue({ disposition: "solo" });
        else if (choice === "later") void onContinue({ disposition: "deferred" });
        else void onContinue({ disposition: "proposed", proposedName: name.trim() });
      }}
    >
      <OnboardingHeading
        eyebrow="The adults"
        title="Who else helps coordinate your family?"
        copy="You’re only telling Florence who to expect. No one is contacted from a name alone."
      />
      <div className="onboarding-choice-list">
        <ChoiceCard
          name="coordinator"
          value="partner"
          checked={choice === "partner"}
          title="A partner or co-parent"
          copy="Another parent who helps make family decisions."
          onChange={() => setChoice("partner")}
        />
        <ChoiceCard
          name="coordinator"
          value="caregiver"
          checked={choice === "caregiver"}
          title="Another caregiver"
          copy="For example, a grandparent or regular caregiver."
          onChange={() => setChoice("caregiver")}
        />
        <ChoiceCard
          name="coordinator"
          value="solo"
          checked={choice === "solo"}
          title="Just me"
          copy="I’m the only person coordinating right now."
          onChange={() => setChoice("solo")}
        />
        <ChoiceCard
          name="coordinator"
          value="later"
          checked={choice === "later"}
          title="I’ll add someone later"
          copy="Finish setup now and invite them when you’re ready."
          onChange={() => setChoice("later")}
        />
      </div>
      {needsName ? (
        <label className="onboarding-field onboarding-reveal-field">
          <span>What should Florence call them?</span>
          <input
            autoComplete="off"
            maxLength={80}
            placeholder="Their first name"
            value={name}
            onChange={(event) => setName(event.target.value)}
          />
        </label>
      ) : null}
      <OnboardingPrimaryButton disabled={busy || choice === null || (needsName && !name.trim())} busy={busy}>
        Continue
      </OnboardingPrimaryButton>
    </form>
  );
}

function ChildrenStep({
  household,
  busy,
  onAdd,
  onUpdate,
  onFinished,
}: {
  household: NonNullable<OnboardingView["household"]>;
  busy: boolean;
  onAdd: (draft: DependentDraft) => Promise<boolean>;
  onUpdate: (childId: string, draft: DependentDraft) => Promise<boolean>;
  onFinished: () => Promise<boolean>;
}) {
  const [editingId, setEditingId] = useState<string | null>(null);
  const [adding, setAdding] = useState(household.children.length === 0);
  return (
    <div className="onboarding-form">
      <OnboardingHeading
        eyebrow="Your children"
        title={
          household.children.length ? "Who else should Florence know?" : "Tell Florence about your children"
        }
        copy="One parent can add these shared basics for the whole family. The other parent won’t have to enter them again."
      />
      {household.children.length > 0 ? (
        <div className="onboarding-child-list">
          {household.children.map((child) =>
            editingId === child.id ? (
              <ChildDetailsForm
                key={child.id}
                initial={childDraft(child)}
                busy={busy}
                submitLabel="Save changes"
                onCancel={() => setEditingId(null)}
                onSave={async (draft) => {
                  const saved = await onUpdate(child.id, draft);
                  if (saved) setEditingId(null);
                  return saved;
                }}
              />
            ) : (
              <article className="onboarding-child-summary" key={child.id}>
                <Avatar name={child.name} />
                <div>
                  <strong>{child.name}</strong>
                  <span>{onboardingChildSummary(child)}</span>
                </div>
                <button type="button" onClick={() => setEditingId(child.id)}>
                  Edit
                </button>
              </article>
            ),
          )}
        </div>
      ) : null}
      {adding ? (
        <ChildDetailsForm
          initial={emptyDependentDraft()}
          busy={busy}
          submitLabel={household.children.length ? "Add child" : "Save child"}
          {...(household.children.length ? { onCancel: () => setAdding(false) } : {})}
          onSave={async (draft) => {
            const saved = await onAdd(draft);
            if (saved) setAdding(false);
            return saved;
          }}
        />
      ) : (
        <button type="button" className="onboarding-add-another" onClick={() => setAdding(true)}>
          <span aria-hidden="true">＋</span> Add another child
        </button>
      )}
      {!adding && editingId === null ? (
        <OnboardingPrimaryButton disabled={busy} busy={busy} onClick={() => void onFinished()}>
          That’s everyone
        </OnboardingPrimaryButton>
      ) : null}
    </div>
  );
}

function ChildDetailsForm({
  initial,
  busy,
  submitLabel,
  onSave,
  onCancel,
}: {
  initial: DependentDraft;
  busy: boolean;
  submitLabel: string;
  onSave: (draft: DependentDraft) => Promise<boolean>;
  onCancel?: () => void;
}) {
  const [draft, setDraft] = useState(initial);
  return (
    <form
      className="onboarding-child-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (draft.displayName.trim()) void onSave(draft);
      }}
    >
      <label className="onboarding-field">
        <span>Name</span>
        <input
          maxLength={80}
          placeholder="What do you call them?"
          value={draft.displayName}
          onChange={(event) => setDraft({ ...draft, displayName: event.target.value })}
        />
      </label>
      <div className="onboarding-field-row">
        <label className="onboarding-field">
          <span>
            Also called <small>Optional</small>
          </span>
          <input
            maxLength={300}
            placeholder="Johnny, Jonathan"
            value={draft.aliases}
            onChange={(event) => setDraft({ ...draft, aliases: event.target.value })}
          />
        </label>
        <label className="onboarding-field onboarding-birth-year">
          <span>
            Birth year <small>Optional</small>
          </span>
          <input
            type="number"
            inputMode="numeric"
            min={1900}
            max={new Date().getFullYear()}
            placeholder="2019"
            value={draft.birthYear}
            onChange={(event) => setDraft({ ...draft, birthYear: event.target.value })}
          />
        </label>
      </div>
      <label className="onboarding-field">
        <span>
          School or daycare <small>Optional</small>
        </span>
        <input
          maxLength={160}
          placeholder="School name"
          value={draft.school}
          onChange={(event) => setDraft({ ...draft, school: event.target.value })}
        />
      </label>
      <label className="onboarding-field">
        <span>
          Activities <small>Optional</small>
        </span>
        <input
          maxLength={1000}
          placeholder="Soccer, piano, swim"
          value={draft.activities}
          onChange={(event) => setDraft({ ...draft, activities: event.target.value })}
        />
        <small>Separate more than one with commas.</small>
      </label>
      <div className="onboarding-inline-actions">
        {onCancel ? (
          <button type="button" className="onboarding-secondary-button" onClick={onCancel}>
            Cancel
          </button>
        ) : null}
        <OnboardingPrimaryButton disabled={busy || !draft.displayName.trim()} busy={busy}>
          {submitLabel}
        </OnboardingPrimaryButton>
      </div>
    </form>
  );
}

function CoordinatorInviteStep({
  household,
  invitees,
  busy,
  onRefresh,
  onInvite,
  onLater,
}: {
  household: NonNullable<OnboardingView["household"]>;
  invitees: OnboardingView["eligibleInvitees"];
  busy: boolean;
  onRefresh: () => Promise<boolean>;
  onInvite: (
    invitee: OnboardingView["eligibleInvitees"][number],
    name: string,
    role: "steward" | "caregiver",
  ) => Promise<boolean>;
  onLater: () => Promise<boolean>;
}) {
  const [selectedId, setSelectedId] = useState(invitees[0]?.identityId ?? "");
  const [name, setName] = useState(household.coordinator.proposedName ?? "");
  const [role, setRole] = useState<"steward" | "caregiver">("steward");
  const selected = invitees.find((invitee) => invitee.identityId === selectedId) ?? null;
  useEffect(() => {
    if (!selectedId && invitees[0]) setSelectedId(invitees[0].identityId);
  }, [invitees, selectedId]);
  return (
    <form
      className="onboarding-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (selected && name.trim()) void onInvite(selected, name.trim(), role);
      }}
    >
      <OnboardingHeading
        eyebrow="Invite privately"
        title={`Connect Florence with ${household.coordinator.proposedName ?? "your co-coordinator"}`}
        copy="Add Florence to an iMessage group with them. Florence uses that exact group to identify the right person, then sends the invitation in a private message."
      />
      {invitees.length ? (
        <>
          <div className="onboarding-choice-list">
            {invitees.map((invitee) => (
              <label
                className="onboarding-choice"
                key={`${invitee.identityId}:${invitee.participantEpochId}`}
              >
                <input
                  type="radio"
                  name="invitee"
                  checked={selectedId === invitee.identityId}
                  onChange={() => setSelectedId(invitee.identityId)}
                />
                <span>
                  <strong>{invitee.name}</strong>
                  <small>
                    {invitee.registered ? "Already uses Florence" : "Florence will ask them privately"}
                  </small>
                </span>
              </label>
            ))}
          </div>
          <label className="onboarding-field">
            <span>Their name</span>
            <input maxLength={80} value={name} onChange={(event) => setName(event.target.value)} />
          </label>
          <label className="onboarding-field">
            <span>Their role</span>
            <select value={role} onChange={(event) => setRole(event.target.value as typeof role)}>
              <option value="steward">Partner or co-parent</option>
              <option value="caregiver">Caregiver</option>
            </select>
          </label>
          <OnboardingPrimaryButton disabled={busy || !selected || !name.trim()} busy={busy}>
            Send private invitation
          </OnboardingPrimaryButton>
        </>
      ) : (
        <div className="onboarding-empty-invite">
          <span aria-hidden="true">💬</span>
          <strong>No shared group found yet</strong>
          <p>After you add Florence to the group, come back and Florence will recognize the participants.</p>
          <button
            type="button"
            className="onboarding-secondary-button"
            disabled={busy}
            onClick={() => void onRefresh()}
          >
            Check again
          </button>
        </div>
      )}
      <button type="button" className="onboarding-text-button" disabled={busy} onClick={() => void onLater()}>
        Do this later
      </button>
    </form>
  );
}

function SharedContextStep({
  household,
  busy,
  onContinue,
}: {
  household: NonNullable<OnboardingView["household"]>;
  busy: boolean;
  onContinue: () => Promise<boolean>;
}) {
  return (
    <div className="onboarding-form">
      <OnboardingHeading
        eyebrow="Your family"
        title="The family basics are already here"
        copy="Another parent added these shared details. You don’t need to enter them again; you can correct them later if anything looks off."
      />
      <div className="onboarding-child-list">
        {household.children.map((child) => (
          <article className="onboarding-child-summary" key={child.id}>
            <Avatar name={child.name} />
            <div>
              <strong>{child.name}</strong>
              <span>{onboardingChildSummary(child)}</span>
            </div>
          </article>
        ))}
      </div>
      {household.children.length === 0 ? (
        <p className="onboarding-plain-note">
          No children have been added yet. You can add them together later.
        </p>
      ) : null}
      <OnboardingPrimaryButton disabled={busy} busy={busy} onClick={() => void onContinue()}>
        Looks right
      </OnboardingPrimaryButton>
    </div>
  );
}

function GoogleOnboardingStep({
  viewer,
  google,
  busy,
  editing,
  onContinue,
  onRequestLink,
  onSkip,
}: {
  viewer: Viewer;
  google: OnboardingView["google"];
  busy: boolean;
  editing: boolean;
  onContinue: () => void;
  onRequestLink: () => Promise<boolean>;
  onSkip: () => Promise<boolean>;
}) {
  const expiresAt = viewer.session.assuranceExpiresAt;
  const assuranceReady =
    (viewer.session.assuranceKind === "onboarding" || viewer.session.assuranceKind === "google_connect") &&
    (expiresAt === null || new Date(expiresAt) > new Date());
  if (google.decision === "connected") {
    return (
      <div className="onboarding-form">
        <OnboardingHeading
          eyebrow="Your private sources"
          title="Google is connected"
          copy="Florence starts with recent mail and calendars, then works backward quietly. Nothing is shared with your family unless you approve it."
        />
        <div className="onboarding-connected-account">
          <span>G</span>
          <div>
            <strong>{google.accountEmail ?? "Personal Google"}</strong>
            <small>Mail and Calendar · Private to you</small>
          </div>
        </div>
        {editing ? <OnboardingPrimaryButton onClick={onContinue}>Continue</OnboardingPrimaryButton> : null}
      </div>
    );
  }
  if (google.decision === "skipped" && editing) {
    return (
      <div className="onboarding-form">
        <OnboardingHeading
          eyebrow="Your private sources"
          title="Google is set for later"
          copy="You can connect personal Gmail and Calendar from Sources whenever you’re ready."
        />
        <OnboardingPrimaryButton onClick={onContinue}>Continue</OnboardingPrimaryButton>
      </div>
    );
  }
  return (
    <div className="onboarding-form">
      <OnboardingHeading
        eyebrow="Your private sources"
        title="Let Florence find the details hidden in Google"
        copy="Florence privately reviews personal Gmail and Calendar for school notices, activities, forms, and schedule changes."
      />
      <div className="onboarding-privacy-promise">
        <strong>Your account stays yours.</strong>
        <span>
          Other family members never see your email. Florence only shares family meaning after your approval.
        </span>
      </div>
      {assuranceReady ? (
        <a
          className="onboarding-primary-button"
          href="/oauth/google/start?profile=personal_family&from=onboarding"
        >
          Connect personal Google
        </a>
      ) : (
        <OnboardingPrimaryButton disabled={busy} busy={busy} onClick={() => void onRequestLink()}>
          Connect personal Google
        </OnboardingPrimaryButton>
      )}
      <button type="button" className="onboarding-text-button" disabled={busy} onClick={() => void onSkip()}>
        Not now
      </button>
    </div>
  );
}

function OnboardingReviewStep({
  view,
  busy,
  onComplete,
}: {
  view: OnboardingView;
  busy: boolean;
  onComplete: () => Promise<boolean>;
}) {
  const household = view.household as NonNullable<OnboardingView["household"]>;
  const coordinatorLabel =
    household.coordinator.disposition === "solo"
      ? "Just you for now"
      : household.coordinator.disposition === "deferred"
        ? "Add someone later"
        : (household.coordinator.proposedName ?? "Invitation in progress");
  return (
    <div className="onboarding-form">
      <OnboardingHeading
        eyebrow="Ready to begin"
        title={`Florence has the basics, ${view.person.name}`}
        copy="This is enough to start helping. You can change any of it later."
      />
      <div className="onboarding-review-list">
        <ReviewLine label="You" value={`${view.person.name} · ${friendlyTimeZone(view.person.timeZone)}`} />
        <ReviewLine label="Coordinating with" value={coordinatorLabel} />
        <ReviewLine
          label="Children"
          value={
            household.children.length ? household.children.map((child) => child.name).join(", ") : "Add later"
          }
        />
        <ReviewLine
          label="Google"
          value={
            view.google.decision === "connected"
              ? (view.google.accountEmail ?? "Connected privately")
              : "Not now"
          }
        />
      </div>
      <OnboardingPrimaryButton disabled={busy} busy={busy} onClick={() => void onComplete()}>
        Start using Florence
      </OnboardingPrimaryButton>
    </div>
  );
}

function OnboardingCompleteStep({ onContinue }: { onContinue: () => void }) {
  return (
    <div className="onboarding-form onboarding-complete-step">
      <span className="onboarding-complete-mark" aria-hidden="true">
        ✓
      </span>
      <OnboardingHeading
        eyebrow="You’re ready"
        title="Florence is on it"
        copy="Keep talking with Florence in iMessage. Your private companion now shows what needs your attention and the sources Florence is watching."
      />
      <OnboardingPrimaryButton onClick={onContinue}>Open Florence</OnboardingPrimaryButton>
    </div>
  );
}

function ChoiceCard({
  name,
  value,
  checked,
  title,
  copy,
  onChange,
}: {
  name: string;
  value: string;
  checked: boolean;
  title: string;
  copy: string;
  onChange: () => void;
}) {
  return (
    <label className="onboarding-choice">
      <input type="radio" name={name} value={value} checked={checked} onChange={onChange} />
      <span>
        <strong>{title}</strong>
        <small>{copy}</small>
      </span>
    </label>
  );
}

function OnboardingHeading({ eyebrow, title, copy }: { eyebrow: string; title: string; copy: string }) {
  return (
    <header className="onboarding-heading">
      <span>{eyebrow}</span>
      <h1>{title}</h1>
      <p>{copy}</p>
    </header>
  );
}

function OnboardingPrimaryButton({
  busy = false,
  disabled = false,
  onClick,
  children,
}: {
  busy?: boolean;
  disabled?: boolean;
  onClick?: () => void;
  children: ReactNode;
}) {
  return (
    <button
      type={onClick ? "button" : "submit"}
      className="onboarding-primary-button"
      disabled={disabled || busy}
      onClick={onClick}
    >
      {busy ? "Saving…" : children}
    </button>
  );
}

function ReviewLine({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
    </div>
  );
}

function OnboardingLoading() {
  return (
    <div className="onboarding-shell">
      <header className="onboarding-topbar">
        <Brand />
      </header>
      <main className="onboarding-main onboarding-loading-state">
        <div className="loading-dot" />
        <p>Opening your private setup…</p>
      </main>
    </div>
  );
}

function OnboardingExpired() {
  const florencePhone = document.documentElement.dataset.florencePhone;
  return (
    <OnboardingProblem
      title="This private session has ended"
      copy="Your saved setup is still here. Text Florence for a fresh private link and continue where you left off."
      {...(florencePhone
        ? { href: `sms:${florencePhone}&body=${encodeURIComponent("Send me a fresh setup link")}` }
        : {})}
      action="Text Florence"
    />
  );
}

function OnboardingProblem({
  title,
  copy,
  onRetry,
  href,
  action = "Try again",
  embedded = false,
}: {
  title: string;
  copy: string;
  onRetry?: () => void;
  href?: string;
  action?: string;
  embedded?: boolean;
}) {
  const content = (
    <div className="onboarding-form onboarding-problem">
      <OnboardingHeading eyebrow="Setup paused" title={title} copy={copy} />
      {href ? (
        <a className="onboarding-primary-button" href={href}>
          {action}
        </a>
      ) : null}
      {onRetry ? <OnboardingPrimaryButton onClick={onRetry}>{action}</OnboardingPrimaryButton> : null}
    </div>
  );
  if (embedded) return content;
  return (
    <div className="onboarding-shell">
      <header className="onboarding-topbar">
        <Brand />
        <a href="/privacy">Privacy</a>
      </header>
      <main className="onboarding-main">
        <section className="onboarding-card-main">{content}</section>
      </main>
    </div>
  );
}

function previousOnboardingStep(view: OnboardingView, step: OnboardingStep): OnboardingStep | null {
  if (step === "create_household" || step === "choose_household") return "confirm_profile";
  if (step === "coordinator")
    return view.householdChoices.length > 1 ? "choose_household" : "confirm_profile";
  if (step === "children") return "coordinator";
  if (step === "review_shared_context") return "confirm_profile";
  if (step === "google") {
    if (view.branch === "invited_adult" || view.branch === "caregiver") return "review_shared_context";
    return null;
  }
  if (step === "review") return "google";
  return null;
}

function childDraft(child: NonNullable<OnboardingView["household"]>["children"][number]): DependentDraft {
  return {
    displayName: child.name,
    aliases: child.aliases.join(", "),
    birthYear: child.birthYear === null ? "" : String(child.birthYear),
    school: child.school ?? "",
    activities: child.activities.join(", "),
  };
}

function onboardingChildSummary(child: NonNullable<OnboardingView["household"]>["children"][number]): string {
  const details = [
    child.school,
    child.activities.length ? child.activities.join(", ") : null,
    child.birthYear ? `Born ${child.birthYear}` : null,
    child.aliases.length ? `Also ${child.aliases.join(", ")}` : null,
  ].filter((detail): detail is string => Boolean(detail));
  return details.length ? details.join(" · ") : "Details can be added later";
}

function timeZoneChoices(current: string): { value: string; label: string }[] {
  const common = [
    { value: "America/Los_Angeles", label: "Pacific Time" },
    { value: "America/Denver", label: "Mountain Time" },
    { value: "America/Chicago", label: "Central Time" },
    { value: "America/New_York", label: "Eastern Time" },
  ];
  return common.some((entry) => entry.value === current)
    ? common
    : [{ value: current, label: friendlyTimeZone(current) }, ...common];
}

function friendlyTimeZone(value: string): string {
  const labels: Record<string, string> = {
    "America/Los_Angeles": "Pacific Time",
    "America/Denver": "Mountain Time",
    "America/Chicago": "Central Time",
    "America/New_York": "Eastern Time",
  };
  return labels[value] ?? value.replaceAll("_", " ").replace("America/", "");
}

interface DependentDraft {
  displayName: string;
  aliases: string;
  birthYear: string;
  school: string;
  activities: string;
}

function emptyDependentDraft(): DependentDraft {
  return { displayName: "", aliases: "", birthYear: "", school: "", activities: "" };
}

function dependentPayload(value: DependentDraft) {
  return {
    displayName: value.displayName.trim(),
    aliases: commaSeparatedValues(value.aliases),
    birthYear: value.birthYear.trim() ? Number(value.birthYear) : null,
    school: value.school.trim(),
    activities: commaSeparatedValues(value.activities),
  };
}

function commaSeparatedValues(value: string): string[] {
  return [
    ...new Set(
      value
        .split(",")
        .map((entry) => entry.trim())
        .filter(Boolean),
    ),
  ];
}

function Brand() {
  return (
    <div className="brand">
      <span className="brand-mark">F</span>
      <span>Florence</span>
    </div>
  );
}

function Avatar({ name }: { name: string }) {
  return <div className="avatar">{name.slice(0, 1).toUpperCase()}</div>;
}
