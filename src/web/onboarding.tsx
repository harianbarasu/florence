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
    } else if (query.get("google") === "stale") {
      setNotice("Your setup changed before Google finished. Nothing extra was connected.");
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

  async function connectPersonalGoogle() {
    setBusy("/oauth/google/start");
    setActionError(null);
    setNotice(null);
    try {
      const started = await postJson<{ authorizationUrl: string }>("/oauth/google/start", viewer.csrfToken, {
        profile: "personal_family",
        from: "onboarding",
        integrationId: null,
      });
      window.location.assign(started.authorizationUrl);
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401) setSessionExpired(true);
      else setActionError("Google connection couldn’t start. Nothing changed—please try again.");
      setBusy(null);
    }
  }

  async function connectAdult(
    adult: NonNullable<OnboardingView["household"]>["adults"][number],
    invitee: OnboardingView["eligibleInvitees"][number],
  ) {
    if (!data?.household) return false;
    const path = `/api/onboarding/adults/${adult.id}/invite`;
    const body = {
      householdId: data.household.id,
      conversationId: invitee.conversationId,
      expectedParticipantEpochId: invitee.participantEpochId,
      expectedParticipantDigest: invitee.participantDigest,
      inviteeIdentityId: invitee.identityId,
      inviteePersonId: invitee.personId,
      expectedIntentVersion: adult.version,
    };
    setBusy(path);
    setActionError(null);
    setNotice(null);
    try {
      await postJson(path, viewer.csrfToken, body);
      await load(false);
      setNotice(`Florence sent ${adult.displayName} a private invitation.`);
      return true;
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401 && adult.role === "steward") {
        try {
          await postJson("/api/safety/request-step-up", viewer.csrfToken, {
            purpose: "household_invitation",
            context: {
              action: "invite",
              householdId: data.household.id,
              onboardingAdultIntentId: adult.id,
              onboardingAdultIntentVersion: String(adult.version),
              conversationId: invitee.conversationId,
              expectedParticipantEpochId: invitee.participantEpochId,
              expectedParticipantDigest: invitee.participantDigest,
              inviteeIdentityId: invitee.identityId,
              inviteePersonId: invitee.personId,
              proposedDisplayName: adult.displayName,
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
  } else if (step === "adults" && household) {
    content = (
      <AdultsStep
        adults={household.adults}
        busy={busy !== null}
        onContinue={(adults) =>
          save("/api/onboarding/adults", {
            householdId: household.id,
            expectedMembershipVersion: household.versions.membership,
            expectedIntakeVersion: household.versions.intake,
            adults,
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
        google={data.google}
        busy={busy !== null}
        editing={isEditingEarlierStep}
        onContinue={() => {
          if (isEditingEarlierStep) setPreviewStep(null);
          else void load(false);
        }}
        onSkip={() => save("/api/onboarding/google-skip", {})}
        onConnect={connectPersonalGoogle}
      />
    );
  } else if (step === "review" && household) {
    content = (
      <OnboardingReviewStep
        view={data}
        busy={busy !== null}
        onConnectAdult={connectAdult}
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

function AdultsStep({
  adults,
  busy,
  onContinue,
}: {
  adults: NonNullable<OnboardingView["household"]>["adults"];
  busy: boolean;
  onContinue: (adults: AdultRosterInput[]) => Promise<boolean>;
}) {
  const [drafts, setDrafts] = useState<AdultRosterDraft[]>(() =>
    adults.map((adult) => ({
      id: adult.id,
      displayName: adult.displayName,
      role: adult.role,
      progress: adult.progress,
      bound: adult.matchedPersonId !== null || adult.invitationId !== null,
    })),
  );
  const complete = drafts.every((adult) => adult.displayName.trim().length > 0);
  const update = (index: number, next: Partial<AdultRosterDraft>) => {
    setDrafts((current) =>
      current.map((adult, adultIndex) => (adultIndex === index ? { ...adult, ...next } : adult)),
    );
  };
  const submit = () =>
    onContinue(
      drafts.map((adult) => ({
        ...(adult.id ? { id: adult.id } : {}),
        displayName: adult.displayName.trim(),
        role: adult.role,
      })),
    );
  return (
    <form
      className="onboarding-form"
      onSubmit={(event) => {
        event.preventDefault();
        if (complete) void submit();
      }}
    >
      <OnboardingHeading
        eyebrow="The adults"
        title="Who helps care for your family?"
        copy="Add the people Florence should know about. A name and relationship are enough—Florence will learn how you work together from conversation."
      />
      <div className="onboarding-adult-list">
        {drafts.map((adult, index) => (
          <AdultRosterCard
            key={adult.id ?? `new-${index}`}
            adult={adult}
            index={index}
            onChange={(next) => update(index, next)}
            onRemove={() => setDrafts((current) => current.filter((_, itemIndex) => itemIndex !== index))}
          />
        ))}
      </div>
      <button
        type="button"
        className="onboarding-add-another"
        disabled={busy}
        onClick={() => setDrafts((current) => [...current, emptyAdultRosterDraft()])}
      >
        <span aria-hidden="true">＋</span> Add another person
      </button>
      {drafts.length ? (
        <p className="onboarding-plain-note">
          No one is contacted from a name alone. You can connect each person to their exact iMessage identity
          later.
        </p>
      ) : null}
      {drafts.length ? (
        <OnboardingPrimaryButton disabled={busy || !complete} busy={busy}>
          Continue
        </OnboardingPrimaryButton>
      ) : (
        <OnboardingPrimaryButton disabled={busy} busy={busy} onClick={() => void onContinue([])}>
          Just me for now
        </OnboardingPrimaryButton>
      )}
    </form>
  );
}

function AdultRosterCard({
  adult,
  index,
  onChange,
  onRemove,
}: {
  adult: AdultRosterDraft;
  index: number;
  onChange: (adult: Partial<AdultRosterDraft>) => void;
  onRemove: () => void;
}) {
  const locked = adult.bound === true;
  return (
    <article className="onboarding-adult-card">
      <div className="onboarding-adult-card-heading">
        <strong>{adult.displayName.trim() || `Person ${index + 1}`}</strong>
        {locked ? (
          <span>{adultProgressLabel(adult.progress ?? "not_connected")}</span>
        ) : (
          <button type="button" onClick={onRemove} aria-label={`Remove person ${index + 1}`}>
            Remove
          </button>
        )}
      </div>
      <label className="onboarding-field">
        <span>Their name</span>
        <input
          autoComplete="off"
          maxLength={80}
          placeholder="First name"
          value={adult.displayName}
          disabled={locked}
          onChange={(event) => onChange({ displayName: event.target.value })}
        />
      </label>
      <label className="onboarding-field">
        <span>Their relationship</span>
        <select
          value={adult.role}
          disabled={locked}
          onChange={(event) => onChange({ role: event.target.value as AdultRosterDraft["role"] })}
        >
          <option value="steward">Parent or co-parent</option>
          <option value="caregiver">Babysitter or caregiver</option>
        </select>
      </label>
    </article>
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
  google,
  busy,
  editing,
  onContinue,
  onSkip,
  onConnect,
}: {
  google: OnboardingView["google"];
  busy: boolean;
  editing: boolean;
  onContinue: () => void;
  onSkip: () => Promise<boolean>;
  onConnect: () => Promise<void>;
}) {
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
      <button
        type="button"
        className="onboarding-primary-button"
        disabled={busy}
        onClick={() => void onConnect()}
      >
        Connect personal Google
      </button>
      <button type="button" className="onboarding-text-button" disabled={busy} onClick={() => void onSkip()}>
        Not now
      </button>
    </div>
  );
}

function OnboardingReviewStep({
  view,
  busy,
  onConnectAdult,
  onComplete,
}: {
  view: OnboardingView;
  busy: boolean;
  onConnectAdult: (
    adult: NonNullable<OnboardingView["household"]>["adults"][number],
    invitee: OnboardingView["eligibleInvitees"][number],
  ) => Promise<boolean>;
  onComplete: () => Promise<boolean>;
}) {
  const household = view.household as NonNullable<OnboardingView["household"]>;
  return (
    <div className="onboarding-form">
      <OnboardingHeading
        eyebrow="Ready to begin"
        title={`Florence has the basics, ${view.person.name}`}
        copy="This is enough to start helping. You can change any of it later."
      />
      <div className="onboarding-review-list">
        <ReviewLine label="You" value={`${view.person.name} · ${friendlyTimeZone(view.person.timeZone)}`} />
        {view.branch !== "caregiver" || household.sharedIntakeComplete ? (
          <ReviewLine
            label="Children"
            value={
              household.children.length
                ? household.children.map((child) => child.name).join(", ")
                : "Add later"
            }
          />
        ) : null}
        <ReviewLine
          label="Google"
          value={
            view.google.decision === "connected"
              ? (view.google.accountEmail ?? "Connected privately")
              : "Not now"
          }
        />
      </div>
      {view.branch === "caregiver" && !household.sharedIntakeComplete ? (
        <p className="onboarding-plain-note">
          A parent is still finishing the shared family details. You can finish your private setup now;
          Florence won’t ask you to enter them.
        </p>
      ) : (
        <section className="onboarding-review-adults">
          <div>
            <strong>Family adults</strong>
            <span>Connecting them is optional and won’t hold up setup.</span>
          </div>
          {household.adults.length ? (
            household.adults.map((adult) => (
              <AdultConnectionRow
                key={adult.id}
                adult={adult}
                invitees={view.eligibleInvitees.filter((invitee) => {
                  return !household.adults.some(
                    (other) => other.id !== adult.id && other.matchedPersonId === invitee.personId,
                  );
                })}
                busy={busy}
                onConnect={onConnectAdult}
              />
            ))
          ) : (
            <p className="onboarding-plain-note">Just you for now. You can add people later.</p>
          )}
        </section>
      )}
      <OnboardingPrimaryButton disabled={busy} busy={busy} onClick={() => void onComplete()}>
        Start using Florence
      </OnboardingPrimaryButton>
    </div>
  );
}

function AdultConnectionRow({
  adult,
  invitees,
  busy,
  onConnect,
}: {
  adult: NonNullable<OnboardingView["household"]>["adults"][number];
  invitees: OnboardingView["eligibleInvitees"];
  busy: boolean;
  onConnect: (
    adult: NonNullable<OnboardingView["household"]>["adults"][number],
    invitee: OnboardingView["eligibleInvitees"][number],
  ) => Promise<boolean>;
}) {
  const [selectedIdentityId, setSelectedIdentityId] = useState(invitees[0]?.identityId ?? "");
  const selected = invitees.find((invitee) => invitee.identityId === selectedIdentityId) ?? null;
  useEffect(() => {
    if (!invitees.some((invitee) => invitee.identityId === selectedIdentityId)) {
      setSelectedIdentityId(invitees[0]?.identityId ?? "");
    }
  }, [invitees, selectedIdentityId]);
  const pending = adult.progress !== "not_connected";
  return (
    <article className="onboarding-review-adult">
      <div className="onboarding-review-adult-heading">
        <Avatar name={adult.displayName} />
        <div>
          <strong>{adult.displayName}</strong>
          <span>
            {adultRoleLabel(adult.role)} · {adultProgressLabel(adult.progress)}
          </span>
        </div>
      </div>
      {!pending && invitees.length ? (
        <div className="onboarding-adult-connect">
          <label className="onboarding-field">
            <span>Match their iMessage identity</span>
            <select
              value={selectedIdentityId}
              onChange={(event) => setSelectedIdentityId(event.target.value)}
            >
              {invitees.map((invitee) => (
                <option
                  value={invitee.identityId}
                  key={`${invitee.identityId}:${invitee.participantEpochId}`}
                >
                  {invitee.name}
                </option>
              ))}
            </select>
          </label>
          <button
            type="button"
            className="onboarding-secondary-button"
            disabled={busy || !selected}
            onClick={() => {
              if (selected) void onConnect(adult, selected);
            }}
          >
            Connect privately
          </button>
        </div>
      ) : !pending ? (
        <p>Add Florence to an iMessage group with {adult.displayName} to connect them later.</p>
      ) : null}
    </article>
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
    <div className="onboarding-shell">
      <header className="onboarding-topbar">
        <Brand />
        <a href="/privacy">Privacy</a>
      </header>
      <main className="onboarding-main">
        <section className="onboarding-card-main">
          <div className="onboarding-form onboarding-problem">
            <OnboardingHeading
              eyebrow="Setup paused"
              title="Your session has ended"
              copy="Everything you saved is still here. Sign in with your linked Google account and Florence will resume at the right step."
            />
            <a className="onboarding-primary-button" href="/sign-in?returnTo=%2Fonboarding">
              Sign in with Google
            </a>
            {florencePhone ? (
              <a
                className="onboarding-secondary-button"
                href={`sms:${florencePhone}&body=${encodeURIComponent("Help me reopen Florence")}`}
              >
                Recover through iMessage
              </a>
            ) : null}
          </div>
        </section>
      </main>
    </div>
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
  if (step === "adults") return view.householdChoices.length > 1 ? "choose_household" : "confirm_profile";
  if (step === "children") return "adults";
  if (step === "review_shared_context") return "confirm_profile";
  if (step === "google") {
    if (
      view.branch === "invited_adult" ||
      (view.branch === "caregiver" && view.household?.sharedIntakeComplete)
    ) {
      return "review_shared_context";
    }
    if (view.branch === "caregiver") return "confirm_profile";
    return "children";
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

interface AdultRosterDraft {
  id?: string;
  displayName: string;
  role: "steward" | "caregiver";
  progress?: NonNullable<OnboardingView["household"]>["adults"][number]["progress"];
  bound?: boolean;
}

type AdultRosterInput = AdultRosterDraft;

function emptyAdultRosterDraft(): AdultRosterDraft {
  return { displayName: "", role: "steward" };
}

function adultRoleLabel(role: AdultRosterDraft["role"]): string {
  return role === "steward" ? "Parent or co-parent" : "Babysitter or caregiver";
}

function adultProgressLabel(
  progress: NonNullable<OnboardingView["household"]>["adults"][number]["progress"],
): string {
  if (progress === "joined") return "Joined";
  if (progress === "awaiting_steward_approval") return "Waiting for parent approval";
  if (progress === "awaiting_acceptance") return "Invitation sent";
  return "Not connected yet";
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
