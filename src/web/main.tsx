import { type FormEvent, StrictMode, useCallback, useEffect, useMemo, useState } from "react";
import { createRoot } from "react-dom/client";
import { BrowserRouter, Navigate, NavLink, Outlet, Route, Routes, useLocation } from "react-router-dom";
import {
  ApiError,
  type ChatView,
  type DataSafetyView,
  getJson,
  type HomeView,
  type PeopleView,
  postJson,
  type RoutineView,
  type SourceView,
  type Viewer,
} from "./api.js";
import "./styles.css";

function FlorenceApp() {
  const [viewer, setViewer] = useState<Viewer | null>(null);
  const [loading, setLoading] = useState(true);
  const [unauthorized, setUnauthorized] = useState(false);

  useEffect(() => {
    getJson<Viewer>("/api/me")
      .then(setViewer)
      .catch((error: unknown) => {
        if (error instanceof ApiError && error.status === 401) setUnauthorized(true);
      })
      .finally(() => setLoading(false));
  }, []);

  if (loading) return <LoadingScreen />;
  if (!viewer || unauthorized) return <PublicLanding />;

  return (
    <Routes>
      <Route element={<AppShell viewer={viewer} />}>
        <Route index element={<Navigate to="/home" replace />} />
        <Route path="/home" element={<HomePage />} />
        <Route path="/people" element={<PeoplePage viewer={viewer} />} />
        <Route path="/chats" element={<ChatsPage />} />
        <Route path="/sources" element={<SourcesPage viewer={viewer} />} />
        <Route path="/safety" element={<SafetyPage viewer={viewer} />} />
      </Route>
      <Route path="*" element={<Navigate to="/home" replace />} />
    </Routes>
  );
}

function PublicLanding() {
  const florencePhone = document.documentElement.dataset.florencePhone;
  return (
    <main className="landing">
      <div className="landing-glow" />
      <nav className="public-nav">
        <Brand />
        <a href="/privacy">Privacy</a>
      </nav>
      <section className="hero">
        <div className="eyebrow">Your family’s Chief of Staff</div>
        <h1>Nothing important falls through.</h1>
        <p>
          Add or forward it to Florence. She catches the family logistics hidden across messages, email,
          calendars, and PDFs—and makes sure someone actually has it covered.
        </p>
        <div className="message-demo">
          <div className="bubble incoming">I can’t make Wednesday pickup anymore.</div>
          <div className="bubble florence">Wednesday pickup at 3:00 is open. Mary, can you cover it?</div>
          <div className="bubble incoming right">Yes, I have it.</div>
          <div className="loop-closed">
            <span>✓</span> Coverage confirmed
          </div>
        </div>
        <div className="cta-card">
          <strong>Start in iMessage</strong>
          <span>Text Florence to register. Your private link will open this companion securely.</span>
          <a className="primary-button" href={florencePhone ? `sms:${florencePhone}` : "/privacy"}>
            {florencePhone ? "Text Florence" : "Florence is connecting"}
          </a>
        </div>
      </section>
      <section className="promise-grid">
        <PromiseCard
          icon="↗"
          title="Lives where your family talks"
          copy="Private DMs and the exact groups you authorize."
        />
        <PromiseCard
          icon="◌"
          title="Understands the firehose"
          copy="Gmail, calendars, PDFs, screenshots, and routines."
        />
        <PromiseCard
          icon="✓"
          title="Closes the coverage loop"
          copy="Florence stays with it until a person explicitly takes it."
        />
      </section>
    </main>
  );
}

function AppShell({ viewer }: { viewer: Viewer }) {
  const location = useLocation();
  const currentPage = useMemo(
    () => APP_LINKS.find((link) => link.to === location.pathname) ?? APP_LINKS[0],
    [location.pathname],
  );
  const primaryHousehold = viewer.households[0];
  const householdName = primaryHousehold?.name ?? "Your family";
  const householdDetail = primaryHousehold
    ? `${primaryHousehold.memberCount} ${primaryHousehold.memberCount === 1 ? "person" : "people"}`
    : "Ready to set up";
  const florencePhone = document.documentElement.dataset.florencePhone;

  return (
    <div className="app-shell">
      <aside className="app-sidebar" aria-label="Florence navigation">
        <div className="sidebar-heading">
          <Brand />
          <div className="sidebar-household">
            <span className="sidebar-household-mark">{householdName.slice(0, 1).toUpperCase()}</span>
            <span>
              <strong>{householdName}</strong>
              <small>{householdDetail}</small>
            </span>
          </div>
        </div>
        {florencePhone ? (
          <a className="ask-florence" href={`sms:${florencePhone}`}>
            <ShellIcon name="message" />
            <span>Ask Florence</span>
          </a>
        ) : (
          <div className="ask-florence unavailable">
            <ShellIcon name="message" />
            <span>Florence is connecting</span>
          </div>
        )}
        <nav className="sidebar-navigation" aria-label="Family controls">
          <AppNavigation />
        </nav>

        <NavLink className="sidebar-person" to="/people">
          <Avatar name={viewer.person.name} />
          <div>
            <strong>{viewer.person.name}</strong>
            <span>Your private profile</span>
          </div>
          <span className="profile-arrow" aria-hidden="true">
            ›
          </span>
        </NavLink>
      </aside>

      <div className="app-column">
        <header className="mobile-header">
          <div className="mobile-identity">
            <Brand compact />
            <div>
              <span>{householdName}</span>
              <h1>{currentPage.title}</h1>
            </div>
          </div>
          {florencePhone ? (
            <a className="mobile-ask" href={`sms:${florencePhone}`} aria-label="Ask Florence in iMessage">
              <ShellIcon name="message" />
              <span>Ask</span>
            </a>
          ) : null}
        </header>
        <main className="app-main">
          <header className="canvas-header">
            <span className="section-kicker">{currentPage.group}</span>
            <h1>{currentPage.title}</h1>
            <p>{currentPage.description}</p>
          </header>
          <Outlet />
        </main>
        <nav className="bottom-nav">
          <AppNavigation compact />
        </nav>
      </div>
    </div>
  );
}

const APP_LINKS = [
  {
    to: "/home",
    label: "Today",
    title: "Today",
    group: "Overview",
    icon: "today",
    description: "Anything that needs your attention, and nothing that doesn’t.",
  },
  {
    to: "/people",
    label: "Family",
    title: "Family & people",
    group: "Family",
    icon: "people",
    description: "Manage who Florence knows and what each person can do.",
  },
  {
    to: "/chats",
    label: "Groups",
    title: "iMessage groups",
    group: "Family",
    icon: "chats",
    description: "Review the iMessage groups where Florence is present.",
  },
  {
    to: "/sources",
    label: "Sources",
    title: "Sources & routines",
    group: "Florence setup",
    icon: "sources",
    description: "Connect private sources and manage the routines Florence watches.",
  },
  {
    to: "/safety",
    label: "Privacy",
    title: "Privacy & account",
    group: "Florence setup",
    icon: "safety",
    description: "Control your data, browser sessions, and Florence’s access.",
  },
] as const;

function AppNavigation({ compact = false }: { compact?: boolean }) {
  if (compact) {
    return (
      <div className="nav-links compact">
        {APP_LINKS.map((link) => (
          <NavLink key={link.to} to={link.to} className={({ isActive }) => (isActive ? "active" : "")}>
            <span className="nav-icon">
              <ShellIcon name={link.icon} />
            </span>
            <span>{link.label}</span>
          </NavLink>
        ))}
      </div>
    );
  }

  return (
    <div className="nav-links sidebar-nav">
      {APP_LINKS.map((link) => (
        <NavLink key={link.to} to={link.to} className={({ isActive }) => (isActive ? "active" : "")}>
          <span className="nav-icon">
            <ShellIcon name={link.icon} />
          </span>
          <span>{link.title}</span>
        </NavLink>
      ))}
    </div>
  );
}

function ShellIcon({ name }: { name: (typeof APP_LINKS)[number]["icon"] | "message" }) {
  if (name === "today") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M4 10.5 12 4l8 6.5V20H4z" />
        <path d="M9 20v-6h6v6" />
      </svg>
    );
  }
  if (name === "people") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <circle cx="9" cy="8" r="3" />
        <path d="M3.5 19c.3-3.4 2.1-5.2 5.5-5.2s5.2 1.8 5.5 5.2" />
        <path d="M15 6.2a3 3 0 0 1 0 5.6M16 14c2.6.3 4 2 4.2 5" />
      </svg>
    );
  }
  if (name === "chats" || name === "message") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <path d="M5 5.5h14v10H9l-4 3z" />
        <path d="M8 9h8M8 12h5" />
      </svg>
    );
  }
  if (name === "sources") {
    return (
      <svg viewBox="0 0 24 24" aria-hidden="true">
        <rect x="4" y="4" width="7" height="7" rx="1.5" />
        <rect x="13" y="4" width="7" height="7" rx="1.5" />
        <rect x="4" y="13" width="7" height="7" rx="1.5" />
        <path d="M16.5 14v5M14 16.5h5" />
      </svg>
    );
  }
  return (
    <svg viewBox="0 0 24 24" aria-hidden="true">
      <path d="M12 3 19 6v5c0 4.4-2.8 7.9-7 10-4.2-2.1-7-5.6-7-10V6z" />
      <path d="m9 12 2 2 4-4" />
    </svg>
  );
}

function HomePage() {
  const { data, loading, error, reload } = useResource<HomeView>("/api/home");
  useEffect(() => {
    let stopped = false;
    let refreshTimer: ReturnType<typeof setTimeout> | null = null;

    const stopTimer = () => {
      if (refreshTimer !== null) clearTimeout(refreshTimer);
      refreshTimer = null;
    };
    const scheduleRefresh = () => {
      stopTimer();
      if (stopped || document.visibilityState !== "visible") return;
      refreshTimer = setTimeout(async () => {
        refreshTimer = null;
        if (stopped || document.visibilityState !== "visible") return;
        await reload(false);
        scheduleRefresh();
      }, 5_000);
    };
    const handleVisibilityChange = () => {
      if (document.visibilityState !== "visible") {
        stopTimer();
        return;
      }
      void reload(false).finally(scheduleRefresh);
    };

    document.addEventListener("visibilitychange", handleVisibilityChange);
    scheduleRefresh();
    return () => {
      stopped = true;
      stopTimer();
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [reload]);

  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load Florence."} />;
  const attentionItems = data.items.filter((item) => item.phase !== "confirmed");
  const confirmedItems = data.items.filter((item) => item.phase === "confirmed");
  return (
    <div className="page-stack">
      <section className={`monitor-card ${data.monitoring.status}`}>
        <div className="monitor-orbit">
          <span />
        </div>
        <div>
          <div className="eyebrow">Florence is watching</div>
          <h2>{data.monitoring.label}</h2>
          <p>{data.monitoring.detail}</p>
        </div>
      </section>
      {data.onboarding ? (
        <section className="onboarding-card">
          <div className="onboarding-copy">
            <span className="section-kicker">Set up Florence</span>
            <h2>{data.onboarding.next ?? "You’re ready"}</h2>
            {data.onboarding.detail ? <p>{data.onboarding.detail}</p> : null}
            {data.onboarding.href && data.onboarding.actionLabel ? (
              <NavLink className="onboarding-action" to={data.onboarding.href}>
                {data.onboarding.actionLabel}
              </NavLink>
            ) : null}
          </div>
          <div className="onboarding-progress">
            <div className="progress">
              <span style={{ width: `${(data.onboarding.completed / data.onboarding.total) * 100}%` }} />
            </div>
            <small>
              {data.onboarding.completed} of {data.onboarding.total} complete
            </small>
          </div>
        </section>
      ) : null}
      <SectionHeading title="Needs your attention" count={data.attentionCount} />
      {attentionItems.length === 0 ? (
        <EmptyState
          title="Nothing needs you right now"
          copy="Florence will stay quiet until something changes."
        />
      ) : (
        <div className="item-list">
          {attentionItems.map((item) => (
            <ExceptionRow key={item.id} item={item} />
          ))}
        </div>
      )}
      {confirmedItems.length > 0 ? (
        <>
          <SectionHeading title="Covered in the last 24 hours" count={confirmedItems.length} />
          <div className="item-list confirmed-list">
            {confirmedItems.map((item) => (
              <ExceptionRow key={item.id} item={item} />
            ))}
          </div>
        </>
      ) : null}
    </div>
  );
}

function PeoplePage({ viewer }: { viewer: Viewer }) {
  const { data, loading, error, reload } = useResource<PeopleView>("/api/people");
  const location = useLocation();
  const [busy, setBusy] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [pendingGroupApprovals, setPendingGroupApprovals] = useState<Record<string, boolean>>({});
  const [inviteRoles, setInviteRoles] = useState<Record<string, "steward" | "caregiver" | "participant">>({});
  const [dependentDrafts, setDependentDrafts] = useState<Record<string, DependentDraft>>({});
  const requestedGroupCoverageOutcome = new URLSearchParams(location.search).get("group_coverage");

  useEffect(() => {
    if (!data || !location.hash.startsWith("#coverage-")) return;
    window.requestAnimationFrame(() => {
      document.querySelector(location.hash)?.scrollIntoView({ block: "center" });
    });
  }, [data, location.hash]);

  async function runAction(key: string, path: string, body: unknown, success: string) {
    setBusy(key);
    setActionMessage(null);
    setActionError(null);
    try {
      await postJson(path, viewer.csrfToken, body);
      setActionMessage(success);
      await reload();
      return true;
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "Florence could not save that change.");
      return false;
    } finally {
      setBusy(null);
    }
  }

  async function runProtectedAction(
    key: string,
    path: string,
    body: unknown,
    success: string,
    stepUp: unknown,
    callbacks?: { onCompleted?: () => void; onStepUpSent?: () => void },
  ) {
    setBusy(key);
    setActionMessage(null);
    setActionError(null);
    try {
      await postJson(path, viewer.csrfToken, body);
      setActionMessage(success);
      await reload();
      callbacks?.onCompleted?.();
    } catch (reason) {
      if (reason instanceof ApiError && reason.status === 401) {
        try {
          await postJson("/api/safety/request-step-up", viewer.csrfToken, stepUp);
          callbacks?.onStepUpSent?.();
          setActionMessage(
            "Check your private iMessage from Florence. The secure confirmation there will finish this action.",
          );
        } catch (stepUpReason) {
          setActionError(
            stepUpReason instanceof Error
              ? stepUpReason.message
              : "Florence could not send the private confirmation.",
          );
        }
      } else {
        setActionError(reason instanceof Error ? reason.message : "Florence could not save that change.");
      }
    } finally {
      setBusy(null);
    }
  }

  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load your family."} />;
  const anchoredConversationId = location.hash.startsWith("#coverage-")
    ? location.hash.slice("#coverage-".length)
    : null;
  const anchoredCoverageGroup = data.households
    .flatMap((household) => household.coverageGroups)
    .find((group) => group.conversationId === anchoredConversationId);
  const groupCoverageOutcome =
    requestedGroupCoverageOutcome === "approved"
      ? anchoredCoverageGroup?.viewerApproved || anchoredCoverageGroup?.active
        ? "approved"
        : "changed"
      : requestedGroupCoverageOutcome;
  return (
    <div className="page-stack">
      <Intro
        title="The people Florence coordinates with"
        copy="Use this private page for family membership and permissions. Keep ordinary family conversation in iMessage."
      />
      {groupCoverageOutcome === "approved" ? (
        <div className="success-notice" role="status">
          Your approval is saved for the group shown below. Florence will stay silent there until every
          current person approves.
        </div>
      ) : groupCoverageOutcome === "changed" ? (
        <div className="error-notice" role="alert">
          That group changed before approval finished. Review the current people below and approve again if
          you still want Florence to write there.
        </div>
      ) : groupCoverageOutcome === "retry" ? (
        <div className="error-notice" role="alert">
          Florence couldn’t save that approval yet. Your private confirmation is still active—tap “Approve
          this group” below to try once more.
        </div>
      ) : null}
      {actionMessage ? (
        <div className="success-notice" role="status">
          {actionMessage}
        </div>
      ) : null}
      {actionError ? (
        <div className="error-notice" role="alert">
          {actionError}
        </div>
      ) : null}
      <section className="identity-card">
        <Avatar name={viewer.person.name} large />
        <div>
          <span className="section-kicker">Your private Florence account</span>
          <h2>{viewer.person.name}</h2>
          <p>Verified in iMessage · {viewer.person.timezone}</p>
        </div>
        <span className="status-pill good">Verified</span>
      </section>

      {data.invitations.length > 0 ? (
        <>
          <SectionHeading title="Needs your decision" count={data.invitations.length} />
          <div className="card-list">
            {data.invitations.map((invitation) => {
              const actionKey = `invitation:${invitation.id}`;
              const approving = invitation.action === "approve";
              return (
                <article className="family-decision" key={`${invitation.action}:${invitation.id}`}>
                  <div>
                    <span className="section-kicker">
                      {approving ? "Family approval" : "Invitation to join"}
                    </span>
                    <h3>
                      {approving
                        ? `${invitation.personName} as ${roleLabel(invitation.role)}`
                        : `Join ${invitation.householdName} as ${roleLabel(invitation.role)}`}
                    </h3>
                    <p>{invitation.detail}</p>
                    {!approving && invitation.sharedContext ? (
                      <div>
                        <strong>Family context already shared with Florence</strong>
                        <p>
                          Florence already knows these details, so you won’t be asked to enter them again. By
                          joining, you agree Florence can use them as shared context for this family.
                        </p>
                        <div className="family-members">
                          {invitation.sharedContext.children.map((child) => (
                            <div
                              className="family-person"
                              key={`${child.preferredName}:${child.birthYear ?? ""}:${child.school}`}
                            >
                              <Avatar name={child.preferredName} />
                              <div>
                                <strong>{child.preferredName}</strong>
                                <span>{invitationChildSummary(child)}</span>
                              </div>
                            </div>
                          ))}
                        </div>
                        <p>
                          {invitation.role === "steward"
                            ? "After you join, you can correct these details from this page."
                            : "After you join, these details stay visible here and a family steward can correct them."}
                        </p>
                      </div>
                    ) : null}
                  </div>
                  <button
                    type="button"
                    className="primary-button compact-button"
                    disabled={!invitation.canAct || busy === actionKey}
                    onClick={() =>
                      void runProtectedAction(
                        actionKey,
                        `/api/invitations/${invitation.id}/${approving ? "approve" : "accept"}`,
                        {},
                        approving
                          ? "Approved. The invitation is ready once every steward has approved."
                          : "You joined the family.",
                        {
                          purpose: "household_invitation",
                          context: {
                            action: approving ? "approve" : "accept",
                            householdId: invitation.householdId,
                            invitationId: invitation.id,
                          },
                        },
                      )
                    }
                  >
                    {approving ? "Approve" : invitation.canAct ? "Join family" : "Waiting for approval"}
                  </button>
                </article>
              );
            })}
          </div>
        </>
      ) : null}

      <SectionHeading title="Families" count={data.households.length} />
      {data.households.length === 0 ? (
        <section className="family-start-card">
          <span className="family-symbol">⌂</span>
          <div>
            <h3>Create your family</h3>
            <p>
              Start with yourself. You can then add children or invite registered people from a shared group.
            </p>
          </div>
          <button
            type="button"
            className="primary-button compact-button"
            disabled={busy === "create-family"}
            onClick={() =>
              void runAction("create-family", "/api/households", {}, "Your family is ready to set up.")
            }
          >
            Create family
          </button>
        </section>
      ) : null}

      {data.households.map((household) => (
        <section className="family-card" key={household.id}>
          <header className="family-header">
            <div>
              <span className="section-kicker">You’re a {roleLabel(household.viewerRole)}</span>
              <h3>{household.name}</h3>
              <p>
                {household.members.length} {household.members.length === 1 ? "person" : "people"} · Family
                setup {household.status === "onboarding" ? "in progress" : "active"}
              </p>
            </div>
            <span className={`status-pill ${household.status === "active" ? "good" : ""}`}>
              {household.status === "active" ? "Active" : "Setting up"}
            </span>
          </header>

          <div className="family-subsection">
            <h4>People</h4>
            <div className="family-members">
              {household.members.map((member) => {
                const context = member.context;
                const editKey = `dependent-edit:${member.id}`;
                const draft = dependentDrafts[member.id] ?? dependentDraft(member);
                return (
                  <div className="family-person" key={member.id}>
                    <Avatar name={member.name} />
                    <div>
                      <strong>{member.name}</strong>
                      <span>
                        {member.represented ? "Child represented by the family" : roleLabel(member.role)}
                      </span>
                      {context ? <small>{dependentContextSummary(context)}</small> : null}
                      {member.represented && household.canAddDependent ? (
                        <details>
                          <summary>Edit the details Florence uses to recognize them</summary>
                          <form
                            onSubmit={(event) => {
                              event.preventDefault();
                              if (!draft.displayName.trim()) return;
                              void runAction(
                                editKey,
                                `/api/households/${household.id}/dependents/${member.id}`,
                                dependentPayload(draft),
                                `${draft.displayName.trim()}’s details are updated for the whole family.`,
                              );
                            }}
                          >
                            <DependentFields
                              idPrefix={`edit-${member.id}`}
                              value={draft}
                              onChange={(next) =>
                                setDependentDrafts((current) => ({ ...current, [member.id]: next }))
                              }
                            />
                            <button type="submit" className="quiet-button" disabled={busy === editKey}>
                              Save for everyone
                            </button>
                          </form>
                        </details>
                      ) : null}
                    </div>
                    {member.self ? <span className="tiny-label">You</span> : null}
                    {member.represented ? <span className="tiny-label represented">Represented</span> : null}
                  </div>
                );
              })}
            </div>
          </div>

          {household.canAddDependent ? (
            <form
              className="family-inline-form"
              onSubmit={(event) => {
                event.preventDefault();
                const draft = dependentDrafts[household.id] ?? emptyDependentDraft();
                const displayName = draft.displayName.trim();
                if (!displayName) return;
                void runAction(
                  `dependent:${household.id}`,
                  `/api/households/${household.id}/dependents`,
                  dependentPayload(draft),
                  `${displayName} was added. These details are shared with family members, so Florence won’t ask them again.`,
                ).then((saved) => {
                  if (saved)
                    setDependentDrafts((current) => ({ ...current, [household.id]: emptyDependentDraft() }));
                });
              }}
            >
              <label htmlFor={`dependent-${household.id}-name`}>Add a child</label>
              <DependentFields
                idPrefix={`dependent-${household.id}`}
                value={dependentDrafts[household.id] ?? emptyDependentDraft()}
                onChange={(next) => setDependentDrafts((current) => ({ ...current, [household.id]: next }))}
              />
              <button
                type="submit"
                className="quiet-button"
                disabled={
                  busy === `dependent:${household.id}` ||
                  !(dependentDrafts[household.id] ?? emptyDependentDraft()).displayName.trim()
                }
              >
                Add child
              </button>
              <small>
                Name is enough to start. Aliases, birth year, school, and activities help Florence match
                messages without asking the other parent again.
              </small>
            </form>
          ) : null}

          {household.canInvite ? (
            <div className="family-subsection">
              <div className="family-section-heading">
                <div>
                  <h4>Invite from a current group</h4>
                  <p>
                    Choose someone already sharing an iMessage group with you. Florence will enroll them in a
                    private chat before sharing any family context.
                  </p>
                </div>
              </div>
              {household.eligibleParticipants.length > 0 ? (
                <div className="family-candidates">
                  {household.eligibleParticipants.map((participant) => {
                    const selectionKey = `${household.id}:${participant.personId}`;
                    const role = inviteRoles[selectionKey] ?? "steward";
                    const inviteKey = `invite:${selectionKey}`;
                    return (
                      <div className="family-candidate" key={selectionKey}>
                        <div>
                          <strong>{participant.name}</strong>
                          <span>
                            {participant.registered
                              ? "Registered in a shared group"
                              : "Not registered · Florence will ask privately"}
                          </span>
                        </div>
                        <select
                          aria-label={`Role for ${participant.name}`}
                          value={role}
                          onChange={(event) =>
                            setInviteRoles((current) => ({
                              ...current,
                              [selectionKey]: event.target.value as "steward" | "caregiver" | "participant",
                            }))
                          }
                        >
                          <option value="steward">Parent / steward</option>
                          <option value="caregiver">Caregiver</option>
                          <option value="participant">Family participant</option>
                        </select>
                        <button
                          type="button"
                          className="quiet-button"
                          disabled={busy === inviteKey}
                          onClick={() => {
                            const path = `/api/households/${household.id}/invitations`;
                            const body = {
                              conversationId: participant.conversationId,
                              inviteePersonId: participant.personId,
                              role,
                            };
                            const success = participant.registered
                              ? `Florence texted ${participant.name} a fresh private family invitation.`
                              : `Florence privately asked ${participant.name} whether they want to join.`;
                            void (role === "steward"
                              ? runProtectedAction(inviteKey, path, body, success, {
                                  purpose: "household_invitation",
                                  context: {
                                    action: "invite",
                                    householdId: household.id,
                                    conversationId: participant.conversationId,
                                    inviteePersonId: participant.personId,
                                  },
                                })
                              : runAction(inviteKey, path, body, success));
                          }}
                        >
                          Invite
                        </button>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <p className="family-empty-line">
                  Add Florence to an iMessage group with the person you want to invite, then return here.
                </p>
              )}
            </div>
          ) : null}

          <div className="family-subsection">
            <div className="family-section-heading">
              <div>
                <h4>Proactive coverage in groups</h4>
                <p>
                  Florence stays silent in the group while approval is pending. Each current person approves
                  privately—no one needs to reply in the group. If anyone joins or leaves, approval resets.
                </p>
              </div>
            </div>
            {household.coverageGroups.length > 0 ? (
              <div className="coverage-permissions">
                {household.coverageGroups.map((group) => {
                  const approvalKey = `coverage:${group.conversationId}`;
                  const awaitingPrivateConfirmation =
                    pendingGroupApprovals[group.conversationId] === true &&
                    !group.viewerApproved &&
                    !group.active;
                  return (
                    <div
                      className="coverage-permission"
                      id={`coverage-${group.conversationId}`}
                      key={group.conversationId}
                    >
                      <div>
                        <strong>{group.label}</strong>
                        <span>
                          {group.active
                            ? "Everyone approved. Florence may help with family coverage here."
                            : awaitingPrivateConfirmation
                              ? "Check your private iMessage and tap “Approve this group” to finish."
                              : (group.blockedReason ??
                                `${group.approvedCount} of ${group.requiredCount} people have approved.`)}
                        </span>
                      </div>
                      {group.active ? (
                        <span className="status-pill good">Everyone approved</span>
                      ) : group.viewerApproved ? (
                        <span className="status-pill">You approved</span>
                      ) : awaitingPrivateConfirmation ? (
                        <span className="status-pill">Check iMessage</span>
                      ) : (
                        <button
                          type="button"
                          className="quiet-button"
                          aria-label={`${group.label}: approve Florence`}
                          disabled={!group.canApprove || busy === approvalKey}
                          onClick={() =>
                            void runProtectedAction(
                              approvalKey,
                              `/api/chats/${group.conversationId}/coverage-rule-approval`,
                              {
                                expectedParticipantEpochId: group.participantEpochId,
                                expectedParticipantSetDigest: group.participantSetDigest,
                                expectedConversationAuthorityVersion: group.conversationAuthorityVersion,
                                expectedHouseholdControlEpoch: group.householdControlEpoch,
                              },
                              `${group.label}: your approval is saved. The group status is updated below.`,
                              {
                                purpose: "group_coverage",
                                context: {
                                  action: "approve",
                                  conversationId: group.conversationId,
                                  expectedParticipantEpochId: group.participantEpochId,
                                  expectedParticipantSetDigest: group.participantSetDigest,
                                  expectedConversationAuthorityVersion: String(
                                    group.conversationAuthorityVersion,
                                  ),
                                  expectedHouseholdControlEpoch: String(group.householdControlEpoch),
                                },
                              },
                              {
                                onCompleted: () =>
                                  setPendingGroupApprovals((current) => ({
                                    ...current,
                                    [group.conversationId]: false,
                                  })),
                                onStepUpSent: () =>
                                  setPendingGroupApprovals((current) => ({
                                    ...current,
                                    [group.conversationId]: true,
                                  })),
                              },
                            )
                          }
                        >
                          {busy === approvalKey ? "Sending…" : "Approve this group"}
                        </button>
                      )}
                    </div>
                  );
                })}
              </div>
            ) : (
              <p className="family-empty-line">
                Family groups will appear here after everyone has joined the family.
              </p>
            )}
          </div>
        </section>
      ))}
    </div>
  );
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

function dependentDraft(member: PeopleView["households"][number]["members"][number]): DependentDraft {
  return {
    displayName: member.name,
    aliases: member.context?.aliases.join(", ") ?? "",
    birthYear: member.context?.birthYear ? String(member.context.birthYear) : "",
    school: member.context?.school ?? "",
    activities: member.context?.activities.join(", ") ?? "",
  };
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

function dependentContextSummary(
  context: NonNullable<PeopleView["households"][number]["members"][number]["context"]>,
): string {
  const facts = [
    context.aliases.length ? `also ${context.aliases.join(", ")}` : null,
    context.birthYear ? `born ${context.birthYear}` : null,
    context.school || null,
    context.activities.length ? context.activities.join(", ") : null,
  ].filter((entry): entry is string => Boolean(entry));
  return facts.length ? facts.join(" · ") : "Add school, aliases, or activities when useful.";
}

function invitationChildSummary(
  child: NonNullable<PeopleView["invitations"][number]["sharedContext"]>["children"][number],
): string {
  const facts = [
    child.aliases.length ? `Also called ${child.aliases.join(", ")}` : null,
    child.birthYear ? `Born ${child.birthYear}` : null,
    child.school ? `School: ${child.school}` : null,
    child.activities.length ? `Activities: ${child.activities.join(", ")}` : null,
  ].filter((entry): entry is string => Boolean(entry));
  return facts.length ? facts.join(" · ") : "Preferred name";
}

function DependentFields({
  idPrefix,
  value,
  onChange,
}: {
  idPrefix: string;
  value: DependentDraft;
  onChange: (next: DependentDraft) => void;
}) {
  return (
    <div>
      <input
        id={`${idPrefix}-name`}
        maxLength={80}
        placeholder="Name"
        aria-label="Child’s name"
        value={value.displayName}
        onChange={(event) => onChange({ ...value, displayName: event.target.value })}
      />
      <input
        maxLength={300}
        placeholder="Also called (comma separated)"
        aria-label="Child’s aliases"
        value={value.aliases}
        onChange={(event) => onChange({ ...value, aliases: event.target.value })}
      />
      <input
        type="number"
        min={1900}
        max={2100}
        placeholder="Birth year"
        aria-label="Child’s birth year"
        value={value.birthYear}
        onChange={(event) => onChange({ ...value, birthYear: event.target.value })}
      />
      <input
        maxLength={160}
        placeholder="School"
        aria-label="Child’s school"
        value={value.school}
        onChange={(event) => onChange({ ...value, school: event.target.value })}
      />
      <input
        maxLength={1000}
        placeholder="Activities (comma separated)"
        aria-label="Child’s activities"
        value={value.activities}
        onChange={(event) => onChange({ ...value, activities: event.target.value })}
      />
    </div>
  );
}

function ChatsPage() {
  const { data, loading, error } = useResource<ChatView[]>("/api/chats");
  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load chats."} />;
  return (
    <div className="page-stack">
      <Intro
        title="Every chat is its own privacy boundary"
        copy="Florence can privately retain permitted post-addition context without speaking in the source group. Group writing needs a separate approval for the exact current participants."
      />
      <div className="card-list">
        {data.map((chat) => (
          <ChatCard chat={chat} key={chat.id} />
        ))}
      </div>
      {data.length === 0 ? (
        <EmptyState
          title="No chats connected"
          copy="Add Florence to an iMessage group, then come back to inspect its status."
        />
      ) : null}
    </div>
  );
}

function SourcesPage({ viewer }: { viewer: Viewer }) {
  const { data, loading, error, reload } = useResource<SourceView>("/api/sources");
  const [busy, setBusy] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [shareDestinations, setShareDestinations] = useState<Record<string, string>>({});
  const [includeWorkMail, setIncludeWorkMail] = useState(false);
  const location = useLocation();
  const googleStepUpReady =
    viewer.session.assuranceKind === "google_connect" &&
    viewer.session.assuranceExpiresAt !== null &&
    new Date(viewer.session.assuranceExpiresAt) > new Date();
  useEffect(() => {
    if (!data?.privateReviews.some((review) => review.preparingShare)) return;
    const poll = window.setInterval(() => void reload(false), 3_000);
    return () => window.clearInterval(poll);
  }, [data, reload]);
  const googleSyncIncomplete = data?.connections.some(
    (connection) =>
      connection.status === "active" &&
      (connection.mail?.liveState === "waiting" ||
        connection.mail?.liveState === "needs_attention" ||
        connection.mail?.historyState === "waiting" ||
        connection.mail?.historyState === "running" ||
        connection.mail?.historyState === "needs_attention" ||
        connection.calendar?.syncState === "waiting" ||
        connection.calendar?.syncState === "needs_attention"),
  );
  useEffect(() => {
    if (!googleSyncIncomplete) return;
    const poll = window.setInterval(() => {
      if (document.visibilityState === "visible") void reload(false);
    }, 10_000);
    const refreshWhenVisible = () => {
      if (document.visibilityState === "visible") void reload(false);
    };
    document.addEventListener("visibilitychange", refreshWhenVisible);
    return () => {
      window.clearInterval(poll);
      document.removeEventListener("visibilitychange", refreshWhenVisible);
    };
  }, [googleSyncIncomplete, reload]);
  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load sources."} />;
  async function changeCalendar(
    connectionId: string,
    calendarId: string,
    mode: SourceView["connections"][number]["calendars"][number]["mode"],
  ) {
    const busyKey = `calendar:${connectionId}:${calendarId}`;
    setBusy(busyKey);
    setActionError(null);
    try {
      await postJson("/api/sources/calendar-mode", viewer.csrfToken, { connectionId, calendarId, mode });
      setActionMessage(
        mode === "off"
          ? "Florence will no longer use that calendar."
          : "Saved. This calendar stays private to you.",
      );
      await reload();
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "That calendar setting did not save.");
    } finally {
      setBusy(null);
    }
  }
  async function reviewCandidate(candidateId: string, decision: "accepted" | "rejected") {
    setBusy(`review:${candidateId}`);
    setActionError(null);
    try {
      await postJson(`/api/sources/private-reviews/${candidateId}`, viewer.csrfToken, { decision });
      setActionMessage(
        decision === "accepted"
          ? "Kept privately. Nothing was shared with your family."
          : "Dismissed. Florence will not use this suggestion.",
      );
      await reload();
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "That review did not save.");
    } finally {
      setBusy(null);
    }
  }
  async function prepareShare(review: SourceView["privateReviews"][number], conversationId: string) {
    setBusy(`share:${review.id}`);
    setActionError(null);
    try {
      await postJson(`/api/sources/private-reviews/${review.id}/prepare-share`, viewer.csrfToken, {
        conversationId,
      });
      setActionMessage("Florence is drafting the shortest family-safe version for your approval.");
      await reload();
      window.setTimeout(() => void reload(), 2_000);
    } catch (reason) {
      setActionError(
        reason instanceof Error ? reason.message : "Florence could not prepare that sharing choice.",
      );
    } finally {
      setBusy(null);
    }
  }
  async function approveShare(
    proposal: NonNullable<SourceView["privateReviews"][number]["shareProposal"]>,
    mode: "once" | "standing",
  ) {
    setBusy(`approve:${proposal.actionIntentId}:${mode}`);
    setActionError(null);
    const approval = {
      actionDigest: proposal.actionDigest,
      dataDigest: proposal.dataDigest,
      policyDigest: proposal.policyDigest,
      targetDigest: proposal.targetDigest,
      mode,
    } as const;
    try {
      await postJson(
        `/api/sources/private-bridge/${proposal.actionIntentId}/approve`,
        viewer.csrfToken,
        approval,
      );
      setActionMessage(
        mode === "standing"
          ? "Approved. Florence may share only matching future coverage items from that exact source."
          : "Approved once. Florence is applying the exact family coverage change you reviewed.",
      );
      await reload();
    } catch (reason) {
      if (mode === "standing" && reason instanceof ApiError && reason.status === 401) {
        try {
          await postJson("/api/safety/request-step-up", viewer.csrfToken, {
            purpose: "private_bridge_standing",
            context: {
              action: "approve",
              actionIntentId: proposal.actionIntentId,
              ...approval,
            },
          });
          setActionMessage(
            "Check your private iMessage from Florence, open its secure link, then approve this exact future rule again.",
          );
        } catch (stepUpReason) {
          setActionError(
            stepUpReason instanceof Error
              ? stepUpReason.message
              : "Florence could not send the private confirmation.",
          );
        }
      } else {
        setActionError(
          reason instanceof Error ? reason.message : "That exact sharing approval is no longer current.",
        );
      }
    } finally {
      setBusy(null);
    }
  }
  async function requestGoogleConnect(profile: "personal_family" | "work", includeMail = false) {
    const busyKey = `google-connect:${profile}`;
    setBusy(busyKey);
    setActionError(null);
    try {
      await postJson("/api/safety/request-step-up", viewer.csrfToken, {
        purpose: "google_connect",
        context: {
          profile,
          ...(profile === "work" && includeMail ? { mail: "include" } : {}),
        },
      });
      setActionMessage(
        `Check your private iMessage from Florence to connect your ${profile === "work" ? "work" : "personal"} Google account.`,
      );
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "Florence could not send the secure link.");
    } finally {
      setBusy(null);
    }
  }
  async function forgetMemory(memoryId: string) {
    if (!window.confirm("Forget this private memory? Florence will remove its stored copy.")) return;
    setBusy(`memory:${memoryId}`);
    setActionError(null);
    try {
      await postJson(`/api/sources/memories/${memoryId}/forget`, viewer.csrfToken, {});
      setActionMessage("Forgotten. Florence removed that private memory.");
      await reload();
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "Florence could not forget that memory.");
    } finally {
      setBusy(null);
    }
  }
  async function revokeRule(ruleId: string) {
    setBusy(`rule:${ruleId}`);
    setActionError(null);
    try {
      await postJson(`/api/sources/bridge-rules/${ruleId}/revoke`, viewer.csrfToken, {});
      setActionMessage("Sharing stopped. The rule cannot disclose anything else.");
      await reload();
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "Florence could not stop that rule.");
    } finally {
      setBusy(null);
    }
  }
  return (
    <div className="page-stack">
      <Intro
        title="Private first, always"
        copy="Your accounts belong to you. Florence shares only the minimum family meaning you explicitly approve."
      />
      {location.search.includes("connected=1") ? (
        <div className="success-notice">
          Google is connected. Florence is syncing the newest information first and will keep working quietly
          in the background.
        </div>
      ) : null}
      {location.search.includes("google=cancelled") ? (
        <div className="error-notice">Google was not connected. Nothing changed.</div>
      ) : null}
      {actionMessage ? (
        <div className="success-notice" role="status">
          {actionMessage}
        </div>
      ) : null}
      {actionError ? (
        <div className="error-notice" role="alert">
          {actionError}
        </div>
      ) : null}
      <SectionHeading title="Connections" count={data.connections.length} />
      <div className="card-list">
        {data.connections.map((connection) => {
          const requiresReconnect = connection.status === "reauth_required" || connection.status === "error";
          const needsAttention =
            requiresReconnect ||
            connection.mail?.liveState === "needs_attention" ||
            connection.mail?.historyState === "needs_attention" ||
            connection.calendar?.syncState === "needs_attention";
          const reconnectLabel =
            connection.accountKind === "work" ? "Reconnect work Google" : "Reconnect personal Google";
          const reconnectIncludesWorkMail = connection.accountKind === "work" && connection.mail !== null;
          const reconnectHref = `/oauth/google/start?profile=${connection.accountKind}${reconnectIncludesWorkMail ? "&mail=include" : ""}`;
          return (
            <article className="source-card" key={connection.id}>
              <div className="source-heading">
                <div className="provider-mark">G</div>
                <div>
                  <h3>{connection.email}</h3>
                  <p>
                    {connection.label} · {connection.accountKindLabel}
                  </p>
                </div>
                <span
                  className={`status-pill ${connection.status === "active" && !needsAttention ? "good" : ""}`}
                >
                  {connection.statusLabel}
                </span>
              </div>
              <div className="source-status-grid">
                {connection.mail ? (
                  <div>
                    <span>New mail</span>
                    <strong>{connection.mail.liveLabel}</strong>
                    <small>
                      {connection.mail.lastCheckedAt
                        ? `Last checked ${friendlyTime(connection.mail.lastCheckedAt)}`
                        : "Starting shortly"}
                    </small>
                  </div>
                ) : null}
                {connection.calendar ? (
                  <div>
                    <span>Calendar</span>
                    <strong>{connection.calendar.syncLabel}</strong>
                    <small>
                      {connection.calendar.lastCheckedAt
                        ? `Last checked ${friendlyTime(connection.calendar.lastCheckedAt)}`
                        : "Starting shortly"}
                    </small>
                  </div>
                ) : null}
                {connection.mail ? (
                  <div className="mail-history-card">
                    <div className="mail-history-heading">
                      <span>Earlier mail</span>
                      <strong>{connection.mail.historyLabel}</strong>
                    </div>
                    <ul className="sync-milestone-list" aria-label="Earlier mail progress">
                      {connection.mail.milestones.map((milestone) => (
                        <li className={`sync-milestone ${milestone.state}`} key={milestone.id}>
                          <span className="sync-milestone-marker" aria-hidden="true" />
                          <span className="sync-milestone-copy">
                            <strong>{milestone.label}</strong>
                            <small>{milestone.detail}</small>
                          </span>
                          <span className="sync-milestone-state">{milestone.stateLabel}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                ) : null}
              </div>
              {connection.calendar ? (
                <>
                  <div className="calendar-heading">
                    <div>
                      <h4>Calendars</h4>
                      <p>{connection.calendar.catalogLabel}</p>
                    </div>
                    <span>Private to you</span>
                  </div>
                  <div className="calendar-list">
                    {connection.calendars.map((calendar) => {
                      const busyKey = `calendar:${connection.id}:${calendar.id}`;
                      return (
                        <label key={calendar.id}>
                          <span>
                            <strong>{calendar.name}</strong>
                            <small>
                              {calendar.primary
                                ? "Primary calendar"
                                : (calendar.timezone ?? "Google Calendar")}
                            </small>
                          </span>
                          <select
                            aria-label={`How Florence may use ${calendar.name}`}
                            disabled={busy === busyKey}
                            value={calendar.mode}
                            onChange={(event) =>
                              void changeCalendar(
                                connection.id,
                                calendar.id,
                                event.target
                                  .value as SourceView["connections"][number]["calendars"][number]["mode"],
                              )
                            }
                          >
                            <option value="off">Don’t use</option>
                            <option value="availability_only">Busy times only</option>
                            <option value="full_private">Read details privately</option>
                          </select>
                        </label>
                      );
                    })}
                  </div>
                  {connection.calendars.length === 0 ? (
                    <div className="source-empty">
                      Florence will list each calendar here as soon as Google finishes connecting.
                    </div>
                  ) : null}
                </>
              ) : null}
              {requiresReconnect ? (
                <div className="source-reconnect">
                  <p>
                    {connection.accountKind === "work"
                      ? connection.mail
                        ? "Reconnect this work Mail and Calendar profile. Its contents remain private to you."
                        : "Reconnect this calendar-only work profile. Work Gmail remains optional."
                      : "Reconnect this personal profile to restore its private Mail and Calendar access."}
                  </p>
                  {googleStepUpReady ? (
                    <a className="secondary-button" href={reconnectHref}>
                      {reconnectLabel}
                    </a>
                  ) : (
                    <button
                      type="button"
                      className="secondary-button"
                      disabled={busy === `google-connect:${connection.accountKind}`}
                      onClick={() =>
                        void requestGoogleConnect(connection.accountKind, reconnectIncludesWorkMail)
                      }
                    >
                      {reconnectLabel}
                    </button>
                  )}
                </div>
              ) : null}
              {connection.status === "active" &&
              connection.accountKind === "work" &&
              connection.mail === null ? (
                <div className="source-reconnect">
                  <p>
                    Want Florence to find family logistics in this account too? Work Gmail is optional and
                    stays private to you.
                  </p>
                  {googleStepUpReady ? (
                    <a className="secondary-button" href="/oauth/google/start?profile=work&mail=include">
                      Add work Gmail
                    </a>
                  ) : (
                    <button
                      type="button"
                      className="secondary-button"
                      disabled={busy === "google-connect:work"}
                      onClick={() => void requestGoogleConnect("work", true)}
                    >
                      Add work Gmail
                    </button>
                  )}
                </div>
              ) : null}
            </article>
          );
        })}
      </div>
      {data.connections.length === 0 ? (
        <EmptyState
          title="No private accounts connected"
          copy="Connect personal Google for Mail and Calendar. Work Google can be Calendar only or include Gmail—your choice."
        />
      ) : null}
      <div className="source-connect-panel">
        <label className="work-mail-option">
          <input
            type="checkbox"
            checked={includeWorkMail}
            onChange={(event) => setIncludeWorkMail(event.target.checked)}
          />
          <span>
            <strong>Include work Gmail</strong>
            <small>
              Optional. Florence privately reviews work mail for family logistics. Nothing is shared with your
              household unless you approve it.
            </small>
          </span>
        </label>
        {googleStepUpReady ? (
          <div className="source-connect-actions">
            <a className="primary-button" href="/oauth/google/start?profile=personal_family">
              Connect personal Google
              <small>Mail and Calendar</small>
            </a>
            <a
              className="secondary-button"
              href={`/oauth/google/start?profile=work${includeWorkMail ? "&mail=include" : ""}`}
            >
              Connect work Google
              <small>{includeWorkMail ? "Mail and Calendar" : "Calendar only"}</small>
            </a>
          </div>
        ) : (
          <div className="source-connect-actions">
            <button
              type="button"
              className="primary-button"
              disabled={busy === "google-connect:personal_family"}
              onClick={() => void requestGoogleConnect("personal_family")}
            >
              Connect personal Google
              <small>Mail and Calendar</small>
            </button>
            <button
              type="button"
              className="secondary-button"
              disabled={busy === "google-connect:work"}
              onClick={() => void requestGoogleConnect("work", includeWorkMail)}
            >
              Connect work Google
              <small>{includeWorkMail ? "Mail and Calendar" : "Calendar only"}</small>
            </button>
          </div>
        )}
      </div>
      <RoutinesSection viewer={viewer} />
      <SectionHeading title="Private review" count={data.privateReviews.length} />
      <p className="section-explainer">
        Only you can see these suggestions. Keeping one does not share it with a chat or family member.
      </p>
      <div className="card-list">
        {data.privateReviews.map((review) => {
          const selectedDestination =
            shareDestinations[review.id] ?? review.destinations[0]?.conversationId ?? "";
          const shareProposal = review.shareProposal;
          return (
            <article className="review-card" key={review.id}>
              <div className="review-meta">
                <span>{review.sourceLabel}</span>
                <time>{friendlyDate(review.proposedAt)}</time>
              </div>
              <h3>{review.title}</h3>
              <p className="review-summary">{review.summary}</p>
              {review.details.length > 0 ? (
                <ul>
                  {review.details.map((detail) => (
                    <li key={detail}>{detail}</li>
                  ))}
                </ul>
              ) : null}
              {shareProposal ? (
                <div className="source-empty">
                  <strong>Exactly what the family will see</strong>
                  <p>{shareProposal.outboundText}</p>
                  <div className="review-actions">
                    <button
                      type="button"
                      className="primary-button"
                      disabled={busy?.startsWith(`approve:${shareProposal.actionIntentId}`) === true}
                      onClick={() => void approveShare(shareProposal, "once")}
                    >
                      {review.kind === "coverage_loop_update_review"
                        ? "Apply this update"
                        : "Share this once"}
                    </button>
                    {shareProposal.canCreateStandingRule ? (
                      <button
                        type="button"
                        className="quiet-button"
                        disabled={busy?.startsWith(`approve:${shareProposal.actionIntentId}`) === true}
                        onClick={() => void approveShare(shareProposal, "standing")}
                      >
                        Share matching items automatically
                      </button>
                    ) : null}
                  </div>
                  {shareProposal.standingRuleLabel ? (
                    <small>{shareProposal.standingRuleLabel}. You can stop this anytime below.</small>
                  ) : null}
                </div>
              ) : review.preparingShare ? (
                <div className="source-empty">
                  Florence is preparing or committing this exact family-safe share…
                </div>
              ) : review.destinations.length > 0 ? (
                <div className="review-actions">
                  <select
                    aria-label="Family group for this coverage item"
                    value={selectedDestination}
                    onChange={(event) =>
                      setShareDestinations((current) => ({ ...current, [review.id]: event.target.value }))
                    }
                  >
                    {review.destinations.map((destination) => (
                      <option key={destination.conversationId} value={destination.conversationId}>
                        {destination.label}
                      </option>
                    ))}
                  </select>
                  <button
                    type="button"
                    className="primary-button"
                    disabled={!selectedDestination || busy === `share:${review.id}`}
                    onClick={() => void prepareShare(review, selectedDestination)}
                  >
                    {review.kind === "coverage_loop_update_review"
                      ? "Prepare this update"
                      : "Prepare for this group"}
                  </button>
                </div>
              ) : review.kind === "coverage_proposal" || review.kind === "coverage_loop_update_review" ? (
                <div className="source-empty">
                  No fully registered family group currently allows Florence to write. Nothing can be shared
                  yet.
                </div>
              ) : null}
              <div className="review-actions">
                {review.kind === "coverage_proposal" ? (
                  <button
                    type="button"
                    className="primary-button"
                    disabled={busy === `review:${review.id}`}
                    onClick={() => void reviewCandidate(review.id, "accepted")}
                  >
                    Keep privately
                  </button>
                ) : (
                  <span className="source-empty">
                    Florence will change the existing family loop only after showing you the exact group
                    update for explicit approval.
                  </span>
                )}
                <button
                  type="button"
                  className="quiet-button"
                  disabled={busy === `review:${review.id}`}
                  onClick={() => void reviewCandidate(review.id, "rejected")}
                >
                  Dismiss
                </button>
              </div>
            </article>
          );
        })}
      </div>
      {data.privateReviews.length === 0 ? (
        <EmptyState
          title="Nothing to review"
          copy="Florence will stay quiet unless a private source contains something that may matter."
        />
      ) : null}
      <SectionHeading title="What Florence knows" count={data.memories.length} />
      <div className="item-list">
        {data.memories.map((memory) => (
          <KnowledgeRow
            key={memory.id}
            memory={memory}
            busy={busy === `memory:${memory.id}`}
            onForget={() => void forgetMemory(memory.id)}
          />
        ))}
      </div>
      <SectionHeading title="Sharing rules" count={data.rules.length} />
      <div className="item-list">
        {data.rules.map((rule) => (
          <RuleRow
            key={rule.id}
            rule={rule}
            busy={busy === `rule:${rule.id}`}
            onStop={() => void revokeRule(rule.id)}
          />
        ))}
      </div>
    </div>
  );
}

interface RoutineFormState {
  destinationConversationId: string;
  title: string;
  sharedMeaning: string;
  weekdays: number[];
  startsOn: string;
  endsOn: string;
  timeZone: string;
  localEventTime: string;
  earliestUsefulMinutesBefore: number;
  lastResponsibleMinutesBefore: number;
  notificationMode: "exceptions_only" | "always" | "silent";
  usualPersonId: string;
  standingSelfCoverage: boolean;
}

const weekdayChoices = [
  [1, "Mon"],
  [2, "Tue"],
  [3, "Wed"],
  [4, "Thu"],
  [5, "Fri"],
  [6, "Sat"],
  [7, "Sun"],
] as const;

function RoutinesSection({ viewer }: { viewer: Viewer }) {
  const { data, loading, error, reload } = useResource<RoutineView>("/api/routines");
  const [form, setForm] = useState<RoutineFormState>(() => emptyRoutineForm(viewer));
  const [editing, setEditing] = useState<{ id: string; version: number } | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);

  useEffect(() => {
    if (!data || form.destinationConversationId) return;
    const destination = data.destinations.find((candidate) => candidate.canCreate);
    if (destination) {
      setForm((current) => ({ ...current, destinationConversationId: destination.conversationId }));
    }
  }, [data, form.destinationConversationId]);

  if (loading) return <div className="skeleton" />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load family routines."} />;

  const availableDestinations = data.destinations.filter((destination) => destination.canCreate);
  const selectedDestination = data.destinations.find(
    (destination) => destination.conversationId === form.destinationConversationId,
  );
  const availablePeople = selectedDestination
    ? data.people.filter((person) => person.householdId === selectedDestination.householdId)
    : [];
  const selfIsUsual = form.usualPersonId === viewer.person.id;

  function updateForm<Key extends keyof RoutineFormState>(key: Key, value: RoutineFormState[Key]) {
    setForm((current) => ({ ...current, [key]: value }));
  }

  function resetForm() {
    const destination = availableDestinations[0];
    setEditing(null);
    setForm({
      ...emptyRoutineForm(viewer),
      destinationConversationId: destination?.conversationId ?? "",
    });
  }

  function editRoutine(routine: RoutineView["routines"][number]) {
    setEditing({ id: routine.id, version: routine.version });
    setMessage(null);
    setActionError(null);
    setForm({
      destinationConversationId: routine.destination.conversationId,
      title: routine.title,
      sharedMeaning: routine.sharedMeaning,
      weekdays: [...routine.weekdays],
      startsOn: routine.startsOn,
      endsOn: routine.endsOn ?? "",
      timeZone: routine.timeZone,
      localEventTime: routine.localEventTime,
      earliestUsefulMinutesBefore: routine.earliestUsefulMinutesBefore,
      lastResponsibleMinutesBefore: routine.lastResponsibleMinutesBefore,
      notificationMode: routine.notificationMode,
      usualPersonId: routine.holder?.personId ?? "",
      standingSelfCoverage: routine.standingSelfCoverage,
    });
  }

  async function saveRoutine(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (form.weekdays.length === 0) {
      setActionError("Choose at least one day of the week.");
      return;
    }
    const busyKey = editing ? `routine:edit:${editing.id}` : "routine:create";
    setBusy(busyKey);
    setMessage(null);
    setActionError(null);
    try {
      const payload = {
        ...form,
        endsOn: form.endsOn || null,
        usualPersonId: form.usualPersonId || null,
        standingSelfCoverage: selfIsUsual && form.standingSelfCoverage,
        ...(editing ? { expectedVersion: editing.version } : {}),
      };
      await postJson(
        editing ? `/api/routines/${editing.id}/revisions` : "/api/routines",
        viewer.csrfToken,
        payload,
      );
      setMessage(
        editing
          ? "Routine updated. Florence kept anything already in motion and refreshed future weeks."
          : "Routine added. Florence will start watching before it becomes useful.",
      );
      resetForm();
      await reload();
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "Florence could not save that routine.");
    } finally {
      setBusy(null);
    }
  }

  async function changeStatus(
    routine: RoutineView["routines"][number],
    status: "active" | "paused" | "retired",
  ) {
    if (status === "retired" && !window.confirm("Retire this routine permanently? It cannot be resumed.")) {
      return;
    }
    setBusy(`routine:status:${routine.id}`);
    setMessage(null);
    setActionError(null);
    try {
      await postJson(`/api/routines/${routine.id}/status`, viewer.csrfToken, {
        expectedVersion: routine.version,
        status,
      });
      setMessage(
        status === "active"
          ? "Routine resumed."
          : status === "paused"
            ? "Routine paused. Anything already in motion is unchanged."
            : "Routine retired. Future weeks were closed.",
      );
      if (editing?.id === routine.id) resetForm();
      await reload();
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "Florence could not update that routine.");
    } finally {
      setBusy(null);
    }
  }

  return (
    <section className="routines-section">
      <SectionHeading title="Family routines" count={data.routines.length} />
      <p className="section-explainer">
        Tell Florence what repeats. She opens each week’s handoff early enough for your family to cover it.
      </p>
      {message ? <div className="success-notice">{message}</div> : null}
      {actionError ? (
        <div className="error-notice" role="alert">
          {actionError}
        </div>
      ) : null}
      {availableDestinations.length > 0 ? (
        <details className="routine-builder" open={editing ? true : undefined}>
          <summary>{editing ? "Edit this routine" : "Add a weekly routine"}</summary>
          <form onSubmit={(event) => void saveRoutine(event)}>
            <label className="routine-field routine-field-wide">
              <span>What repeats?</span>
              <input
                required
                maxLength={200}
                placeholder="Wednesday school pickup"
                value={form.title}
                onChange={(event) => updateForm("title", event.target.value)}
              />
            </label>
            <label className="routine-field routine-field-wide">
              <span>What should Florence tell the family?</span>
              <textarea
                required
                maxLength={500}
                placeholder="Someone needs to handle Wednesday pickup."
                value={form.sharedMeaning}
                onChange={(event) => updateForm("sharedMeaning", event.target.value)}
              />
            </label>
            <fieldset className="routine-days routine-field-wide">
              <legend>Which days?</legend>
              {weekdayChoices.map(([value, label]) => (
                <label key={value}>
                  <input
                    type="checkbox"
                    checked={form.weekdays.includes(value)}
                    onChange={() =>
                      updateForm(
                        "weekdays",
                        form.weekdays.includes(value)
                          ? form.weekdays.filter((weekday) => weekday !== value)
                          : [...form.weekdays, value].sort(),
                      )
                    }
                  />
                  <span>{label}</span>
                </label>
              ))}
            </fieldset>
            <label className="routine-field">
              <span>Starts</span>
              <input
                required
                type="date"
                value={form.startsOn}
                onChange={(event) => updateForm("startsOn", event.target.value)}
              />
            </label>
            <label className="routine-field">
              <span>Happens at</span>
              <input
                required
                type="time"
                value={form.localEventTime}
                onChange={(event) => updateForm("localEventTime", event.target.value)}
              />
            </label>
            <label className="routine-field routine-field-wide">
              <span>Family group</span>
              <select
                value={form.destinationConversationId}
                onChange={(event) => {
                  updateForm("destinationConversationId", event.target.value);
                  updateForm("usualPersonId", "");
                  updateForm("standingSelfCoverage", false);
                }}
              >
                {availableDestinations.map((destination) => (
                  <option key={destination.conversationId} value={destination.conversationId}>
                    {destination.label} · {destination.participantCount} people
                  </option>
                ))}
              </select>
            </label>
            <label className="routine-field routine-field-wide">
              <span>Who usually handles it? (optional)</span>
              <select
                value={form.usualPersonId}
                onChange={(event) => {
                  updateForm("usualPersonId", event.target.value);
                  updateForm("standingSelfCoverage", false);
                }}
              >
                <option value="">No usual person</option>
                {availablePeople.map((person) => (
                  <option key={`${person.householdId}:${person.personId}`} value={person.personId}>
                    {person.name}
                  </option>
                ))}
              </select>
            </label>
            {selfIsUsual ? (
              <label className="routine-standing routine-field-wide">
                <input
                  type="checkbox"
                  checked={form.standingSelfCoverage}
                  onChange={(event) => updateForm("standingSelfCoverage", event.target.checked)}
                />
                <span>
                  <strong>Mark me as responsible every time</strong>
                  <small>
                    Use this only if you have already agreed to handle every occurrence. Florence will treat
                    it as covered without asking you again.
                  </small>
                </span>
              </label>
            ) : null}
            <details className="routine-options routine-field-wide">
              <summary>Timing and reminder options</summary>
              <div>
                <label className="routine-field">
                  <span>Start watching</span>
                  <select
                    value={form.earliestUsefulMinutesBefore}
                    onChange={(event) =>
                      updateForm("earliestUsefulMinutesBefore", Number(event.target.value))
                    }
                  >
                    <option value={60}>1 hour before</option>
                    <option value={120}>2 hours before</option>
                    <option value={180}>3 hours before</option>
                    <option value={360}>6 hours before</option>
                    <option value={720}>12 hours before</option>
                    <option value={1440}>1 day before</option>
                  </select>
                </label>
                <label className="routine-field">
                  <span>Last responsible moment</span>
                  <select
                    value={form.lastResponsibleMinutesBefore}
                    onChange={(event) =>
                      updateForm("lastResponsibleMinutesBefore", Number(event.target.value))
                    }
                  >
                    <option value={0}>At the event time</option>
                    <option value={15}>15 minutes before</option>
                    <option value={30}>30 minutes before</option>
                    <option value={60}>1 hour before</option>
                    <option value={120}>2 hours before</option>
                  </select>
                </label>
                <label className="routine-field">
                  <span>Florence should</span>
                  <select
                    value={form.notificationMode}
                    onChange={(event) =>
                      updateForm(
                        "notificationMode",
                        event.target.value as RoutineFormState["notificationMode"],
                      )
                    }
                  >
                    <option value="exceptions_only">Speak up when coverage needs attention</option>
                    <option value="always">Always check in</option>
                    <option value="silent">Track silently</option>
                  </select>
                </label>
                <label className="routine-field">
                  <span>Time zone</span>
                  <input
                    required
                    value={form.timeZone}
                    onChange={(event) => updateForm("timeZone", event.target.value)}
                  />
                </label>
                <label className="routine-field">
                  <span>Ends (optional)</span>
                  <input
                    type="date"
                    min={form.startsOn}
                    value={form.endsOn}
                    onChange={(event) => updateForm("endsOn", event.target.value)}
                  />
                </label>
              </div>
            </details>
            <div className="routine-form-actions routine-field-wide">
              <button type="submit" className="primary-button" disabled={busy !== null}>
                {editing ? "Save future weeks" : "Add routine"}
              </button>
              {editing ? (
                <button type="button" className="quiet-button" onClick={resetForm}>
                  Cancel
                </button>
              ) : null}
            </div>
          </form>
        </details>
      ) : (
        <div className="source-empty">
          Finish setting up a fully registered family group before adding a routine.
        </div>
      )}
      <div className="routine-list">
        {data.routines.map((routine) => (
          <article className="routine-card" key={routine.id}>
            <div className="routine-card-heading">
              <div>
                <h3>{routine.title}</h3>
                <p>{routine.sharedMeaning}</p>
              </div>
              <span className={`status-pill ${routine.status === "active" ? "good" : "paused"}`}>
                {routine.status === "active" ? "Active" : routine.status === "paused" ? "Paused" : "Retired"}
              </span>
            </div>
            <div className="routine-facts">
              <span>{routine.cadence}</span>
              <span>{routine.time}</span>
              <span>{routine.destination.label}</span>
              <span>
                {routine.holder
                  ? `${routine.holder.name}${routine.holder.standing ? " · usually covered" : " · usually asked"}`
                  : "No usual person"}
              </span>
            </div>
            {routine.canRevise || routine.canManage ? (
              <div className="routine-actions">
                {routine.canRevise ? (
                  <button type="button" className="quiet-button" onClick={() => editRoutine(routine)}>
                    Edit
                  </button>
                ) : null}
                {routine.canManage ? (
                  <>
                    <button
                      type="button"
                      className="quiet-button"
                      disabled={busy === `routine:status:${routine.id}`}
                      onClick={() =>
                        void changeStatus(routine, routine.status === "paused" ? "active" : "paused")
                      }
                    >
                      {routine.status === "paused" ? "Resume" : "Pause"}
                    </button>
                    <button
                      type="button"
                      className="quiet-button"
                      disabled={busy === `routine:status:${routine.id}`}
                      onClick={() => void changeStatus(routine, "retired")}
                    >
                      Retire
                    </button>
                  </>
                ) : null}
              </div>
            ) : null}
          </article>
        ))}
      </div>
      {data.routines.length === 0 ? (
        <div className="source-empty">
          No routines yet. Start with one weekly handoff your family repeats.
        </div>
      ) : null}
    </section>
  );
}

function emptyRoutineForm(viewer: Viewer): RoutineFormState {
  const today = new Date();
  const weekday = ((today.getDay() + 6) % 7) + 1;
  return {
    destinationConversationId: "",
    title: "",
    sharedMeaning: "",
    weekdays: [weekday],
    startsOn: today.toISOString().slice(0, 10),
    endsOn: "",
    timeZone: viewer.person.timezone,
    localEventTime: "15:00",
    earliestUsefulMinutesBefore: 180,
    lastResponsibleMinutesBefore: 30,
    notificationMode: "exceptions_only",
    usualPersonId: "",
    standingSelfCoverage: false,
  };
}

function SafetyPage({ viewer }: { viewer: Viewer }) {
  const { data, loading, error, reload } = useResource<DataSafetyView>("/api/safety");
  const [busy, setBusy] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const accountControlsReady =
    viewer.session.assuranceKind === "account_controls" &&
    viewer.session.assuranceExpiresAt !== null &&
    new Date(viewer.session.assuranceExpiresAt) > new Date();

  async function runAction(key: string, path: string, body: unknown, success: string, after?: () => void) {
    setBusy(key);
    setActionMessage(null);
    setActionError(null);
    try {
      await postJson(path, viewer.csrfToken, body);
      setActionMessage(success);
      if (after) after();
      else await reload();
    } catch (reason) {
      setActionError(reason instanceof Error ? reason.message : "Florence could not save that change.");
    } finally {
      setBusy(null);
    }
  }

  function requestAccountControls() {
    void runAction(
      "step-up",
      "/api/safety/request-step-up",
      { purpose: "account_controls" },
      "Check your private iMessage from Florence, then open its secure link.",
    );
  }

  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load safety controls."} />;
  return (
    <div className="page-stack">
      <Intro
        title="You stay in control"
        copy="Narrowing takes effect immediately. Sensitive or destructive changes step up through your private iMessage."
      />
      {actionMessage ? (
        <div className="success-notice" role="status">
          {actionMessage}
        </div>
      ) : null}
      {actionError ? (
        <div className="error-notice" role="alert">
          {actionError}
        </div>
      ) : null}
      <div className="settings-list">
        <Setting
          title="Pause Florence"
          copy={data.paused ? "Florence is paused." : "Stop proactive work while keeping your account."}
          action={data.paused ? "Resume" : "Pause"}
          disabled={busy === "pause"}
          onClick={() =>
            void runAction(
              "pause",
              "/api/safety/pause",
              { paused: !data.paused },
              data.paused ? "Florence resumed proactive work." : "Florence is paused.",
            )
          }
        />
        <Setting
          title="Export my data"
          copy="Download a private JSON copy. A fresh private confirmation is required."
          action={accountControlsReady ? "Download" : "Confirm privately"}
          disabled={busy === "step-up"}
          {...(accountControlsReady ? { href: "/api/safety/export" } : { onClick: requestAccountControls })}
        />
      </div>
      <SectionHeading title="Active browser sessions" count={data.sessions.length} />
      <div className="item-list">
        {data.sessions.map((session) => (
          <article className="knowledge-row" key={session.id}>
            <div>
              <h3>{session.current ? "This browser" : "Signed-in browser"}</h3>
              <p>Last used {friendlyTime(session.lastSeenAt)}</p>
            </div>
            {accountControlsReady ? (
              <button
                type="button"
                className="quiet-button"
                disabled={busy === `session:${session.id}`}
                onClick={() =>
                  void runAction(
                    `session:${session.id}`,
                    `/api/safety/sessions/${session.id}/revoke`,
                    {},
                    "That browser session was signed out.",
                    session.current ? () => window.location.assign("/") : undefined,
                  )
                }
              >
                Sign out
              </button>
            ) : null}
          </article>
        ))}
      </div>
      <SectionHeading title="Private connections" count={data.connections.length} />
      <div className="item-list">
        {data.connections.map((connection) => (
          <article className="knowledge-row" key={connection.id}>
            <div>
              <h3>{connection.email}</h3>
              <p>Google · {connection.status.replaceAll("_", " ")}</p>
            </div>
            {accountControlsReady ? (
              <button
                type="button"
                className="quiet-button"
                disabled={busy === `connection:${connection.id}`}
                onClick={() =>
                  void runAction(
                    `connection:${connection.id}`,
                    `/api/sources/${connection.id}/disconnect`,
                    {},
                    `${connection.email} was disconnected and its private source access was revoked.`,
                  )
                }
              >
                Disconnect
              </button>
            ) : null}
          </article>
        ))}
      </div>
      {!accountControlsReady && (data.sessions.length > 1 || data.connections.length > 0) ? (
        <button
          type="button"
          className="secondary-button"
          disabled={busy === "step-up"}
          onClick={requestAccountControls}
        >
          Manage private access
        </button>
      ) : null}
      <section className="danger-zone">
        <span className="section-kicker">High-risk controls</span>
        <h2>Delete my Florence account</h2>
        <p>
          This immediately fences your authority and erases your private credentials, source content, and
          memories. Family coverage that depended on you is reopened without disclosing private details.
        </p>
        <button
          type="button"
          className="danger-button"
          disabled={busy === "step-up" || busy === "delete-person"}
          onClick={() => {
            if (!accountControlsReady) return requestAccountControls();
            if (!window.confirm("Permanently delete your Florence account and private data?")) return;
            void runAction(
              "delete-person",
              "/api/safety/delete-person",
              {},
              "Your Florence account was deleted.",
              () => window.location.assign("/"),
            );
          }}
        >
          {accountControlsReady ? "Delete my account" : "Confirm privately first"}
        </button>
      </section>
    </div>
  );
}

function ChatCard({ chat }: { chat: ChatView }) {
  const labels = {
    registration_required: "Finish registration",
    observe_only: "Observe only",
    trusted_write_enabled: "Active",
    paused: "Paused",
  };
  return (
    <article className="chat-card">
      <div className="chat-top">
        <div>
          <h3>{chat.title}</h3>
          <p>
            {chat.participants.length} participants · epoch {chat.epochId.slice(0, 6)}
          </p>
        </div>
        <span className={`status-pill ${chat.mode}`}>{labels[chat.mode]}</span>
      </div>
      <div className="participant-stack">
        {chat.participants.map((participant) => (
          <div
            className={participant.registered && participant.consented ? "participant ready" : "participant"}
            key={participant.id}
            title={participant.name}
          >
            {participant.name.slice(0, 1).toUpperCase()}
          </div>
        ))}
      </div>
      {chat.blockedReason ? <div className="notice">{chat.blockedReason}</div> : null}
      <div className="chat-meta">
        <span>
          {chat.mode === "observe_only" && chat.kind === "group"
            ? chat.retentionDays
              ? `Your private view · ${chat.retentionDays}-day raw retention`
              : "No private source view"
            : chat.retentionDays
              ? `${chat.retentionDays}-day raw retention`
              : "No content retained"}
        </span>
        <span>{chat.proactive ? "Proactive help on" : "Proactive help off"}</span>
      </div>
    </article>
  );
}

function Brand({ compact = false }: { compact?: boolean }) {
  return (
    <div className={compact ? "brand compact" : "brand"}>
      <span className="brand-mark">F</span>
      {compact ? null : <span>Florence</span>}
    </div>
  );
}
function roleLabel(role: PeopleView["households"][number]["members"][number]["role"]): string {
  switch (role) {
    case "steward":
      return "parent / steward";
    case "caregiver":
      return "caregiver";
    case "dependent":
      return "child or dependent";
    default:
      return "family participant";
  }
}
function Avatar({ name, large = false }: { name: string; large?: boolean }) {
  return <div className={large ? "avatar large" : "avatar"}>{name.slice(0, 1).toUpperCase()}</div>;
}
function PromiseCard({ icon, title, copy }: { icon: string; title: string; copy: string }) {
  return (
    <article>
      <span>{icon}</span>
      <h3>{title}</h3>
      <p>{copy}</p>
    </article>
  );
}
function Intro({ title, copy }: { title: string; copy: string }) {
  return (
    <section className="intro">
      <span className="section-kicker">How Florence works</span>
      <h2>{title}</h2>
      <p>{copy}</p>
    </section>
  );
}
function SectionHeading({ title, count }: { title: string; count: number }) {
  return (
    <div className="section-heading">
      <h2>{title}</h2>
      <span>{count}</span>
    </div>
  );
}
function ExceptionRow({ item }: { item: HomeView["items"][number] }) {
  const phaseLabel =
    item.phase === "confirmed" ? "Confirmed" : item.phase === "awaiting" ? "Awaiting reply" : "Open";
  const className = `exception-row${item.phase ? ` coverage-${item.phase}` : ""}`;
  const content = (
    <>
      <span className={`urgency-dot ${item.urgency}`} />
      <div>
        <div className="exception-meta">
          <span className={item.phase ? `phase-label ${item.phase}` : "section-kicker"}>
            {item.phase ? phaseLabel : item.kind.replace("_", " ")}
          </span>
          {item.changedAt ? (
            <span>
              {item.phase === "confirmed" ? "Confirmed" : "Updated"} {friendlyTime(item.changedAt)}
            </span>
          ) : null}
        </div>
        <h3>{item.title}</h3>
        <p>{item.detail}</p>
      </div>
      {item.href ? <span className="chevron">›</span> : null}
    </>
  );
  return item.href ? (
    <a className={className} href={item.href}>
      {content}
    </a>
  ) : (
    <article className={className}>{content}</article>
  );
}
function KnowledgeRow({
  memory,
  busy,
  onForget,
}: {
  memory: SourceView["memories"][number];
  busy: boolean;
  onForget: () => void;
}) {
  return (
    <article className="knowledge-row">
      <div>
        <h3>{memory.label}</h3>
        <p>
          {memory.scope} · {memory.source} · {memory.asOf}
        </p>
      </div>
      <button type="button" className="quiet-button" disabled={busy} onClick={onForget}>
        Forget
      </button>
    </article>
  );
}
function RuleRow({
  rule,
  busy,
  onStop,
}: {
  rule: SourceView["rules"][number];
  busy: boolean;
  onStop: () => void;
}) {
  return (
    <article className="knowledge-row">
      <div>
        <h3>{rule.label}</h3>
        <p>
          {rule.source} → {rule.destination}
        </p>
      </div>
      <button type="button" className="quiet-button" disabled={busy} onClick={onStop}>
        Stop
      </button>
    </article>
  );
}
function Setting({
  title,
  copy,
  action,
  onClick,
  href,
  disabled = false,
}: {
  title: string;
  copy: string;
  action: string;
  onClick?: () => void;
  href?: string;
  disabled?: boolean;
}) {
  return (
    <article>
      <div>
        <h3>{title}</h3>
        <p>{copy}</p>
      </div>
      {href ? (
        <a className="quiet-button" href={href}>
          {action}
        </a>
      ) : (
        <button type="button" className="quiet-button" disabled={disabled} onClick={onClick}>
          {action}
        </button>
      )}
    </article>
  );
}
function EmptyState({ title, copy }: { title: string; copy: string }) {
  return (
    <div className="empty-state">
      <span>✓</span>
      <h3>{title}</h3>
      <p>{copy}</p>
    </div>
  );
}
function ErrorCard({ message }: { message: string }) {
  return (
    <div className="error-card">
      <h2>Florence needs a moment</h2>
      <p>{message}</p>
      <button type="button" className="secondary-button" onClick={() => location.reload()}>
        Try again
      </button>
    </div>
  );
}
function PageSkeleton() {
  return (
    <div className="page-stack">
      <div className="skeleton hero-skeleton" />
      <div className="skeleton" />
      <div className="skeleton" />
    </div>
  );
}
function LoadingScreen() {
  return (
    <div className="loading-screen">
      <Brand />
      <div className="loading-dot" />
    </div>
  );
}

function friendlyTime(value: string): string {
  const instant = new Date(value);
  if (Number.isNaN(instant.getTime())) return "recently";
  return new Intl.DateTimeFormat(undefined, {
    hour: "numeric",
    minute: "2-digit",
  }).format(instant);
}

function friendlyDate(value: string): string {
  const instant = new Date(value);
  if (Number.isNaN(instant.getTime())) return "Recently";
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
  }).format(instant);
}

function useResource<T>(path: string) {
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const reload = useCallback(
    async (showLoading = true) => {
      if (showLoading) setLoading(true);
      try {
        setData(await getJson<T>(path));
        setError(null);
      } catch (reason) {
        setError(reason instanceof Error ? reason.message : "Request failed");
      } finally {
        setLoading(false);
      }
    },
    [path],
  );
  useEffect(() => {
    void reload();
  }, [reload]);
  return { data, loading, error, reload };
}

createRoot(document.getElementById("root") as HTMLElement).render(
  <StrictMode>
    <BrowserRouter>
      <FlorenceApp />
    </BrowserRouter>
  </StrictMode>,
);
