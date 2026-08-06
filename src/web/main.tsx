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
  const title = useMemo(() => {
    const titles: Record<string, string> = {
      "/home": "Today",
      "/people": "Me & relationships",
      "/chats": "Chats",
      "/sources": "Sources & privacy",
      "/safety": "Data & safety",
    };
    return titles[location.pathname] ?? "Florence";
  }, [location.pathname]);

  return (
    <div className="app-shell">
      <aside className="sidebar">
        <Brand />
        <AppNavigation />
        <div className="sidebar-person">
          <Avatar name={viewer.person.name} />
          <div>
            <strong>{viewer.person.name}</strong>
            <span>{viewer.households[0]?.name ?? "Personal"}</span>
          </div>
        </div>
      </aside>
      <div className="app-column">
        <header className="mobile-header">
          <Brand compact />
          <h1>{title}</h1>
        </header>
        <main className="app-main">
          <Outlet />
        </main>
        <nav className="bottom-nav">
          <AppNavigation compact />
        </nav>
      </div>
    </div>
  );
}

function AppNavigation({ compact = false }: { compact?: boolean }) {
  const links = [
    ["/home", "Today", "⌂"],
    ["/people", "People", "◉"],
    ["/chats", "Chats", "◌"],
    ["/sources", "Sources", "⌁"],
    ["/safety", "Safety", "◇"],
  ] as const;
  return (
    <div className={compact ? "nav-links compact" : "nav-links"}>
      {links.map(([to, label, icon]) => (
        <NavLink key={to} to={to} className={({ isActive }) => (isActive ? "active" : "")}>
          <span className="nav-icon">{icon}</span>
          <span>{label}</span>
        </NavLink>
      ))}
    </div>
  );
}

function HomePage() {
  const { data, loading, error } = useResource<HomeView>("/api/home");
  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load Florence."} />;
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
          <div>
            <span className="section-kicker">Set up Florence</span>
            <h2>{data.onboarding.next ?? "You’re ready"}</h2>
          </div>
          <div className="progress">
            <span style={{ width: `${(data.onboarding.completed / data.onboarding.total) * 100}%` }} />
          </div>
          <small>
            {data.onboarding.completed} of {data.onboarding.total} complete
          </small>
        </section>
      ) : null}
      <SectionHeading title="Needs your attention" count={data.items.length} />
      {data.items.length === 0 ? (
        <EmptyState
          title="Nothing needs you right now"
          copy="Florence will stay quiet until something changes."
        />
      ) : (
        <div className="item-list">
          {data.items.map((item) => (
            <ExceptionRow key={item.id} item={item} />
          ))}
        </div>
      )}
    </div>
  );
}

function PeoplePage({ viewer }: { viewer: Viewer }) {
  const { data, loading, error, reload } = useResource<PeopleView>("/api/people");
  const [busy, setBusy] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const [inviteRoles, setInviteRoles] = useState<Record<string, "steward" | "caregiver" | "participant">>({});
  const [dependentNames, setDependentNames] = useState<Record<string, string>>({});

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

  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load your family."} />;
  return (
    <div className="page-stack">
      <Intro
        title="The people Florence coordinates with"
        copy="Use this private page for family membership and permissions. Keep ordinary family conversation in iMessage."
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
                  </div>
                  <button
                    type="button"
                    className="primary-button compact-button"
                    disabled={!invitation.canAct || busy === actionKey}
                    onClick={() =>
                      void runAction(
                        actionKey,
                        `/api/invitations/${invitation.id}/${approving ? "approve" : "accept"}`,
                        {},
                        approving
                          ? "Approved. The invitation is ready once every steward has approved."
                          : "You joined the family.",
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
              {household.members.map((member) => (
                <div className="family-person" key={member.id}>
                  <Avatar name={member.name} />
                  <div>
                    <strong>{member.name}</strong>
                    <span>
                      {member.represented ? "Represented child or dependent" : roleLabel(member.role)}
                    </span>
                  </div>
                  {member.self ? <span className="tiny-label">You</span> : null}
                  {member.represented ? <span className="tiny-label represented">Represented</span> : null}
                </div>
              ))}
            </div>
          </div>

          {household.canAddDependent ? (
            <form
              className="family-inline-form"
              onSubmit={(event) => {
                event.preventDefault();
                const displayName = dependentNames[household.id]?.trim();
                if (!displayName) return;
                void runAction(
                  `dependent:${household.id}`,
                  `/api/households/${household.id}/dependents`,
                  { displayName },
                  `${displayName} was added as a represented child or dependent.`,
                ).then((saved) => {
                  if (saved) setDependentNames((current) => ({ ...current, [household.id]: "" }));
                });
              }}
            >
              <label htmlFor={`dependent-${household.id}`}>Add a child or dependent</label>
              <div>
                <input
                  id={`dependent-${household.id}`}
                  maxLength={80}
                  placeholder="Name"
                  value={dependentNames[household.id] ?? ""}
                  onChange={(event) =>
                    setDependentNames((current) => ({ ...current, [household.id]: event.target.value }))
                  }
                />
                <button
                  type="submit"
                  className="quiet-button"
                  disabled={busy === `dependent:${household.id}` || !dependentNames[household.id]?.trim()}
                >
                  Add
                </button>
              </div>
              <small>They’re represented by the family and do not need their own Florence account.</small>
            </form>
          ) : null}

          {household.canInvite ? (
            <div className="family-subsection">
              <div className="family-section-heading">
                <div>
                  <h4>Invite from a current group</h4>
                  <p>Only registered people already sharing an iMessage group with you appear here.</p>
                </div>
              </div>
              {household.eligibleParticipants.length > 0 ? (
                <div className="family-candidates">
                  {household.eligibleParticipants.map((participant) => {
                    const selectionKey = `${household.id}:${participant.personId}`;
                    const role = inviteRoles[selectionKey] ?? "caregiver";
                    const inviteKey = `invite:${selectionKey}`;
                    return (
                      <div className="family-candidate" key={selectionKey}>
                        <div>
                          <strong>{participant.name}</strong>
                          <span>Registered in a shared group</span>
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
                          onClick={() =>
                            void runAction(
                              inviteKey,
                              `/api/households/${household.id}/invitations`,
                              {
                                conversationId: participant.conversationId,
                                inviteePersonId: participant.personId,
                                role,
                              },
                              `The invitation is ready for ${participant.name} in their private Florence account.`,
                            )
                          }
                        >
                          Invite
                        </button>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <p className="family-empty-line">
                  No one is ready to invite. Add Florence to an iMessage group and have each person register
                  privately.
                </p>
              )}
            </div>
          ) : null}

          <div className="family-subsection">
            <div className="family-section-heading">
              <div>
                <h4>Proactive coverage in groups</h4>
                <p>Florence can open and follow coverage loops only after every current person approves.</p>
              </div>
            </div>
            {household.coverageGroups.length > 0 ? (
              <div className="coverage-permissions">
                {household.coverageGroups.map((group) => {
                  const approvalKey = `coverage:${group.conversationId}`;
                  return (
                    <div className="coverage-permission" key={group.conversationId}>
                      <div>
                        <strong>{group.label}</strong>
                        <span>
                          {group.active
                            ? "Everyone approved. Florence may help with family coverage here."
                            : (group.blockedReason ??
                              `${group.approvedCount} of ${group.requiredCount} people have approved.`)}
                        </span>
                      </div>
                      {group.active ? (
                        <span className="status-pill good">Everyone approved</span>
                      ) : group.viewerApproved ? (
                        <span className="status-pill">You approved</span>
                      ) : (
                        <button
                          type="button"
                          className="quiet-button"
                          disabled={!group.canApprove || busy === approvalKey}
                          onClick={() =>
                            void runAction(
                              approvalKey,
                              `/api/chats/${group.conversationId}/coverage-rule-approval`,
                              {},
                              "Your approval is saved. The group status is updated below.",
                            )
                          }
                        >
                          Approve for me
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

function ChatsPage() {
  const { data, loading, error } = useResource<ChatView[]>("/api/chats");
  if (loading) return <PageSkeleton />;
  if (error || !data) return <ErrorCard message={error ?? "Could not load chats."} />;
  return (
    <div className="page-stack">
      <Intro
        title="Every chat is its own privacy boundary"
        copy="Florence only reads and writes when the exact current participants and their settings permit it."
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
  const location = useLocation();
  const googleStepUpReady =
    viewer.session.assuranceKind === "google_connect" &&
    viewer.session.assuranceExpiresAt !== null &&
    new Date(viewer.session.assuranceExpiresAt) > new Date();
  useEffect(() => {
    if (!data?.privateReviews.some((review) => review.preparingShare)) return;
    const poll = window.setInterval(() => void reload(), 3_000);
    return () => window.clearInterval(poll);
  }, [data, reload]);
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
    try {
      await postJson(`/api/sources/private-bridge/${proposal.actionIntentId}/approve`, viewer.csrfToken, {
        actionDigest: proposal.actionDigest,
        dataDigest: proposal.dataDigest,
        policyDigest: proposal.policyDigest,
        targetDigest: proposal.targetDigest,
        mode,
      });
      setActionMessage(
        mode === "standing"
          ? "Approved. Florence may share only matching future coverage items from that exact source."
          : "Approved once. Florence is opening the family coverage loop.",
      );
      await reload();
    } catch (reason) {
      setActionError(
        reason instanceof Error ? reason.message : "That exact sharing approval is no longer current.",
      );
    } finally {
      setBusy(null);
    }
  }
  async function requestGoogleConnect() {
    setBusy("google-connect");
    setActionError(null);
    try {
      await postJson("/api/safety/request-step-up", viewer.csrfToken, { purpose: "google_connect" });
      setActionMessage("Check your private iMessage from Florence, then open its secure link.");
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
          Google is connected. Florence is starting with new mail, then working backward quietly.
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
      <RoutinesSection viewer={viewer} />
      <SectionHeading title="Connections" count={data.connections.length} />
      <div className="card-list">
        {data.connections.map((connection) => (
          <article className="source-card" key={connection.id}>
            <div className="source-heading">
              <div className="provider-mark">G</div>
              <div>
                <h3>{connection.email}</h3>
                <p>{connection.label}</p>
              </div>
              <span className={`status-pill ${connection.gmail.liveState === "watching" ? "good" : ""}`}>
                {connection.statusLabel}
              </span>
            </div>
            <div className="source-status-grid">
              <div>
                <span>New mail</span>
                <strong>{connection.gmail.liveLabel}</strong>
                <small>
                  {connection.gmail.lastCheckedAt
                    ? `Last checked ${friendlyTime(connection.gmail.lastCheckedAt)}`
                    : "Starting shortly"}
                </small>
              </div>
              <div>
                <span>Earlier mail</span>
                <strong>{connection.gmail.backfillLabel}</strong>
                <div
                  className="mini-progress"
                  role="progressbar"
                  aria-label="Past mail setup"
                  aria-valuemin={0}
                  aria-valuemax={connection.gmail.backfillTotal}
                  aria-valuenow={connection.gmail.backfillCompleted}
                >
                  <span
                    style={{
                      width: `${(connection.gmail.backfillCompleted / connection.gmail.backfillTotal) * 100}%`,
                    }}
                  />
                </div>
              </div>
            </div>
            <div className="calendar-heading">
              <div>
                <h4>Calendars</h4>
                <p>{connection.calendarCatalogLabel}</p>
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
                        {calendar.primary ? "Primary calendar" : (calendar.timezone ?? "Google Calendar")}
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
          </article>
        ))}
      </div>
      {data.connections.length === 0 ? (
        <EmptyState
          title="No private accounts connected"
          copy="Connect Google to let Florence quietly watch new mail and show you each calendar before using it."
        />
      ) : null}
      {googleStepUpReady ? (
        <a className="secondary-button" href="/oauth/google/start">
          Continue securely to Google
        </a>
      ) : (
        <button
          type="button"
          className="secondary-button"
          disabled={busy === "google-connect"}
          onClick={() => void requestGoogleConnect()}
        >
          {data.connections.length === 0 ? "Connect Google" : "Connect another Google account"}
        </button>
      )}
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
                  <p>{shareProposal.minimumMeaning}</p>
                  <div className="review-actions">
                    <button
                      type="button"
                      className="primary-button"
                      disabled={busy?.startsWith(`approve:${shareProposal.actionIntentId}`) === true}
                      onClick={() => void approveShare(shareProposal, "once")}
                    >
                      Share this once
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
              ) : review.kind === "coverage_proposal" && review.destinations.length > 0 ? (
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
                    Prepare for this group
                  </button>
                </div>
              ) : review.kind === "coverage_proposal" ? (
                <div className="source-empty">
                  No fully registered family group currently allows Florence to write. Nothing can be shared
                  yet.
                </div>
              ) : null}
              <div className="review-actions">
                <button
                  type="button"
                  className="primary-button"
                  disabled={busy === `review:${review.id}`}
                  onClick={() => void reviewCandidate(review.id, "accepted")}
                >
                  Keep privately
                </button>
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
                  I normally have this covered. Florence may treat my choice as my standing confirmation.
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
    content_disabled: "Waiting for registration",
    read_enabled_write_disabled: "Reading silently",
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
        <span>{chat.retentionDays ? `${chat.retentionDays}-day raw retention` : "No content retained"}</span>
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
  return (
    <article className="exception-row">
      <span className={`urgency-dot ${item.urgency}`} />
      <div>
        <span className="section-kicker">{item.kind.replace("_", " ")}</span>
        <h3>{item.title}</h3>
        <p>{item.detail}</p>
      </div>
      <span className="chevron">›</span>
    </article>
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
  const reload = useCallback(async () => {
    setLoading(true);
    try {
      setData(await getJson<T>(path));
      setError(null);
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Request failed");
    } finally {
      setLoading(false);
    }
  }, [path]);
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
