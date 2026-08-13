import type { FamilyMemberProfile, HouseholdProfile } from "@florence/contracts";
import { Link, Navigate, Outlet, useNavigate } from "@tanstack/react-router";
import {
  ArrowRight,
  ChevronRight,
  CircleUserRound,
  Home,
  LockKeyhole,
  MessageCircleMore,
  PlugZap,
  Plus,
  Settings,
  ShieldCheck,
  Sparkles,
  UsersRound,
} from "lucide-react";
import { type FormEvent, useEffect, useRef, useState } from "react";
import { FlorenceRequestError, issueLinqEnrollment } from "./api";
import { MemberEditor } from "./components/MemberEditor";
import {
  useCreateHousehold,
  useCreateSession,
  useDeleteSession,
  useDisconnectGoogleConnection,
  useGoogleConnections,
  useHousehold,
  useHouseholds,
  useSession,
  useStartGoogleConnection,
  useUpsertMember,
} from "./queries";

const selectedHouseholdKey = "florence.selected-household";

export function AppShell() {
  const session = useSession();
  if (session.isLoading) return <PageLoader />;
  if (session.error instanceof FlorenceRequestError && session.error.status === 401) {
    return <AccessPage />;
  }
  if (session.isError) return <LoadError error={session.error} />;
  return (
    <div className="app-shell">
      <DesktopSidebar />
      <main className="main-shell">
        <MobileHeader />
        <Outlet />
      </main>
      <MobileNav />
    </div>
  );
}

function AccessPage() {
  const create = useCreateSession();
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    const code = read(new FormData(event.currentTarget), "accessCode");
    try {
      await create.mutateAsync(code);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not verify this access code.");
    }
  }

  return (
    <main className="access-shell">
      <section className="onboarding-copy">
        <span className="florence-orb">
          <Sparkles size={24} />
        </span>
        <p className="eyebrow">Florence family pilot</p>
        <h1>Your family's control surface stays private.</h1>
        <p>
          Use the pilot access code supplied to your household. Florence keeps the code in a secure browser
          session rather than local storage.
        </p>
      </section>
      <form className="onboarding-card access-card" onSubmit={(event) => void submit(event)}>
        <div>
          <p className="eyebrow">Household access</p>
          <h2>Open Florence</h2>
        </div>
        <label className="field">
          <span>Pilot access code</span>
          <input name="accessCode" type="password" autoComplete="current-password" required />
        </label>
        <div className="authority-note">
          <LockKeyhole size={17} />
          <span>
            This grants dashboard access only. It cannot verify another adult or expose their sources.
          </span>
        </div>
        {error && (
          <p className="form-error" role="alert">
            {error}
          </p>
        )}
        <button className="primary-button wide" type="submit" disabled={create.isPending}>
          {create.isPending ? "Opening…" : "Open household"} <ArrowRight size={17} />
        </button>
      </form>
    </main>
  );
}

export function HomePage() {
  const householdId = useSelectedHousehold();
  const household = useHousehold(householdId);
  const households = useHouseholds();

  useEffect(() => {
    if (!householdId && households.data?.[0]) selectHousehold(households.data[0].householdId);
  }, [householdId, households.data]);

  if (households.isError) return <LoadError error={households.error} />;
  if (!householdId && households.isSuccess && households.data.length === 0) {
    return <Navigate to="/onboarding" />;
  }
  if (household.isError) return <LoadError error={household.error} />;
  if (household.isLoading || !household.data) return <PageLoader />;
  const profile = household.data;
  const connectedAdults = profile.identityBoundAdultIds.length;

  return (
    <div className="page home-page">
      <PageHeading
        eyebrow="Household"
        title={profile.name}
        subtitle="Florence keeps family context here. Everyday coordination stays in iMessage."
      />
      <section className="status-grid">
        <article className="hero-card">
          <span className="card-icon">
            <MessageCircleMore size={21} />
          </span>
          <p className="eyebrow">Florence is conversation-first</p>
          <h2>
            {profile.onboardingComplete ? "Your household center is ready." : "Finish connecting the family."}
          </h2>
          <p>
            {profile.onboardingComplete
              ? "The two verified adults and household group are authorized."
              : "Add family context here, then verify the second adult and household group in iMessage."}
          </p>
          <Link className="primary-button" to="/people">
            Review your family <ArrowRight size={16} />
          </Link>
        </article>
        <article className="metric-card">
          <span>Connected adults</span>
          <strong>
            {connectedAdults}
            <small>/2</small>
          </strong>
          <p>Each adult controls their own consent and connected sources.</p>
        </article>
      </section>
    </div>
  );
}

export function OnboardingPage() {
  const create = useCreateHousehold();
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);
  const command = useRef({ commandId: crypto.randomUUID(), occurredAt: new Date().toISOString() });

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    const data = new FormData(event.currentTarget);
    try {
      const result = await create.mutateAsync({
        ...command.current,
        name: read(data, "householdName"),
        timeZone: read(data, "timeZone"),
        foundingAdultDisplayName: read(data, "displayName"),
        secondAdultDisplayName: read(data, "secondAdultDisplayName"),
        secondAdultRole: data.get("secondAdultRole") === "caregiver" ? "caregiver" : "steward",
        secondAdultRelationship: read(data, "secondAdultRelationship"),
      });
      selectHousehold(result.householdId);
      await navigate({ to: "/people" });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not create the household.");
    }
  }

  return (
    <div className="page onboarding-page">
      <div className="onboarding-copy">
        <span className="florence-orb">
          <Sparkles size={24} />
        </span>
        <p className="eyebrow">Meet Florence</p>
        <h1>Set up the family Florence will support.</h1>
        <p>
          Add both adults to the household. The other adult remains planned until they independently connect
          in iMessage and use their own browser access code.
        </p>
        <ol className="onboarding-steps">
          <li className="active">
            <span>1</span>
            <div>
              <strong>Household</strong>
              <small>Name, time zone, and both adults</small>
            </div>
          </li>
          <li>
            <span>2</span>
            <div>
              <strong>People</strong>
              <small>Adults, children, schools, activities</small>
            </div>
          </li>
          <li>
            <span>3</span>
            <div>
              <strong>Connect</strong>
              <small>Independent consent in iMessage</small>
            </div>
          </li>
        </ol>
      </div>
      <form className="onboarding-card" onSubmit={(event) => void submit(event)}>
        <div>
          <p className="eyebrow">Step 1 of 3</p>
          <h2>Your household</h2>
          <p className="section-note">You can edit these structured facts later.</p>
        </div>
        <label className="field">
          <span>Household name</span>
          <input name="householdName" placeholder="The Barasu family" required />
        </label>
        <div className="form-grid">
          <label className="field">
            <span>Other adult's name</span>
            <input name="secondAdultDisplayName" placeholder="Kendall" required />
          </label>
          <label className="field">
            <span>Relationship</span>
            <input name="secondAdultRelationship" placeholder="Co-parent" required />
          </label>
          <label className="field">
            <span>Household role</span>
            <select name="secondAdultRole" defaultValue="steward">
              <option value="steward">Steward</option>
              <option value="caregiver">Caregiver</option>
            </select>
          </label>
        </div>
        <label className="field">
          <span>Your name</span>
          <input name="displayName" autoComplete="name" required />
        </label>
        <label className="field">
          <span>Time zone</span>
          <select name="timeZone" defaultValue={Intl.DateTimeFormat().resolvedOptions().timeZone} required>
            {timeZones().map((zone) => (
              <option key={zone} value={zone}>
                {zone.replaceAll("_", " ")}
              </option>
            ))}
          </select>
        </label>
        <div className="authority-note">
          <ShieldCheck size={17} />
          <span>
            Creating the household makes you its first steward. Every other adult verifies and consents
            independently.
          </span>
        </div>
        {error && (
          <p className="form-error" role="alert">
            {error}
          </p>
        )}
        <button className="primary-button wide" type="submit" disabled={create.isPending}>
          Continue to People <ArrowRight size={17} />
        </button>
      </form>
    </div>
  );
}

export function PeoplePage() {
  const householdId = useSelectedHousehold();
  const household = useHousehold(householdId);
  const [editing, setEditing] = useState<FamilyMemberProfile | "new" | null>(null);
  const upsert = useUpsertMember(householdId ?? "none");
  if (!householdId) return <Navigate to="/onboarding" />;
  if (household.isError) return <LoadError error={household.error} />;
  if (household.isLoading || !household.data) return <PageLoader />;
  const profile = household.data;
  const adults = profile.members.filter((member) => member.kind === "adult");
  const children = profile.members.filter((member) => member.kind === "child");

  return (
    <div className="page people-page">
      <PageHeading
        eyebrow="People"
        title="Your family"
        subtitle="The structured facts Florence uses to understand names, school context, and household responsibilities."
        action={
          <button className="primary-button" type="button" onClick={() => setEditing("new")}>
            <Plus size={17} /> Add person
          </button>
        }
      />
      {editing && (
        <MemberEditor
          {...(editing === "new" ? {} : { member: editing })}
          isSaving={upsert.isPending}
          onCancel={() => setEditing(null)}
          onSave={async (memberId, input) => {
            await upsert.mutateAsync({ memberId, input });
            setEditing(null);
          }}
        />
      )}
      <MemberSection
        title="Adults"
        note="Adults remain planned until they independently verify and consent in iMessage."
        members={adults}
        onEdit={setEditing}
      />
      <LinqConnections household={profile} />
      <MemberSection
        title="Children"
        note="Children are represented family members, never account holders."
        members={children}
        onEdit={setEditing}
      />
    </div>
  );
}

function LinqConnections({ household }: { household: HouseholdProfile }) {
  const adults = household.members.filter((member) => member.kind === "adult");
  return (
    <section className="section-block people-section">
      <div className="section-heading">
        <div>
          <p className="eyebrow">iMessage</p>
          <h2>Connect each adult</h2>
          <p className="section-note">
            Each person sends their own one-time code in a private message to Florence.
          </p>
        </div>
        <span className="count-pill">{household.identityBoundAdultIds.length}/2</span>
      </div>
      <div className="connection-list">
        {adults.map((adult) => (
          <AdultConnection
            key={adult.id}
            householdId={household.householdId}
            adult={adult}
            connected={household.identityBoundAdultIds.includes(adult.id)}
          />
        ))}
      </div>
    </section>
  );
}

function AdultConnection({
  householdId,
  adult,
  connected,
}: {
  householdId: string;
  adult: FamilyMemberProfile;
  connected: boolean;
}) {
  const command = useRef({ commandId: crypto.randomUUID(), occurredAt: new Date().toISOString() });
  const [invite, setInvite] = useState<{ code: string; expiresAt: string } | null>(null);
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  async function connect() {
    setPending(true);
    setError(null);
    try {
      setInvite(await issueLinqEnrollment(householdId, adult.id, command.current));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not create this code.");
    } finally {
      setPending(false);
    }
  }

  return (
    <article className="connection-row">
      <span className={`person-avatar ${adult.kind}`}>{initials(adult.displayName)}</span>
      <div className="connection-copy">
        <strong>{adult.displayName}</strong>
        {connected ? (
          <p>Connected with independent Linq identity and consent.</p>
        ) : invite ? (
          <>
            <p>Send this entire code as a private iMessage to Florence. It expires in 24 hours.</p>
            <code className="enrollment-code">{invite.code}</code>
          </>
        ) : (
          <p>Not connected to iMessage yet.</p>
        )}
        {error && <p className="form-error">{error}</p>}
      </div>
      {connected ? (
        <span className="status-badge verified">Connected</span>
      ) : !invite ? (
        <button className="secondary-button" type="button" onClick={() => void connect()} disabled={pending}>
          {pending ? "Creating…" : "Create connect code"}
        </button>
      ) : null}
    </article>
  );
}

export function SettingsPage() {
  const signOut = useDeleteSession();
  const householdId = useSelectedHousehold();
  return (
    <div className="page settings-page">
      <PageHeading
        eyebrow="Control"
        title="Connections & privacy"
        subtitle="Connected sources and permissions belong to the adult who granted them."
      />
      <div className="settings-list">
        {householdId ? (
          <GoogleConnectionsControl householdId={householdId} />
        ) : (
          <ControlRow
            icon={<PlugZap />}
            title="Google accounts"
            text="Create your household before connecting an account."
            status="Household required"
          />
        )}
        <ControlRow
          icon={<MessageCircleMore />}
          title="iMessage household"
          text="Florence verifies each adult separately before joining the shared family group."
          status="Continue in iMessage"
        />
        <ControlRow
          icon={<LockKeyhole />}
          title="Privacy and sharing"
          text="Private source meaning stays private until its owner promotes the minimum useful family fact."
          status="Protected"
        />
      </div>
      <p className="honesty-note">
        Each Google connection belongs to the adult who authorized it. Gmail findings remain private until
        that adult explicitly shares the minimum useful family meaning.
      </p>
      <button className="secondary-button" type="button" onClick={() => void signOut.mutateAsync()}>
        Sign out of this browser
      </button>
    </div>
  );
}

function GoogleConnectionsControl({ householdId }: { householdId: string }) {
  const connections = useGoogleConnections(householdId);
  const start = useStartGoogleConnection(householdId);
  const disconnect = useDisconnectGoogleConnection(householdId);
  const error = connections.error ?? start.error ?? disconnect.error;

  async function connect() {
    const result = await start.mutateAsync();
    window.location.assign(result.authorizationUrl);
  }

  const accounts = connections.data ?? [];
  return (
    <article className="control-row google-control-row">
      <span className="control-icon">
        <PlugZap />
      </span>
      <div>
        <strong>Google accounts</strong>
        <p>
          {accounts.length
            ? accounts.map((account) => account.emailLabel).join(", ")
            : "Connect your own Gmail and Calendar account through Google's private authorization page."}
        </p>
        {error && <p className="form-error">{error.message}</p>}
      </div>
      <div className="control-actions">
        <span className="control-status">
          {connections.isLoading
            ? "Checking…"
            : accounts.length
              ? `${accounts.length} connected`
              : "Not connected"}
        </span>
        <button
          className="secondary-button compact-button"
          type="button"
          onClick={() => void connect()}
          disabled={start.isPending}
        >
          {start.isPending ? "Opening…" : accounts.length ? "Add account" : "Connect"}
        </button>
        {accounts.map((account) => (
          <button
            className="text-button"
            type="button"
            key={account.connectionId}
            onClick={() => void disconnect.mutateAsync(account.connectionId)}
            disabled={disconnect.isPending}
          >
            Disconnect {account.emailLabel}
          </button>
        ))}
      </div>
    </article>
  );
}

function MemberSection({
  title,
  note,
  members,
  onEdit,
}: {
  title: string;
  note: string;
  members: FamilyMemberProfile[];
  onEdit: (member: FamilyMemberProfile) => void;
}) {
  return (
    <section className="section-block people-section">
      <div className="section-heading">
        <div>
          <p className="eyebrow">{title}</p>
          <h2>{title}</h2>
          <p className="section-note">{note}</p>
        </div>
        <span className="count-pill">{members.length}</span>
      </div>
      {members.length ? (
        <div className="people-grid">
          {members.map((member) => (
            <button className="person-card" type="button" key={member.id} onClick={() => onEdit(member)}>
              <span className={`person-avatar ${member.kind}`}>{initials(member.displayName)}</span>
              <div>
                <strong>{member.displayName}</strong>
                <p>{member.relationship}</p>
                <div className="fact-row">
                  {member.school && <span>{member.school}</span>}
                  {member.currentGrade && <span>{member.currentGrade}</span>}
                  {member.activities?.slice(0, 2).map((activity) => (
                    <span key={activity}>{activity}</span>
                  ))}
                </div>
              </div>
              <span className={`status-badge ${member.status}`}>{member.status}</span>
              <ChevronRight size={17} />
            </button>
          ))}
        </div>
      ) : (
        <div className="calm-empty">
          <UsersRound size={22} />
          <div>
            <strong>No {title.toLowerCase()} added yet.</strong>
            <p>Use “Add person” to give Florence the family context it needs.</p>
          </div>
        </div>
      )}
    </section>
  );
}

function DesktopSidebar() {
  return (
    <aside className="sidebar">
      <Link to="/" className="brand">
        <span className="brand-mark">
          <Sparkles size={17} />
        </span>
        <span>Florence</span>
      </Link>
      <nav className="sidebar-nav" aria-label="Primary navigation">
        <NavLink to="/" icon={<Home size={18} />} label="Home" />
        <NavLink to="/people" icon={<UsersRound size={18} />} label="People" />
        <NavLink to="/settings" icon={<Settings size={18} />} label="Connections & privacy" />
      </nav>
      <div className="sidebar-note">
        <ShieldCheck size={16} />
        <span>
          Private by default
          <br />
          <small>Household truth is source-linked.</small>
        </span>
      </div>
    </aside>
  );
}

function MobileHeader() {
  return (
    <header className="mobile-header">
      <Link to="/" className="brand">
        <span className="brand-mark">
          <Sparkles size={16} />
        </span>
        <span>Florence</span>
      </Link>
      <Link to="/people" className="mobile-avatar" aria-label="Open family">
        <CircleUserRound size={21} />
      </Link>
    </header>
  );
}
function MobileNav() {
  return (
    <nav className="mobile-nav" aria-label="Mobile navigation">
      <NavLink to="/" icon={<Home size={20} />} label="Home" />
      <NavLink to="/people" icon={<UsersRound size={20} />} label="People" />
      <NavLink to="/settings" icon={<Settings size={20} />} label="Control" />
    </nav>
  );
}
function NavLink({
  to,
  icon,
  label,
}: {
  to: "/" | "/people" | "/settings";
  icon: React.ReactNode;
  label: string;
}) {
  return (
    <Link
      to={to}
      activeOptions={{ exact: to === "/" }}
      className="nav-item"
      activeProps={{ className: "nav-item active" }}
    >
      {icon}
      <span>{label}</span>
    </Link>
  );
}
function PageHeading({
  eyebrow,
  title,
  subtitle,
  action,
}: {
  eyebrow: string;
  title: string;
  subtitle: string;
  action?: React.ReactNode;
}) {
  return (
    <header className="page-header">
      <div>
        <p className="eyebrow">{eyebrow}</p>
        <h1>{title}</h1>
        <p className="header-subtitle">{subtitle}</p>
      </div>
      {action}
    </header>
  );
}
function PageLoader() {
  return (
    <div className="page page-loader">
      <span className="florence-orb">
        <Sparkles size={21} />
      </span>
      <p>Opening your household…</p>
    </div>
  );
}
function LoadError({ error }: { error: Error }) {
  return (
    <div className="page page-loader load-error">
      <span className="florence-orb">
        <LockKeyhole size={21} />
      </span>
      <strong>Florence could not open this household.</strong>
      <p>{error.message}</p>
    </div>
  );
}
function ControlRow({
  icon,
  title,
  text,
  status,
}: {
  icon: React.ReactNode;
  title: string;
  text: string;
  status: string;
}) {
  return (
    <article className="control-row">
      <span className="control-icon">{icon}</span>
      <div>
        <strong>{title}</strong>
        <p>{text}</p>
      </div>
      <span className="control-status">{status}</span>
      <ChevronRight size={18} />
    </article>
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
function read(data: FormData, key: string) {
  const value = data.get(key);
  if (typeof value !== "string" || !value.trim()) throw new Error(`${key} is required`);
  return value.trim();
}
function selectHousehold(id: string) {
  localStorage.setItem(selectedHouseholdKey, id);
  window.dispatchEvent(new Event("florence-household-selected"));
}
function useSelectedHousehold() {
  const [id, setId] = useState(() => localStorage.getItem(selectedHouseholdKey));
  useEffect(() => {
    const sync = () => setId(localStorage.getItem(selectedHouseholdKey));
    window.addEventListener("florence-household-selected", sync);
    return () => window.removeEventListener("florence-household-selected", sync);
  }, []);
  return id;
}
function timeZones() {
  return [
    ...new Set([
      Intl.DateTimeFormat().resolvedOptions().timeZone,
      "America/Los_Angeles",
      "America/Denver",
      "America/Chicago",
      "America/New_York",
      "UTC",
    ]),
  ];
}
