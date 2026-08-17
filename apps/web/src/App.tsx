import type {
  FamilyMemberProfile,
  HouseholdVault,
  MessagesInvite,
  PreferencesInput,
  VaultContact,
  VaultFact,
  WorkspaceView,
} from "@florence/contracts";
import { Link, Outlet, useNavigate } from "@tanstack/react-router";
import { Check, ExternalLink, MessageCircle, Plus, Trash2 } from "lucide-react";
import { type FormEvent, useEffect, useState } from "react";
import { FlorenceRequestError } from "./api";
import { MemberEditor } from "./components/MemberEditor";
import {
  useCreateSession,
  useDeleteFact,
  useDeleteSession,
  useDisconnectGoogleConnection,
  useMessagesInvite,
  usePatchFact,
  usePutHousehold,
  usePutMember,
  usePutPreferences,
  useSession,
  useStartGoogleConnection,
  useWorkspace,
} from "./queries";

export function AppShell() {
  const session = useSession();
  if (session.isLoading) return <PageLoader />;
  if (session.error instanceof FlorenceRequestError && session.error.status === 401) {
    return <AccessPage />;
  }
  if (session.isError) return <LoadError error={session.error} />;

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

function AccessPage() {
  const createSession = useCreateSession();
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    try {
      await createSession.mutateAsync(required(new FormData(event.currentTarget), "accessCode"));
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not verify this access code.");
    }
  }

  return (
    <main className="access-page">
      <div className="access-brand">
        <Brand />
        <p>Private family pilot</p>
      </div>
      <form className="access-card" onSubmit={(event) => void submit(event)}>
        <div>
          <h1>Welcome to Florence</h1>
          <p>Use the private access code issued to you. Each participating adult has their own identity.</p>
        </div>
        <label className="field">
          <span>Access code</span>
          <input name="accessCode" type="password" autoComplete="current-password" required />
        </label>
        {error && <p className="form-error">{error}</p>}
        <button className="button primary wide" type="submit" disabled={createSession.isPending}>
          {createSession.isPending ? "Opening…" : "Continue"}
        </button>
      </form>
    </main>
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
    <Page title="Workspace" intro="Your next step and the places you can reach Florence.">
      <SetupChecklist view={view} />

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
  if (!vault) return <HouseholdOnboarding />;
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

      <LinqEnrollment vault={vault} />

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

function HouseholdOnboarding() {
  const put = usePutHousehold();
  const navigate = useNavigate();
  const [error, setError] = useState<string | null>(null);

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    const data = new FormData(event.currentTarget);
    try {
      await put.mutateAsync({
        name: required(data, "householdName"),
        foundingAdultDisplayName: required(data, "displayName"),
        timeZone: required(data, "timeZone"),
      });
      await navigate({ to: "/vault" });
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not create the household.");
    }
  }

  return (
    <Page title="Vault" intro="Start with the family Florence will support.">
      <form className="onboarding-form" onSubmit={(event) => void submit(event)}>
        <div className="onboarding-step">
          <span>1</span>
          <div>
            <strong>Create your household</strong>
            <p>The second adult will verify and consent independently.</p>
          </div>
        </div>
        <label className="field">
          <span>Household name</span>
          <input name="householdName" placeholder="The Barasu family" required />
        </label>
        <label className="field">
          <span>Your name</span>
          <input name="displayName" autoComplete="name" required />
        </label>
        <label className="field">
          <span>Time zone</span>
          <select name="timeZone" defaultValue={Intl.DateTimeFormat().resolvedOptions().timeZone} required>
            {timeZones().map((zone) => (
              <option value={zone} key={zone}>
                {zone.replaceAll("_", " ")}
              </option>
            ))}
          </select>
        </label>
        {error && <p className="form-error">{error}</p>}
        <button className="button primary" type="submit" disabled={put.isPending}>
          {put.isPending ? "Creating…" : "Create household"}
        </button>
      </form>
    </Page>
  );
}

function SetupChecklist({ view }: { view: WorkspaceView }) {
  const setup = view.workspace.setup;
  const adults = view.vault?.members.filter((member) => member.kind === "adult") ?? [];
  const googleConnectionCount = view.workspace.googleConnections.length;
  const adultNames = adults.map((adult) => adult.displayName).join(", ");
  const items = [
    {
      complete: setup.householdCreated,
      label: "Create your household",
      detail: "Add the family Florence will support.",
      action: (
        <Link className="button pill" to="/vault">
          Create household
        </Link>
      ),
    },
    {
      complete: setup.secondAdultAdded,
      label: "Add the second participating adult",
      detail: "Florence's pilot is one household with exactly two participating adults.",
      action: (
        <Link className="button pill" to="/vault">
          Open Vault
        </Link>
      ),
    },
    {
      complete: setup.bothAdultsMessagesConnected,
      label: "Connect both adults privately in Messages",
      detail:
        "Generate a one-use code for each adult in the Vault. That adult sends their code to Florence privately.",
      action: (
        <a className="button pill" href="/vault#messages-identities">
          Open Messages setup
        </a>
      ),
    },
    {
      complete: googleConnectionCount > 0,
      label: "Connect your Google Workspace",
      detail: "Connect Gmail and Calendar from this signed-in Florence account.",
      action: (
        <a className="button pill" href="/#google-connections">
          Connect Google
        </a>
      ),
    },
    {
      complete: setup.familyGroupConnected,
      label: "Create the exact family group in Messages",
      detail: `In Messages, tap compose and add Florence${adultNames ? `, ${adultNames}` : ""} as the only three participants. Send “Hi Florence” so Florence can verify the group.`,
      action: view.workspace.messagesUrl ? (
        <a className="button pill" href={view.workspace.messagesUrl}>
          Open Messages
        </a>
      ) : null,
    },
  ];
  const nextIndex = items.findIndex((item) => !item.complete);
  if (nextIndex === -1) return null;
  const completedCount = items.filter((item) => item.complete).length;
  const next = items[nextIndex];
  if (!next) return null;

  return (
    <section className="section setup-section">
      <div className="setup-heading">
        <SectionLabel>Next setup step</SectionLabel>
        <span>
          {completedCount} of {items.length} complete
        </span>
      </div>
      <div className="setup-next">
        <span>{nextIndex + 1}</span>
        <div>
          <strong>{next.label}</strong>
          <p>{next.detail}</p>
        </div>
        {next.action}
      </div>
    </section>
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

function LinqEnrollment({ vault }: { vault: HouseholdVault }) {
  const adults = vault.members.filter((member) => member.kind === "adult");
  return (
    <VaultSection id="messages-identities" label="Messages identities">
      <div className="identity-list">
        {adults.map((adult) => (
          <AdultIdentity key={adult.id} adult={adult} />
        ))}
      </div>
    </VaultSection>
  );
}

function AdultIdentity({ adult }: { adult: FamilyMemberProfile }) {
  const issue = useMessagesInvite();
  const [invite, setInvite] = useState<MessagesInvite | null>(null);
  const connected = adult.messagesIdentity === "connected";

  async function showInvite() {
    const result = await issue.mutateAsync(adult.id);
    setInvite(result.invite);
  }

  return (
    <div className="identity-row">
      <span className="initials">{initials(adult.displayName)}</span>
      <div>
        <strong>{adult.displayName}</strong>
        {connected ? (
          <p>Connected privately in Messages</p>
        ) : invite ? (
          <>
            <p>Send this entire code privately to Florence. It expires {formatTime(invite.expiresAt)}.</p>
            <code>{invite.code}</code>
          </>
        ) : adult.messagesIdentity === "invited" ? (
          <p>An invitation is ready. Show the current code to continue.</p>
        ) : (
          <p>Not connected to Messages</p>
        )}
        {issue.error && <p className="form-error">{issue.error.message}</p>}
      </div>
      {connected ? (
        <span className="connected-status">
          <Check size={13} /> Connected
        </span>
      ) : !invite ? (
        <button
          className="button pill"
          type="button"
          onClick={() => void showInvite()}
          disabled={issue.isPending}
        >
          {issue.isPending ? "Opening…" : adult.messagesIdentity === "invited" ? "Show code" : "Invite"}
        </button>
      ) : null}
    </div>
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

function required(data: FormData, key: string) {
  const value = data.get(key);
  if (typeof value !== "string" || !value.trim()) throw new Error(`${key} is required`);
  return value.trim();
}

function capitalize(value: string) {
  return value[0]?.toUpperCase() + value.slice(1);
}

function formatTime(value: string) {
  return new Intl.DateTimeFormat(undefined, { dateStyle: "medium", timeStyle: "short" }).format(
    new Date(value),
  );
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
