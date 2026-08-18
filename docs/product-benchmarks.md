# Florence product benchmarks: Instinct, Poke, Ollie, and Milo

_Research date: August 16–17, 2026. This report uses public first-party product pages, policies, documentation, and founder-authored material only. Authenticated Instinct UI and Messages observations are intentionally separated below._

## Bottom line

Florence should combine:

- **Ollie's household awareness and selective proactivity**: one assistant that watches family email and calendars, understands photos and schedules, remembers context, and coordinates the household through text.
- **Poke's messaging-native interaction quality**: natural conversation, native replies and presence, voice and attachment fluency, broad judgment, and actions performed without sending the user into another app.
- **Instinct's generalist agency and Google Workspace reach**: one model can understand context, choose tools, and follow dropped threads across Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks.
- **Milo's action discipline and postmortem lessons**: deterministic software for exact writes, probabilistic assistance for interpretation, and relentless focus on dependability without letting correctness infrastructure consume the relationship.

Florence's own differentiator is not supplied by any of them: **one household, exactly two authenticated adults, parent-authorized child information, and an explicit boundary between each adult's private context and shared household memory**.

## Product identity and status

| Product | Correct identity | Current relevance |
|---|---|---|
| **Instinct** | Spear Street Technology's autonomous personal assistant at [instinct.co](https://instinct.co/). | Active, private-access generalist benchmark. |
| **Poke** | The Interaction Company of California's messaging-native assistant at [poke.com](https://poke.com/). | Active benchmark for conversational behavior, proactivity, and tool use. |
| **Ollie** | Confabulation Corporation's family assistant at [ollie.ai](https://ollie.ai/). It expanded from a meal-planning product into a broader family assistant. | Active and the closest direct Florence competitor. |
| **Milo** | The Commons Company's Family AI at `joinmilo.com`, founded by Avni Patel Thompson. This is not `milo.ai`, `chatmilo.ai`, or `getmilo.co`. | Historical benchmark. The founder [officially discontinued Milo on January 12, 2026](https://joinmilo.substack.com/p/hellogoodbye); its product lessons and failure modes are more useful than its unavailable UI. |

## Evidence matrix

| Dimension | Instinct | Poke | Ollie | Milo |
|---|---|---|---|---|
| **Core promise** | A personal assistant that understands what matters, uses a phone and computer, follows dropped threads, and handles everyday tasks. ([Home](https://instinct.co/)) | An assistant inside familiar messaging that manages email, calendars, reminders, search, and integrations. ([Docs](https://poke.com/docs)) | A family assistant that absorbs mental load, watches calendars and inboxes, remembers household context, and keeps everyone synchronized. ([Home](https://ollie.ai/), [About](https://www.ollie.ai/about/)) | A Family AI that filtered incoming family logistics and turned them into usable information and action. ([Operating-system guide](https://joinmilo.substack.com/p/how-to-set-up-a-family-operating)) |
| **Channels and native behavior** | Publicly supports texting and calling, plus screen, messaging, audio, and location context. Public sources do not verify iMessage, replies, reactions, typing state, multiple bubbles, or group-chat semantics. ([Home](https://instinct.co/)) | Apple Messages, WhatsApp, Telegram, and RCS. Poke documents iMessage inline replies; WhatsApp read receipts, typing indicators, and inline replies; and voice-message input. Reactions and household group semantics are not publicly documented. ([Docs](https://poke.com/docs), [Release notes](https://poke.com/docs/release-notes), [Home](https://poke.com/)) | iMessage and SMS/MMS/RCS, including family group chats. Native replies, reactions, typing/read state, multi-bubble pacing, and voice notes are not publicly documented. ([FAQ](https://ollie.ai/2026/03/10/family-assistant-faq/), [Terms](https://ollie.ai/terms-of-service)) | Primarily SMS, with email as a fallback for submitted tasks and events after opting out of SMS. No first-party evidence establishes native iMessage behavior or voice. ([Terms](https://www.joinmilo.com/terms)) |
| **Proactivity** | Claims proactive calls/texts, follow-up on abandoned threads, ride arrangements, and service-provider bookings. ([Home](https://instinct.co/)) | Automatically uses memory and integrations at useful moments; paid plans add real-time background automations, while Recipes can establish recurring behavior. ([Home](https://poke.com/), [Recipe docs](https://poke.com/docs/creating-recipes)) | Monitors inbox, calendar, and tasks; sends selective alerts, daily briefs, conflict warnings, conditional reminders, and contextual check-ins. ([Email assistant](https://ollie.ai/email-assistant/), [Examples](https://www.ollie.ai/explore/)) | Used daily SMS rundowns for reminders, events, tasks, meals, and pickup/drop-off ownership, plus conflict alerts. ([Operating-system guide](https://joinmilo.substack.com/p/how-to-set-up-a-family-operating)) |
| **Memory and personalization** | Uses prior interactions and personal context for suggestions, but publicly documents no memory editor or private/shared partition. ([Privacy](https://instinct.co/privacy-policy)) | Public documentation confirms a persistent named Memory, but not its contents, editing model, or sharing rules. ([Account merging](https://poke.com/docs/merging-accounts)) | Describes a family profile, preferences, goals, reminders, schedules, and contextual facts reused over time. It does not document private-adult versus household memory. ([FAQ](https://ollie.ai/2026/03/10/family-assistant-faq/), [Examples](https://www.ollie.ai/explore/)) | Collected household details and organized retained information into a family calendar, task lists, and small reference lists such as contacts, birthdays, and recipes. ([Privacy](https://www.joinmilo.com/privacy), [Working-memory article](https://joinmilo.substack.com/p/how-to-rescue-our-overloaded-working)) |
| **Email, calendar, and Google** | Explicitly documents Gmail, Calendar, Drive, Docs, Sheets, Slides, and Tasks. Workspace data is excluded from model training and advertising, and access can be revoked in conversation or settings. ([Privacy](https://instinct.co/privacy-policy)) | Full Gmail and Outlook inbox, calendar, and contact support; Gmail search, drafting, sending, labeling, and organization are documented. Public material does not establish Drive/Docs/Sheets access. ([FAQ](https://poke.com/faq), [Home](https://poke.com/)) | Gmail/Outlook read, search, label, and draft capabilities; Google/Outlook Calendar read/write, availability, and briefing access. Drive/Docs/Sheets are not documented. ([FAQ](https://ollie.ai/2026/03/10/family-assistant-faq/)) | Centered on a shared family Google Calendar and family-information intake. Public founder material discusses school emails and calendars, but not current broad Gmail API access. ([Operating-system guide](https://joinmilo.substack.com/p/how-to-set-up-a-family-operating)) |
| **PDFs, images, voice, and documents** | Can receive text, documents, screen captures, audio, and voice; PDF-specific behavior is not documented. ([Privacy](https://instinct.co/privacy-policy), [Terms](https://instinct.co/terms)) | Supports voice input, image-based data extraction, email attachments, PDF generation/editing, and image/PDF preview before sending. A general inbound-PDF parsing contract is not explicit. ([Release notes](https://poke.com/docs/release-notes)) | Explicitly extracts events from school-flyer photos and uploaded PDF events, and understands photos such as fridge contents. Voice and general-purpose document parsing/generation are not documented. ([Family calendar](https://ollie.ai/family-calendar), [Examples](https://www.ollie.ai/explore/)) | Founder material describes intake of messaging snippets and difficult school PDFs, classifying them into events, reminders, conflicts, or retained information. Voice and document generation were not established. ([Founder product note](https://www.linkedin.com/posts/apatelthompson_as-so-many-of-you-know-im-building-milo-activity-7311448999348051968-TTTd)) |
| **Web, onboarding, and trust** | Public sources establish web, macOS, and mobile apps plus Google connection/revocation, but not the signed-in information architecture. Its assistant can access screens, private communications, credentials, payment data, audio, and location when enabled. Non-Workspace interaction data may be used for model improvement and personalized advertising. ([Privacy](https://instinct.co/privacy-policy)) | The web surface exposes messaging settings, integrations, Recipes, subscriptions, account/privacy controls, and a developer Kitchen. Users can connect/disconnect integrations. The public main-account onboarding flow is not documented in enough detail to copy. ([Integration docs](https://poke.com/docs/managing-integrations), [Recipe docs](https://poke.com/docs/creating-recipes), [Privacy](https://poke.com/privacy)) | Starts by text without an initial account or app, then uses OAuth for email/calendar. It claims no sale of personal data or model training on user data and supports disconnection/deletion. Public sources do not show a family Workspace, Vault, or detailed family onboarding. ([Home](https://ollie.ai/), [Privacy](https://www.ollie.ai/privacy-policy/)) | Historical onboarding collected detailed family information; the product experimented with a kitchen screen, shared calendar/tasks, triage center, and school alerts. The web product is no longer available. ([Farewell](https://joinmilo.substack.com/p/hellogoodbye)) |
| **Action and autonomy** | Describes an always-on assistant, when engaged, that thinks, plans, acts, and can engage third parties. Its exact approval, undo, spending, and audit policies are not public. ([Privacy](https://instinct.co/privacy-policy)) | Interprets a message, selects tools, reads/writes through connected services, acts, and responds. Public docs do not define consistent approval rules for email, calendar writes, purchases, or destructive actions. ([API docs](https://poke.com/docs/api), [Integration docs](https://poke.com/docs/managing-integrations)) | Marketing examples show automatic monitoring, reminders, and some calendar additions; Gmail drafting is documented. There is no public, consistent approval/source/undo contract. | Milo articulated the strongest useful boundary: deterministic software for certainty-sensitive operations, while parents review/edit probabilistic suggestions for ambiguous planning. ([Founder lessons](https://joinmilo.substack.com/p/modern-parenthood-invisible-load)) |

## Authenticated Instinct UI observations — root-owned section

> These are authorized observations from the user's signed-in Instinct workspace on August 16–17, 2026. They are not claims derived from public documentation.

- **Navigation:** only Workspace and Vault occupy primary navigation. Preferences sits behind the account menu alongside invite and sign-out actions. There is no third primary product area to fill with settings or status.
- **Workspace:** one sparse page combines the assistant's contact channels with connectors. Contact offers Messages, WhatsApp, phone, and email. Connector cards expose Google Workspace, Outlook, Linear, GitHub, Slack, Granola, and WhatsApp with one plain-language capability sentence and one Connect button each. There is no project dashboard, task board, chat transcript, activity feed, or infrastructure status surface.
- **Vault:** a second top-level page contains four direct categories—Logins, Cards, Addresses, and Phones—each with a single Add action. Adding an item opens one compact modal that says its values are encrypted, asks only for the relevant fields, and hides the uncommon address field behind an Advanced disclosure. Florence should preserve the simple Vault information architecture but reinterpret it around family members, schools, caregivers, addresses, phone numbers, schedules, and documents; passwords and payment cards are explicitly out of scope.
- **Preferences:** display name; phone and iCloud-email sign-in methods; and Light, Dark, or System appearance. For Florence this maps cleanly to a self-owned display name, read-only verified Messages/phone/sign-in identity, Google revocation and cross-calendar consent, appearance, and sign-out—not a password/recovery system or a panel of speculative behavior toggles.
- **Messages-first relationship:** the authorized thread shows that the relationship starts with an ordinary “Hi.” Instinct welcomes the user in a few natural bubbles, explains its capability in human terms, asks only for a first name, and then asks for one real thing the user would rather not handle. Account and invite links appear inside that established conversation. Florence should copy that order, then use mobile web for the trust-heavy parts: the adult's identity and caregiver attestation, private Google connection, optional partner, and children/school/activity context. It should not ask the family to name an internal household record or expose browser-detected time zone as an onboarding chore.
- **Observed web onboarding rhythm:** the live unauthenticated flow is a full-height white canvas with a centered `340px` column, `24px` side padding, an `80px` rounded mark, one `28px` title, restrained secondary copy, `48px` controls, and one black pill primary action. After phone verification it advances through one focused task at a time—identity, email confirmation, then channel choice—without a visible step counter, progress rail, bordered wizard card, or dense multi-section form. Florence should preserve that single-task rhythm while changing the questions to its family-specific sequence.
- **Private relationships as connective tissue:** when asked how it would serve two adults, Instinct recommended separate private accounts and Google connections, a default of never volunteering one adult's context to the other, explicit ownership and communication preferences, and a deliberately shared logistics layer. It also stated clearly that nothing had been connected or changed during the hypothetical walkthrough. Instinct itself does not use a household group chat; Florence will keep the exact family group as a useful shared coordination layer, while adopting the stronger principle that the two private threads are the trust anchors and crossing information is an explicit product act.
- **Live Messages behavior:** in an authorized conversation, Instinct reacted immediately to substantive requests, used one short progress acknowledgment before multi-minute research, then returned in two or three natural bubbles with a firm recommendation, concrete options, remembered constraints, and at most one high-leverage question. It adapted an active research task immediately when the user corrected a preference, explained why the recommendation changed, and continued without restarting the conversation. It also supplied useful screenshots and direct links instead of making the user manage a browsing workflow.
- **Language is not admission:** when asked what it would do with an ordinary request for a replacement setup link, Instinct said it would issue a fresh one without questions or friction and would use thread context to disambiguate the link. It reserved explicit approval for money, communication in the user's voice, and access or sharing changes; research, drafts, comparisons, and planning remained its job. Florence should therefore put deterministic strictness around identity, private scope, exact consequential objects, and provider effects—not around a dictionary of acceptable English.
- **Live parent-document behavior:** given a fictional school PDF with an explicit no-action/no-retention instruction, Instinct reacted immediately with eyes and answered about a minute later in four distinct replies. It led with the two paper/deadline items that mattered, separated all calendar-worthy dates, detected a hidden pickup-versus-bus-return collision, named the standing rules and contacts it would normally remember, asked one yes/no chaperone question, and explicitly confirmed that it saved and scheduled nothing. After a correction to both adults' availability, it re-ranked without restarting, reduced the problem to the unsupervised pickup gap, drafted one exact message to the teacher, did not send it, and again confirmed that nothing was saved or scheduled. It retained still-relevant earlier trip context while treating the availability correction narrowly. The lesson is behavioral, not architectural: fast native acknowledgment, judgment before extraction, provenance-aware memory, correction without reset, and a clear distinction between drafting and acting.
- **Selective proactivity:** when asked what it would monitor, Instinct named concrete decision windows and material-change thresholds, distinguished useful alerts from ordinary noise, and reduced the interruption rule to two questions: does the user need to decide something now, or is there a useful deadline they would be annoyed to miss? If neither is true, it stays quiet.
- **Memory transparency:** when challenged, Instinct separated user-stated facts from its own inferences and taste, named missing context about another person rather than fabricating it, and invited correction of the assumptions. Florence should make this distinction inspectable for both retained memory and one-off recommendations.
- **Useful imperfection:** after being told not to book anything, Instinct still volunteered that it would later acquire a paid attraction ticket. That is a model-level boundary failure even though it did not purchase anything during the test. Florence should copy Instinct's conversational initiative, but consequential-action authorization must be enforced deterministically at the provider/action boundary and must honor an explicit negative instruction across later follow-ups.
- **Visual system:** the live UI uses licensed GT America regular, medium, and bold with `-apple-system`, BlinkMacSystemFont, and system fallbacks. Measured light-theme tokens are white background/card, `#101113` primary text, `#e5e7eb` borders, `#f3f4f6` muted surfaces, primary text at 60% for secondary copy and 35% for tertiary copy, and a blue focus ring. Body text uses `-0.011em` tracking. The desktop shell has a 200px fixed sidebar, 48px header, and a centered 700px content column with 32px page padding and 636px inner content. At a real 430px viewport the sidebar becomes a shadowed slide-over drawer, the title bar remains compact, Workspace uses a 16px gutter, and Vault uses a 32px inner gutter.
- **Component rhythm:** page titles are 13px/500; ordinary section headings are 14px/500; Vault category labels are 12px/500 uppercase with `0.14em` tracking. Connector cards use a one-pixel border, 12px radius, and 12px-by-16px padding. Their Connect actions are 32px pill buttons. Contact actions are 44px pill buttons. Empty Vault rows use a one-pixel dashed border, six-pixel radius, and 12px-by-16px padding. Preferences group ordinary labels and explanatory secondary copy above quiet bordered cards. There are no gradients, charts, status metrics, decorative dashboard widgets, or gratuitous animation.
- **Match as closely as practical:** reproduce the measured information architecture, proportions, typography, spacing, density, modal behavior, and interaction patterns while using Florence's own copy, data, and properly licensed or equivalent assets.
- **Reinterpret for Florence:** replace the personal credential/payment Vault with a family-information Vault; restrict connectors to Google Workspace; add explicit two-adult privacy and child-data controls that Instinct does not expose.

## What Florence should copy

### 1. Make conversation the product

Use Poke and Instinct as the interaction-quality benchmark: concise judgment, native inline replies, reactions, typing presence, intentionally paced bubbles, voice and attachment input, topic resumption, and the ability to stay silent. Some of these Florence requirements are not publicly verified for every benchmark; they remain explicit product requirements rather than borrowed factual claims.

### 2. Make the household—not tasks—the organizing model

Use Ollie as the closest functional reference and Milo as the clearest conceptual one. Florence should understand two adults, children, schools, caregivers, activities, addresses, phone numbers, doctors, important contacts, preferences, schedules, uploaded documents, and family history. It should turn messy intake into one of five outcomes:

1. event;
2. reminder;
3. conflict or decision;
4. retained information;
5. no action.

This is a family memory and judgment system, not a generalized workflow engine.

### 3. Launch with one source ecosystem

Support Google Workspace only: Gmail, Calendar, Drive, Docs, and Sheets, plus direct PDF and ordinary iPhone-photo intake. Email and calendars should feel like senses Florence already has, not separate feature areas. Voice notes and arbitrary Office files are deferred. Do not build Poke's connector marketplace, Recipes, arbitrary MCP catalog, or API platform for the pilot.

### 4. Make proactivity selective and relational

Copy Ollie's strongest pattern: monitor quietly, send an optional concise daily brief, surface exceptional time-sensitive items, follow up on real commitments, and back off when ignored. Include emotional continuity and gentle check-ins when context warrants them. Avoid continuous summaries, engagement loops, and nagging.

### 5. Keep the web app a trusted control surface

The web app should provide only what messaging cannot safely or clearly provide:

- household onboarding and the two adult identities;
- children, schools, caregivers, addresses, and phone numbers;
- Google connections and permission scopes;
- uploaded family documents and their extracted facts;
- shared household memory and each adult's private memory controls;
- Calendar approval receipts, corrections, item deletion, and disconnect controls;
- appearance and the adult's own verified identity state.

It should not become another calendar, inbox, project dashboard, task board, workflow builder, or document editor. Google remains the durable source for email/calendar/files; Florence provides understanding and control.

### 6. Use a visible autonomy contract

Florence may automatically read authorized sources, research, remember, answer, react, ask follow-ups, maintain commitments, and send high-confidence useful nudges. Research, comparison, drafting, narrowing, and monitoring are Florence's work. Spending money, contacting someone as the family, submitting or booking something, disclosing private information, or making an irreversible external change requires the exact action to be presented once for approval; a direct instruction to perform that exact action is the approval. A negative instruction such as “don't book anything” remains binding for the whole active task and any later follow-up. Provider execution must enforce this boundary deterministically and return a visible source and result. Provide correction and undo where the provider permits it.

### 7. Treat trust as product behavior

Neither Instinct nor Poke supplies a two-adult household model. Florence needs:

- exact adult and chat membership authorization;
- parent-authorized, minimal child profiles;
- private memory for each adult that cannot leak into shared chat;
- explicit sharing scope on documents and facts;
- source provenance for extracted dates and commitments;
- permission revocation and complete deletion;
- no passwords, payment cards, screen recording, ambient location tracking, advertising, or model training on household data.

Ollie's public copy creates an avoidable ambiguity by inviting children into group chats while its terms reserve use for adults. Florence's pilot should be unambiguous: children may be represented in household data, but only the two enrolled adults participate directly.

## What Florence should deliberately avoid

- **Instinct's device-control blast radius:** no always-on screen capture, location tracking, credentials, payments, or broad computer control.
- **Poke's platform breadth:** no integration marketplace, Recipes, developer Kitchen, arbitrary MCPs, website builder, multiple messaging channels, or human-operator service at launch.
- **Ollie's feature sprawl:** do not simultaneously build health tracking, newborn coaching, meal planning, accountability coaching, and every family list. Validate the core household loop first.
- **A second productivity system:** do not clone Gmail, Calendar, Drive, tasks, or family-OS rituals inside Florence.
- **Vague memory promises:** do not say Florence “remembers everything.” Show what is remembered, where it came from, who can see it, and how to correct/delete it.
- **Silent consequential action:** natural conversation does not excuse invisible sends, calendar mutations, disclosure, or purchases.
- **Ambiguous child participation:** the first release has exactly two participating adults.
- **Milo's failure mode:** do not build custom models, large correctness scaffolding, or infrastructure intended to compensate for uncertain model behavior. Milo's founder concluded that the broad Family AI repeatedly produced promising demonstrations without dependable production behavior and ultimately shut it down. ([Farewell](https://joinmilo.substack.com/p/hellogoodbye))
- **Correctness without relationship:** Milo's founder also described the danger of prioritizing reliability so heavily that conversational fun lagged. Florence must prove both action reliability and the feeling of a capable person in the family chat. ([Founder reflection](https://www.linkedin.com/posts/apatelthompson_this-right-here-is-why-its-taken-so-long-activity-7326665999888318465-pHiP))

## Product target in one sentence

**Florence is the warm, capable household chief of staff in the family chat: Ollie's family awareness, Poke's native conversational feel, Instinct's Google-capable generalist agency, and Milo's deterministic action discipline—bounded by a clearer two-adult privacy and trust model than any of them publicly documents.**

## Primary sources

- Instinct: [home](https://instinct.co/), [privacy](https://instinct.co/privacy-policy), [terms](https://instinct.co/terms)
- Poke: [home](https://poke.com/), [docs](https://poke.com/docs), [release notes](https://poke.com/docs/release-notes), [integrations](https://poke.com/docs/managing-integrations), [API](https://poke.com/docs/api), [privacy](https://poke.com/privacy), [terms](https://poke.com/terms)
- Ollie: [home](https://ollie.ai/), [family-assistant FAQ](https://ollie.ai/2026/03/10/family-assistant-faq/), [family calendar](https://ollie.ai/family-calendar), [email assistant](https://ollie.ai/email-assistant/), [examples](https://www.ollie.ai/explore/), [privacy](https://www.ollie.ai/privacy-policy/), [terms](https://ollie.ai/terms-of-service)
- Milo: [farewell/postmortem](https://joinmilo.substack.com/p/hellogoodbye), [family operating-system guide](https://joinmilo.substack.com/p/how-to-set-up-a-family-operating), [AI/product lessons](https://joinmilo.substack.com/p/modern-parenthood-invisible-load), [privacy](https://www.joinmilo.com/privacy), [terms](https://www.joinmilo.com/terms)
