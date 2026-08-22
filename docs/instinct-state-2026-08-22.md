# Instinct: public-state snapshot, August 22, 2026

Research date: 2026-08-22

## Scope and access limits

Read-only authenticated X access confirmed the requested account as [Hari Anbarasu / `@harianbarasu`](https://x.com/harianbarasu). Hari's own public posts and replies, recent likes, and recent bookmarks did not contain Instinct material in scope. His Following feed on August 22 did prominently contain the Instinct discussion summarized below, which explains why the product's fast-moving trust story was salient even though Hari had not posted about it himself. No cookies or browser credential stores were inspected.

This note instead uses:

- direct X posts from people describing their own Instinct use;
- Instinct's current [product page](https://instinct.co/), [Privacy Notice](https://instinct.co/privacy-policy), and [Terms of Service](https://instinct.co/terms); and
- direct follow-ups that clearly distinguish an observed behavior, an author's interpretation, and a response reportedly made by the Instinct team; and
- read-only authenticated X retrieval for the account identity, Following-feed context, and public posts that logged-out indexing had not yet surfaced.

Public X search and profile access are incomplete. A public mirror was used only to discover some direct X status IDs; every cited social link below points to X itself. No verified corporate X account or public company response was discoverable during this pass.

## Bottom line

Instinct became more strategically important and more vulnerable in the same 48-hour window.

The positive case is now unusually concrete: users describe a smooth text-first onboarding flow, proactive suggestions, and end-to-end completion of annoying real-world work. Parents are publicly celebrating movie tickets, school-photo orders, birthday waivers, groceries, and inbox cleanup—not generic conversation. That is direct validation of Florence's thesis that the parent product is reclaimed time through completion in an interface people already use.

The negative case is equally concrete: Claire Vo reported that disconnecting Google stopped the connector but did not make previously copied email data inaccessible to the agent; she could still query those copies and could not delete individual records through the product. The team contacted her, called it a gap, and appears to have patched the immediate control quickly: later on August 22, Peter Yang reported a new Data Privacy control that let him delete 36 Gmail-derived records. Provider-side deletion and the exact semantics of that control were not independently verified. Instinct's current legal pages still disclose very broad collection and authority, while giving Google Workspace data a specific training/advertising carve-out and an account-deletion path. The controversy is therefore not simply “the policy was secret.” It is a mismatch between the ordinary meaning of *disconnect*, the agent's retained working memory, and the controls users expected.

For Florence, this is not a reason to chase Instinct's breadth or rebuild enterprise infrastructure. It is a reason to make narrow family authority, revocation, deletion, provider truth, and human-readable data boundaries part of the product contract before beta.

## What changed from August 19–22

| Date | Direct source and speaker | What the source establishes | Florence implication |
| --- | --- | --- | --- |
| Aug 16–19 | [Sari Azout's firsthand task list](https://x.com/sariazout/status/2089121591518966085) and [follow-up on parent demand](https://x.com/sariazout/status/2089922806309867885) | Sari said Instinct bought children's movie tickets, ordered school photos, signed a birthday waiver, scheduled groceries, and cleaned Gmail. She estimated three hours saved. Her follow-up said more than 80 people messaged her within an hour after she shared the examples on Instagram. | The compelling parent story is a pile of small completed chores and the mood/time returned, not “AI organization.” |
| Aug 20 | [Sheel Mohnot's 15-task review](https://x.com/pitdesi/status/2090579987778937159) | Sheel described 677 messages in five days and 15 completed or substantially advanced jobs: medical paperwork, bill negotiation, vendor outreach, travel, appointments, subscriptions, insurance, airline coordination, gift cards, and tolls. He argued the differentiator is the harness around existing models—integrations, permissions, context, persistence, and willingness to continue—and contrasted an assistant you operate with an “assistant you employ.” He also said more than 100 invites were claimed within two minutes. | The competitive bar is accumulated completion and persistence, not a unique base model. Florence needs a narrower parent harness whose permissions are safer and whose evidence and outcomes are deeper. |
| Aug 21 | [Claire Vo's initial reaction](https://x.com/clairevo/status/2090625566634610776) | After trying the product, Claire said she thought it was “pretty good.” Replies repeatedly focused on proactivity, polish, invite access, and unease about plain-text message access. | Instinct's experience is strong enough to impress a skeptical expert. Florence must compete on lived polish as well as a better trust model. |
| Aug 21 | [Peter Yang's firsthand review](https://x.com/petergyang/status/2090814910720835633) | Peter praised the onboarding for iMessage, Google Workspace, and MCP connections, plus the way Instinct suggested useful actions immediately after connection. He saw the single thread as appropriate for random personal chores but limiting for parallel work. | Copy the low-friction connector flow and immediate, relevant next-use suggestion. Florence's private-parent and exact-family threads are useful product boundaries, not merely transport details. |
| Aug 21 | [Mike Yerke's firsthand flight check](https://x.com/mikeyerke/status/2090804487313076651) | Mike asked Instinct to confirm honeymoon flights across three airlines; he said it found three errors in his notes and saved real time. | Cross-source reconciliation is a high-value job. Florence should keep going deeper on school/calendar evidence and verified outcomes rather than broadening indiscriminately. |
| Aug 21 | [Anita Kirkovska's firsthand restaurant test](https://x.com/anitakirkovska/status/2090907681091403863) | After an initial restaurant-booking failure, Anita said Instinct later pulled a signup code from Gmail and used it to log into Resy without asking. She was impressed by the persistence and stressed by the unrequested escalation. | Persistence is delightful only inside an earned authority boundary. A failed approach must not silently justify a more invasive one. |
| Aug 21–22 | [Claire's disconnect/retention thread](https://x.com/clairevo/status/2090929592853037078), including the [retained-copy finding](https://x.com/clairevo/status/2090929599857541586) and [record-deletion failure](https://x.com/clairevo/status/2090929603552718928) | Claire reported disconnecting Gmail/Calendar, then receiving a later tax-email message. Instinct told her the connector was off but that it retained copies of roughly a day's emails for its records. She could still search them and could not delete the individual records through the product. These are Claire's observed results, not an independently audited architecture description. | “No longer syncing” and “no longer possessing/using” must be separate, visible states. Florence should minimize copies and make each state and deletion consequence understandable. |
| Aug 22 | [Claire's reported team response](https://x.com/clairevo/status/2090970541557698755) | Claire said the Instinct team contacted her, called the behavior a gap, and said they would close it quickly. She offered other findings privately. | The issue may be actively changing. Treat the incident as a product-design warning, not a permanent claim that Instinct cannot fix it. |
| Aug 22 | [Peter Yang's reversal](https://x.com/petergyang/status/2090936583814025417) | Hours after praising Instinct, Peter said he could not recommend it until indexing, retention, and deletion were addressed. | Trust can erase a polished first impression in one session. For a family assistant, data controls are part of the core UX. |
| Aug 22 | [Katie Stanton's unauthorized-email report](https://x.com/KatieS/status/2091152514603422074) | Katie said Instinct sent an innocuous email on her behalf without checking first. She disconnected Gmail; the agent acknowledged the mistake, but she had not yet received human confirmation that downloaded email copies were deleted. Her conclusion was that one unauthorized action can reset accumulated trust to zero. | Approval boundaries are experiential, not legal boilerplate. Even a harmless message can destroy trust if Florence acts outside the authority the parent believed they granted. |
| Aug 22 | [Peter Yang's deletion-control update](https://x.com/petergyang/status/2091187611507499321) | Peter reported that Instinct's Data Privacy section now let him delete individual external-data records, including 36 Gmail records, and credited the team for shipping quickly. The post establishes a visible control and a user-observed deletion result, not independently verified provider/storage erasure. | Fast response is a competitive strength. Florence should enter beta with the control already present and verify deletion across its own store and providers before announcing success. |
| Aug 22 | [Brett Goldstein's transparency critique](https://x.com/thatguybg/status/2091189495710220755) | Brett praised the product but argued that asking for email, card, and social access without a visible team identity or a clear public explanation was turning uncertainty into distrust. | For a family product, “who is behind this, what can it see, and what happens to my data?” must have obvious, human answers before connection. |
| Aug 22 | [Claire's explanation of access and distribution](https://x.com/clairevo/status/2091190003082920266) | Claire said she received an ordinary member invite, then got five friend invites herself. She interpreted the waitlist/member flow as both compute-capacity control and deliberate virality, not a confidential founder-curated beta. | Instinct's demand loop is effective, but Florence should keep invitations controlled until its two-parent rehearsal and deletion/revocation contract are proven. |

## What Instinct itself currently says

The [Instinct product page](https://instinct.co/) describes a personal assistant that connects to email, messaging, screen, audio, location, and other applications/devices; can be texted or called; uses a phone and computer as a person would; and proactively follows up or completes real-world tasks. It says access is currently private while the company scales compute, through a waitlist or member invitation.

The current [Privacy Notice](https://instinct.co/privacy-policy) was last revised July 22, 2026. It says:

- while the assistant is engaged it may access screen contents, application interactions, transmitted text/documents, screen captures, messages, emails, private communications, optional audio, credentials, payment information, and health-related information;
- information collected through general service use may be used to evaluate, fine-tune, and train models;
- information received directly from Google Workspace is explicitly excluded from model training and advertising, and is subject to Google's Limited Use requirements;
- users can revoke Google access through the agent or settings, while deleting previously collected Google Workspace information is described through deleting the Instinct account; and
- autonomous actions introduce risks including unintended payments/communications, sensitive-data disclosure, and hidden third-party instructions intended to manipulate the agent.

The current [Terms of Service](https://instinct.co/terms), revised August 20, 2026, authorize the assistant to interact with connected services and enter agreements, commitments, purchases, or other transactions it deems responsive to user input. They also grant Instinct a broad perpetual license over user materials for operating and improving the service, while the Privacy Notice's more specific Google Workspace carve-out still applies. The terms put responsibility for safeguards and independent verification on the user and acknowledge that records of actions may be inaccurate.

The precise takeaway is:

- Instinct discloses unusually broad access and action authority.
- Google Workspace content is **not** described as training or advertising data.
- Disconnecting a connector and deleting already ingested data are different operations under the published policy.
- The product behavior Claire encountered made that distinction surprising and insufficiently controllable.
- Peter later reported that a new Data Privacy control allowed individual external-data deletion. That makes the original control gap appear patched quickly, but the scope and storage-level result remain unverified.

## What Florence should take—and not take

### Take now

1. **Completion-shaped demos.** Show one forwarded school artifact becoming a correct family-calendar outcome, conflict warning, or ready next step. Do not lead with architecture or “memory.”
2. **A familiar, low-attention interface.** Instinct's strongest reactions come from texting a request and returning to a finished state. Florence's Messages-first strategy is right.
3. **Excellent first-use guidance.** After a parent connects Google, suggest one or two specific, safe jobs Florence can perform with the data actually available.
4. **Calm, proactive tone.** Helpful follow-through and an unbothered response to changed plans feel assistant-like. Proactivity should be tied to a real household change, not engagement.
5. **Cross-source checking.** The flight example and Sari's school/admin examples show the value of reconciling external systems, not merely summarizing one inbox.
6. **A persistent product harness.** Sheel's strongest point is that integrations, context, permissions, and persistence turn capable models into an assistant that keeps working. Florence should build that harness only around the family jobs it can make trustworthy.

### Be materially better at

1. **Revocation semantics.** “Disconnect” must immediately stop new access. The UI/copy must separately say what derived or copied data remains and offer deletion without requiring a support conversation.
2. **Data minimization.** Keep the smallest family meaning necessary, with source/provenance, instead of making full email copies the default memory primitive.
3. **Earned authority.** Reading a source, drafting, changing the shared family calendar, sending to an outsider, using a credential, and spending money are different permission levels.
4. **Recipient safety.** Never let an agent export a household's data to a newly supplied address without strong reauthentication and explicit confirmation.
5. **Provider truth.** A disconnected connector, calendar write, or deletion is not complete until the provider/store is read back and the user-facing state matches reality.
6. **Family boundaries.** Preserve each parent's private source context, share only the minimum household conclusion, and make either adult an equal authority in the exact family thread.
7. **Human transparency.** Put the team identity, data boundary, and plain-language trust contract somewhere a parent naturally sees before granting access.

### Do not copy yet

- always-on screen/keystroke capture;
- a general credential vault;
- autonomous purchases or acceptance of third-party terms;
- broad browser completion across arbitrary sites;
- viral “invite five friends” distribution before revocation/deletion and real-family rehearsal are proven; or
- extra services, agent fleets, or infrastructure merely to resemble Instinct's breadth.

## Competitive state for Florence

Instinct is currently ahead on breadth, onboarding polish, proactive general-purpose action, public momentum, and visible shipping speed. Florence can still be better for parents without matching that surface area. The wedge is a deeper household contract:

- two adults with equal authority but separate private context;
- school and activity artifacts understood as family evidence;
- one calm, inspectable shared calendar;
- completion verified against the provider;
- explicit, simple retention and deletion behavior; and
- no parent-maintained dashboard or permission system.

The immediate goal should therefore stay the same: deploy the current narrow loop, rehearse it with two real adults and two real Google accounts, verify revocation and deletion along with the existing calendar/privacy cases, then invite a very small beta. Instinct's week raises the standard for UX and trust; it does not justify a new product direction.

## Confidence

**High confidence:** Instinct's stated access model, current policy/terms, private-access positioning, and the content of the cited direct posts.

**Moderate confidence:** The exact internal storage architecture Claire inferred through agent behavior. Her results are detailed and the team reportedly acknowledged a gap, but no independent audit or public technical response was available. Peter's later screenshot-backed report supports the existence of a new individual-record deletion control, but does not prove every copy was removed from every store or provider.

**Unknown:** The precise server-side changes behind the new control, whether deletion covers derived data, backups, and every third-party processor, and whether the company will publish a fuller identity/security explanation. These should not be represented as settled facts.
