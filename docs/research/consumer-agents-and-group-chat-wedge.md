# Consumer Agents and Florence's Group-Chat Wedge

Research current as of August 5, 2026. Sources are first-party product pages, documentation, repositories, policies, and company retrospectives. Product claims are treated as claims, not independent proof of quality, retention, or demand.

## Executive conclusion

The product thesis should be expressed as a repeated behavior, not as a list of agent capabilities:

> **When something may matter to the family, put Florence in the loop once. Florence catches it, works out what it means, coordinates the right follow-through, and stays with it until it is handled.**

The consumer shorthand is **“send it to Florence.”** The emotional outcome is **“we stay ahead; nothing falls through.”** Memory, Gmail, calendars, PDFs, group-chat ingestion, reminders, research, and ephemeral agents are capabilities that make that behavior work. None is the behavior by itself.

Hari's broader instinct is coherent, and it changes the product boundary in an important way:

> **Florence should be a general agent when people ask, and a family Chief of Staff when it acts on its own.**

The initial wedge is not merely “AI for parents” or “an assistant in iMessage.” Ollie already markets both. The sharper wedge is:

> **A permission-aware agent that can join the group chats where family life is already coordinated, understand the surrounding context, and turn the right parts into timely follow-through without leaking anyone's private context.**

The group chat is the acquisition surface, the collaboration surface, and an unusually valuable source of context. It is not, by itself, the moat. The durable advantage would come from four systems working together:

1. **A family-context graph:** people, households, groups, schools, teams, events, obligations, preferences, and sources, all with provenance and visibility.
2. **A multi-party policy engine:** Florence knows whose data it is, where it may be used, and which audience may see each result.
3. **A reliable follow-through loop:** source material becomes a reconciled event, obligation, decision, or reminder, and Florence stays with it until it is resolved.
4. **A judgment-learning loop:** corrections become narrow, inspectable rules so Florence asks less over time without interpreting one approval as unlimited authority.

The key product tension is real: ambient reading makes Florence useful, while silent ambient reading makes Florence untrustworthy. The recommended answer is a two-stage group experience:

- **Guest mode:** Florence can be dropped into any supported chat and answer messages explicitly addressed to it. It does not build durable group memory, use members' connected private data, or act proactively.
- **Trusted group mode:** every current participant verifies their identity and agrees to ambient processing. Florence may then build group-local context and act proactively under the intersection of the participants' policies. A membership change pauses ambient behavior until the new participant consents.

This avoids a growth deadlock. Requiring every participant to become a full Florence customer before the agent says anything would make the first experience nearly impossible. Letting it silently ingest everything would create the opposite problem: a viral loop that destroys trust. Guest mode creates the first useful moment; trusted mode earns the compounding value.

## The behavior Florence must own

The [supplied X post](https://x.com/signulll/status/2084801380745908425?s=42) poses the right product test: a bundle of capabilities is not yet an enduring “how I…” behavior. The recommendations below use that framing as a prompt and first-party product sources as the evidence.

Josh Miller's retrospective on Arc Search is a useful warning for agent products: the difficult work is not adding AI but creating a consumer experience people love, repeatedly use, and change their behavior around. He describes working backward from a concrete job—getting an answer quickly—rather than from an AI feature or software category. [Josh Miller on Arc Search](https://www.hallwaychat.co/making-of-arc-search-and-finding-your-ai-product-strategy-with-josh-miller/)

Kirsten Green frames the adjacent consumer shift as moving from access to edit: overwhelmed consumers want a service to select and do, not another surface that gives them more options and tasks. Her two requirements are “magic” in the form of relief and delight, and trust that can progress from doing something with the user to doing it for them. [Forerunner's consumer-AI thesis](https://www.forerunnerventures.com/perspectives/winning-with-consumers-in-ai)

The market evidence supports that distinction. Ohai says its earlier, broadly capable household product failed to make it clear how to use the product or feel value quickly. Granola began with one legible ritual—use it during a meeting and end with trustworthy notes—then let accumulated context become a second brain. [Ohai rebuild retrospective](https://www.ohai.ai/blog/meet-the-new-ohai-household-manager-built-for-your-whole-family/), [Granola launch](https://www.granola.ai/blog/announcement), [Granola 2.0](https://www.granola.ai/blog/two-dot-zero)

Florence therefore needs a “how our family…” answer that survives even if every model and integration becomes a commodity.

### Candidate behaviors, stress-tested

| Candidate | What users would repeatedly do | Why it resonates | Why it is insufficient | Role in the thesis |
|---|---|---|---|---|
| **How our family remembers** | Tell Florence facts and later ask for them | Simple, warm, and clearly improves as context accumulates | Remembering is passive. It can produce a very knowledgeable archive while permission slips, schedule changes, and decisions still fall through. General assistants can also remember. | **Supporting capability:** Florence must remember with provenance, but memory is not the product habit. |
| **How parents stay ahead** | Check or wait for Florence's proactive guidance | Captures the emotional aspiration and the relief parents want | “Stay ahead” is an outcome, not an observable action a new user knows how to take. It also risks becoming a noisy feed of generic advice. | **Brand promise:** the user should feel ahead because the loop works. |
| **How our family makes sure nothing falls through the cracks** | Put every potentially important item into Florence once, then let Florence carry it through | Names a frequent, painful job; spans chats, emails, documents, apps, and time; produces a measurable resolution | The wording is defensive and too long for everyday speech. Florence must also avoid implying that it can guarantee outcomes outside its control. | **Core job:** this is the strongest job-to-be-done. |
| **How our family handles logistics** | Route schedules, lists, and errands through Florence | Concrete and easy to demo | Too narrow. It under-describes decisions, research, travel, family memory, and the general-agent relationship; it sounds like a calendar app. | **Initial task domain, not the lasting behavior.** |
| **How our family captures everything** | Connect or forward every source | Creates a potentially valuable context corpus | Capture is work, not relief. “Everything” sounds like surveillance, rewards hoarding, and ignores interpretation, audience, action, and deletion. | **Reject.** Florence should capture the minimum useful evidence under explicit scope. |

The recommended synthesis is:

> **Behavior:** When something might affect the family, send it to Florence—or put Florence where it is happening.
>
> **Reward:** Florence turns it into the right shared plan and follows through until it is handled.
>
> **Promise:** Our family stays ahead without one parent carrying the whole mental load.

“Send it to Florence” is the durable **family reflex**. “Florence has it” is the product's earned reassurance. “Stay ahead” is the outcome. This distinction should govern naming, onboarding, product copy, metrics, and roadmap decisions.

### The reflex must be source-agnostic

Family information does not arrive through a neat Gmail-and-calendar funnel. It appears in:

- a class-parent or neighborhood group chat;
- a coach's sports thread or app notification;
- an email body, forwarded newsletter, PDF, or attachment;
- a screenshot, photo, form, link, or paper handout;
- a spouse's direct message or household chat;
- a school portal, calendar change, travel booking, or future vertical integration; and
- a conversation that a parent summarizes in their own words.

The behavior must stay identical even when the transport changes:

| Where the information appears | Lowest-friction way to put Florence in the loop |
|---|---|
| A chat Florence is not in | Forward, quote, paste, or share the relevant messages, image, or link to Florence |
| An external group in guest mode | Mention Florence with the relevant current message, reply, attachment, or question |
| A trusted group | Let Florence catch authorized new context ambiently; mention it when an explicit decision is wanted |
| Email or attachment | Forward it, connect the source under a narrow sender/account rule, or share the file |
| School/sports/travel app | Share the notification or screenshot first; later connect a source adapter with the same policy semantics |
| Offline or remembered context | Tell Florence naturally in DM or the household chat |

All routes should converge on one application loop—**catch → interpret → reconcile → coordinate → close → learn**—rather than becoming channel-specific mini-products. Gmail is one source adapter, not Florence's product definition. WhatsApp, iMessage, PDFs, and future school or sports integrations must produce the same source-backed candidate objects and pass through the same audience and action policy.

“Ambient” should describe reduced user effort, not indiscriminate collection. Sometimes ambient means Florence is a consented participant in a trusted group. Sometimes it means a connected sender rule. In a group that has not consented, the same family reflex works through explicit forwarding or mention-only invocation.

### Activation: handle a real item before building a profile

The first session should not be a tour of what Florence can do. It should teach the reflex with one live piece of family context:

1. A parent forwards a real message, screenshot, email, PDF, or link—or adds Florence to a group and explicitly addresses it.
2. Florence states the current audience and mode in plain language.
3. It immediately extracts what matters: what changed, who is affected, when action is useful, what is ambiguous, and what source supports the interpretation.
4. It produces a state change, not a summary alone—for example, a proposed calendar event and preparation reminder, a reconciled schedule change, an RSVP decision request, or an open loop with an owner and useful deadline.
5. Florence asks at most the one question needed to proceed, then confirms what it will carry and when it will return.
6. Only after delivering value does it invite a private DM, source connection, household setup, or trusted-group consent.

The target time-to-value should be measured from the first real artifact to the first useful handled object, not from account creation to dashboard completion. No email connection should be required for that moment.

### The continuing habit loop

1. **Cue:** potentially important family information arrives in any source.
2. **Reflex:** a parent sends it to Florence, mentions Florence, or has already authorized that source or group.
3. **Immediate reward:** Florence confirms what it understood, exposes ambiguity, and shows the next useful action.
4. **Deferred reward:** Florence surfaces the item at the preparation or decision moment, coordinates the allowed audience, and closes the loop.
5. **Learning:** a correction or approval becomes a narrow source/audience/action rule; the same class of item requires less effort next time.
6. **Compounding:** more reliable context lets Florence catch conflicts and dependencies across sources, while provenance and permissions keep that context safe to use.

This becomes a default behavior only if sending once is genuinely enough. If parents must remember to copy the item into a dashboard, re-check the extraction, recreate the reminder, and later mark it done, Florence has added a step rather than changed behavior. A fast acknowledgment without durable follow-through is merely chat; a durable record without a well-timed return is merely storage.

### Seasonal intensity without relearning the product

Family life changes cadence. Back-to-school weeks, sports seasons, wedding planning, illnesses, and travel create short periods of intense coordination; summer or a quiet week may create very little. Florence should preserve the same reflex while adjusting its initiative:

- infer candidate active seasons from source-backed dates, but let the household confirm or correct them;
- increase reconciliation and preparation checks when a live season has many dependencies;
- combine low-urgency items instead of maintaining a fixed notification quota;
- understand temporary states such as “we are away until Sunday,” changing departure assumptions, destinations, quiet hours, and which obligations remain relevant;
- keep critical home/school changes visible during travel without flooding the family with routine local noise; and
- deliberately resurface deferred items at the return or season-transition boundary.

The family should not have to learn a “vacation Florence,” “school Florence,” and “sports Florence.” The one behavior remains “put Florence in the loop”; the context graph and timing engine absorb the season.

### The group-chat invitation is both activation and distribution

In a group, one parent's reflex becomes observable to everyone else:

1. Someone says, in effect, “send that to Florence,” or adds Florence to handle the current item.
2. Non-users see the agent resolve a real coordination problem in the thread where the problem already exists.
3. They can ask one guest-mode follow-up without downloading an app or granting private access.
4. Florence gives an interested person a private path to verification and onboarding.
5. That person uses the same reflex in their own household or another group.

This is stronger than a referral link because the product demonstrates its behavior before asking for adoption. It is also more fragile: one privacy surprise, irrelevant interruption, or public disclosure can teach the opposite reflex—“do not put Florence in our chats.” Consent, audience selection, and minimum disclosure are therefore part of the growth experience, not a compliance layer after it.

### A roadmap and measurement filter

Every proposed capability should pass four questions:

1. Does it make people more likely to put a meaningful family item into Florence?
2. Does it improve Florence's ability to interpret, coordinate, or close that item?
3. Does it increase trust that the result will reach only the right audience and return at the right time?
4. Does it reduce effort on the next similar item?

If not, it may be a useful general-agent feature, but it is not core to the wedge. The same applies to technical architecture: an orchestrator, knowledge graph, connector, or ephemeral worker is justified by better closure, timing, breadth of safe inputs, or learning—not by how sophisticated the agent diagram looks.

The north-star behavior metric should be the share of meaningful family inputs that a household routes through Florence first and that Florence carries to a verified resolution. Supporting metrics include repeat routing across multiple source classes, time to first handled object, source-to-object accuracy, closed-loop rate, useful intervention timing, corrections, privacy surprises, and the number of new households whose first exposure was witnessing a handled item in a group. Feature count and raw message volume are not substitutes.

## What the market already establishes

Several pieces of Florence are already legible to consumers:

- An assistant can live in messaging rather than asking the user to adopt another inbox.
- Email, calendar, reminders, and memory are becoming commodity inputs for personal agents.
- Families will invite an assistant into a shared text thread.
- Proactive background work is more valuable when it returns a decision or completed artifact, not another feed to process.
- Users want autonomy to expand after repeated successful approvals.

What is **not** publicly established is that any current product has solved multi-household group context, participant-level data boundaries, reliable ambient ingestion, and proactive family coordination together.

### Product comparison

| Product | What the first-party sources actually show | What is differentiated | What remains unproven or undisclosed | Florence lesson |
|---|---|---|---|---|
| **Ollie** | A family assistant in iMessage/SMS that monitors email and calendars, supports reminders, meals, school logistics, and tells users to add partners, caregivers, and children to a group text. Its examples show photos becoming calendar events and reminders reaching the relevant person. [Ollie](https://ollie.ai/), [product examples](https://www.ollie.ai/explore/), [family-assistant FAQ](https://ollie.ai/2026/03/10/family-assistant-faq/) | The direct family positioning, zero-app text interface, and group-thread behavior are already coherent. | Public materials do not explain participant-by-participant consent, group-local versus household memory, data flow between a member's inbox and a group, or policy behavior when membership changes. Its public invitation to add children also sits awkwardly beside its adult-only terms and privacy language. [Ollie privacy](https://ollie.ai/privacy-policy/), [terms](https://ollie.ai/terms-of-service) | Florence cannot differentiate on “family assistant by text.” It must make multi-party trust and ambient context visibly better. Keep v1 adult-facing and remove ambiguity about child participation. |
| **Ohai / O** | A household app with shared calendars, assignments, voice capture, memory, document/email extraction, and an upcoming inbox/sports-app sync. Ohai says it rebuilt the product after users reported that they did not know how to use it or feel value quickly enough. [rebuild retrospective](https://www.ohai.ai/blog/meet-the-new-ohai-household-manager-built-for-your-whole-family/), [Smart Ohai Sync](https://www.ohai.ai/blog/How-Smart-Ohai-Sync-works/) | A broad family domain model and one shared operational home. | Its own retrospective says breadth and a home screen did not automatically create activation. Most outcome claims remain first-party. | Do not lead with a dashboard or a catalog of family modules. The first real message or artifact must turn into a useful result in the existing chat. |
| **Granola** | An AI meeting notepad built around one repeatable event: use it during a meeting and end with notes grounded in the user's own steering and source transcript. Granola later expanded the accumulated meeting record into searchable shared context. [launch](https://www.granola.ai/blog/announcement), [Granola 2.0](https://www.granola.ai/blog/two-dot-zero) | One legible ritual produces immediate value and creates the corpus that makes later intelligence better. | Meetings supply a clean event boundary, one primary user, and predictable output. Family coordination is fragmented across sources, audiences, and time. | Florence needs an equally legible reflex—“send it to Florence”—even though its underlying intake and follow-through are much broader. |
| **Poke** | A general assistant in Apple Messages, WhatsApp, Telegram, and RCS with email, calendar, reminders, web search, integrations, proactive memory use, and scheduled automations. Recipes package initial context, integrations, a first behavior, and a share link. [Poke docs](https://poke.com/docs), [recipes](https://poke.com/docs/creating-recipes), [release notes](https://poke.com/docs/release-notes) | It makes a single assistant relationship portable across channels. Recipes turn useful behaviors into distributable units. | Its public documentation is account-centric; it does not disclose a family or mixed-trust group permission model. Release notes also expose the operational failures that matter—duplicate messages, time-zone mistakes, recipient errors, and approval loops. | Messaging can become habitual, and shareable behavior can distribute. Florence needs stronger shared-context semantics and family timing, not just more integrations. |
| **Orchid** | A message-first personal assistant that connects tools, runs user-defined habits, remembers personal details, monitors inbox/calendar context, and frames proactive output as work already handled versus items needing approval. Its launch post calls the product a beta and says users initially approve everything, with autonomy expanding over time. [Orchid](https://orchid.ai/), [beta announcement](https://orchid.ai/blog/orchid-beta-is-here) | The emotional promise is relief from carrying open loops, and its operational UI is an exception queue rather than an agent dashboard. | Public sources provide little detail about durable state, permission granularity, or reliability. The strongest examples are company-authored scenarios and case studies. | Florence's visible states should be “handled,” “waiting,” and “needs you.” Agent topology should stay invisible. |
| **Town** | A work-focused personal assistant that learns preferences, maintains a user-facing wiki, works from tasks, suggests repeated work, and runs routines triggered by schedules or incoming email. It exposes read-only, approval-required, and autonomous modes, including per-tool overrides and action logs. [Townies](https://www.town.com/docs/getting-started/townies), [routines](https://www.town.com/docs/features/routines), [modes and approvals](https://www.town.com/docs/safety/modes-approvals), [security](https://www.town.com/features/security) | Inspectable memory plus a progressive trust dial is substantially more concrete than a blanket “full access” promise. | The product is primarily personal/work-centric. Its public group story concerns trusted teams, not existing social groups composed of separate households. | Give every learned rule an explicit scope and give users a path from approve-once to a narrowly defined standing rule. |
| **Hermes Agent** | An open-source, provider-neutral personal agent with many messaging channels, scheduled automations, bounded persistent memory, session search, self-managed skills, and isolated child agents. Child agents receive fresh context, return only a final summary, and cannot write shared memory. Memory writes can require approval, though that gate is off by default. [repository](https://github.com/NousResearch/hermes-agent), [memory](https://hermes-agent.nousresearch.com/docs/user-guide/features/memory/), [delegation](https://hermes-agent.nousresearch.com/docs/user-guide/features/delegation/) | The closed learning loop, portable model layer, and ephemeral delegation model are strong agent-runtime patterns. | It is a self-hosted agent framework, not a consumer multi-tenant household product. File/profile isolation and messaging allowlists do not by themselves define which family fact may appear in which group. | Borrow bounded task packets, isolated workers, portable skills, and memory promotion. Do not use agent-authored files or transcripts as authoritative household truth. |
| **OpenClaw** | An open-source, always-on personal agent spanning iMessage, WhatsApp, Telegram, Signal, Slack, Discord, and many other channels. Its main session can observe group activity and receive background-agent results. It provides group allowlists, mention gates, context filtering, and per-agent tool policies. [repository](https://github.com/openclaw/openclaw), [main session](https://docs.openclaw.ai/concepts/main-session), [group policies](https://docs.openclaw.ai/channels/groups) | It demonstrates the appeal of one persistent identity across channels and a broad skills ecosystem. Its documentation is candid about group and prompt-injection hazards. | OpenClaw explicitly says its security model assumes one trusted operator, that shared-session ownership is not an isolation boundary, and that mixed-trust users need separate trust boundaries. It also separates trigger authorization from context visibility because an allowed trigger does not imply every quoted or historical message is safe to expose. [multi-user mode](https://docs.openclaw.ai/concepts/multi-user), [security](https://docs.openclaw.ai/gateway/security) | Florence needs an application-level multi-tenant authorization model, not a shared personal-agent gateway. Group admission, model context, tools, memory, and outgoing disclosure are five separate policy decisions. |
| **Maple** | A shared family email and feed that imports only selected senders and extracts dates/deadlines into family events and tasks. [Maple Email](https://www.growmaple.com/email) | Sender allowlisting gives families an understandable way to connect a private inbox without sharing everything. | It is an organizer surface, not a general proactive agent or group-chat participant. | Start private email learning broadly if the user chooses, but promote to household/group state by source rule and minimum necessary meaning. Sender rules are an excellent onboarding primitive. |
| **Yutori Scouts** | Always-on agents monitor web or connected sources, produce recurring findings or live artifacts, and let others view or subscribe. Users can refine findings through follow-ups. [Yutori Scouts](https://yutori.com/scouts) | A proactive stream becomes useful when it is a maintained artifact with feedback, not isolated chat responses. | It does not solve family identity, shared-source permissions, or household follow-through. | Florence's research workers should update a durable family decision or project, not merely send interesting links. |

### Horizontal platform assistants make general answers a baseline

ChatGPT Pulse combined conversation history, memory, Gmail, Calendar, and nightly research to produce proactive updates; OpenAI also acknowledged that early updates could resurface completed work or infer weak relevance. [ChatGPT Pulse](https://openai.com/index/introducing-chatgpt-pulse/)

Gemini Personal Intelligence reasons across Gmail, Photos, YouTube, and Search, cites or explains the personal sources it used, and allows corrections or non-personalized chats. Google explicitly calls out over-personalization, stale relationship assumptions, and confusing observed behavior with actual preference. [Gemini Personal Intelligence](https://blog.google/innovation-and-ai/products/gemini-app/personal-intelligence/)

These products mean Florence should not block ordinary questions just because they are not about parenting. That would make it feel less intelligent than a commodity assistant. It also means “general agent with Gmail” is not a durable product thesis.

The useful boundary is behavioral:

> **Florence answers broadly on request. Florence interrupts narrowly, based on an authorized family mission and current evidence.**

If someone asks for an explanation of tariffs, a restaurant recommendation, help writing a note, or the score of a game, Florence can answer. It does not proactively research generic sports trivia merely because a parent once mentioned football. Its initiative is spent on declared goals, live family obligations, meaningful changes, and likely dropped loops.

## Stripe Kai: copy the platform lessons, not the enterprise product

Stripe describes Kai as one knowledge-agent service exposed through web, Slack, browser extensions, and embedded APIs, connected to more than 1,000 tools and skills. Stripe built it after thousands of workflow-specific micro-agents became repetitive and hard to govern. Kai centralizes surface-agnostic APIs and a secure per-session execution substrate while distributed experts own skills, tools, and definitions of good output. [Stripe's Knowledge AI Platform](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform)

The most important Kai lesson for Florence is not LangChain, Kubernetes, or the number of tools. It is Stripe's authorization invariant: a person may be allowed to access two customer contexts independently while a particular task must still be forbidden from combining them. Access to data is therefore not enough; the system first needs a task-specific context boundary. That is almost exactly Florence's group-chat problem.

| Florence question | What Kai establishes | What Florence should copy | What must differ for a consumer family agent |
|---|---|---|---|
| **Source-agnostic family firehose** | One service spans many surfaces, tools, and skills. | Make iMessage, WhatsApp, web, Gmail, PDFs, school/sports adapters, and future sources interfaces into one identity, policy system, graph, and runtime. | Florence's inputs cross unrelated households and include forwarded social content, screenshots, paper, and seasonal sources. They may conflict, duplicate, be malicious, or lack identity, so ingestion must preserve evidence and uncertainty—not merely make everything searchable. |
| **Permission-aware context** | Kai distinguishes global user access from what one task may combine. | Build a purpose-and-audience envelope before retrieval; give workers only permitted evidence and short-lived tools; keep policy outside prompts. | Florence has multiple data owners and audiences without one IAM administrator. Policy must include source owner, household, exact chat membership, consent, sensitive-data rules, destination, and revocation. |
| **Group chats** | Kai appears in collaborative Slack without becoming a separate agent. Stripe says richer multi-person collaboration remains unfinished. | Treat chat as a view over shared agent state and return results in place. | Enterprise Slack has authenticated employees and administrators. Family chats can contain noncustomers, children, coaches, neighbors, and several households. Florence needs guest/mention-only and unanimously trusted modes; Kai's trust boundary does not transfer. |
| **Evidence to closure** | Kai handles deep research and artifact creation, with task-specific definitions of done. | Use explicit output contracts, keep large evidence outside active context, and let specialists return typed proposals. | A Kai artifact may finish a task. Florence's “done” can arrive weeks later, after the right people know, preparation occurs, a decision is made, and completion is confirmed. Durable objects, timers, reevaluation, and receipts must outlive sessions. |
| **Invisible specialist agents** | Stripe replaced micro-agent sprawl with one platform, governed skills, hidden selection, and per-session isolation. | Present one Florence over a common sandbox and policy substrate; keep specialist topology invisible. | The article does not say every Kai specialist is an ephemeral child agent; Kai supports very long sessions. Florence's workers should be disposable because obligations outlive sessions. They may not message, widen access, or canonize memory, and families should not receive an AgentStudio console. |

Kai's planned learning loop also separates reflection from promotion: the platform proposes and tests skill changes, then a skill owner reviews them. Florence should do the same structurally, but the reviewer is a household member approving a narrow, reversible source/audience/action rule—not a domain team changing a global agent. [Stripe's Knowledge AI Platform](https://stripe.dev/blog/meet-stripes-knowledge-ai-platform)

The resulting Florence pipeline should be:

1. A connector records an immutable source event and provenance.
2. The orchestrator fixes the mission, purpose, destination audience, and permissible evidence before retrieval.
3. Ephemeral workers receive minimal packets and return typed claims or proposals.
4. Deterministic policy and reconciliation code reject, ask, promote, or execute.
5. App-owned objects, jobs, timers, and side-effect receipts carry work across time; each surface renders only audience-appropriate state.

This is where the Kai analogy ends. Kai is principally a secure enterprise knowledge-and-artifact platform. Florence must be an ambient consumer coordination system. Retrieval quality is necessary, but its differentiated unit of value is a family loop safely carried from messy evidence to a well-timed, verified resolution.

## The group chat is a new kind of agent surface

A direct-message assistant has one principal and one conversational audience. A family group has several principals, several data owners, and one public output channel. A neighborhood, school, or team chat may contain people from many households who do not trust one another with their private data.

That changes the authorization question from:

> “Can this user use Gmail?”

to:

> “May Florence use this particular fact, derived from this person's Gmail, for this purpose, in front of this exact set of people, at this moment?”

OpenClaw's documentation provides a useful warning: who may trigger an agent and what context reaches the model are separate controls, and a shared agent is not automatically a security boundary between users. [OpenClaw group policy](https://docs.openclaw.ai/channels/groups), [OpenClaw multi-user mode](https://docs.openclaw.ai/concepts/multi-user)

Meta chose a deliberately narrow model for Meta AI in group conversations: its published privacy explanation says the assistant receives messages that explicitly invoke it, rather than the rest of the chat. [Meta generative-AI privacy explanation](https://about.fb.com/news/2023/09/privacy-matters-metas-generative-ai-features/amp/)

Florence's desired ambient behavior goes further than that. It therefore needs a visibly stronger consent and scope model.

### Recommended group modes

| Mode | What reaches model/task context | Durable memory | Private-source use | Proactivity | When it applies |
|---|---|---|---|---|---|
| **Guest / mention-only** | The addressed message, explicit reply/quote, and the minimum thread context needed to answer | No group memory beyond minimal security, consent, and delivery evidence | Never | Never | Immediately after being added; also the fallback for unverified or large groups |
| **Trusted group** | New messages in the verified chat while every current participant has consented | Group-local facts and objects with provenance | Only information already visible to that group, or an explicitly approved disclosure | Allowed under the group's interruption and capability policy | Small groups whose entire participant set has opted in |
| **Household group** | New messages in the household's verified primary chat | Household objects plus group-local context | Household information may be used only according to each source object's visibility and adult member rules | Core family Chief-of-Staff behavior | Verified adult household members and Florence |
| **Paused** | Delivery and membership events; addressed requests may receive a private recovery path | No new learning | Never | Never | Participant set changed, identity became uncertain, or a member revoked consent |
| **Revoked** | STOP/consent recovery signals only | Retain only what policy or law requires; schedule deletion of the rest | Never | Never | A participant opts out or the group is deactivated |

“Reads but does not store” should not be used as a reassuring slogan. If a model processes a message, the service has processed it. Florence should explain the real distinction: mention-only processing is narrow and ephemeral; ambient processing is explicit and creates group-scoped memory.

The messaging transport will necessarily deliver new group events to Florence's servers while its number is present. In guest mode, unaddressed message content should be rejected at verified ingress, before durable source capture or any model call. Operational metadata required for security, membership, opt-out, and deduplication is a separate, disclosed retention class.

### Consent state machine

1. A user adds Florence to an existing chat.
2. Florence sends one short disclosure explaining guest mode and how to enable trusted mode.
3. Any participant may ask a general question or explicitly hand Florence an artifact in guest mode.
4. A participant requests ambient help. Florence gives every adult participant an in-chat instruction to initiate a private identity-verification and consent DM; it does not cold-message the room's phone numbers.
5. Trusted mode activates only when the live participant set exactly matches the consented set.
6. Adding or removing a participant pauses trusted mode before the new content is used. Florence posts one neutral status message, then stays quiet.
7. After the new participant consents—or the remaining set is reverified—the group resumes.
8. Any participant can return the chat to guest mode. A person can revoke use of their private sources without disabling another person's private Florence.

The “most conservative person wins” intuition is correct **inside a shared audience**, but it should not become a global rule. A cautious member may restrict what Florence does in a group containing them. They should not disable another adult's private DM behavior or a different household's settings.

## Permission and data-flow model

### Every durable fact needs an owner, source, and audience

At minimum, a durable fact or object should carry:

- the source item and source owner;
- the author or subject, if known;
- provenance and an excerpt/reference sufficient to verify the derivation;
- confidence and last-confirmed time;
- a visibility scope: `private:member`, `household`, or `group:chat`;
- a purpose or object type, such as logistics, preference, relationship, obligation, or decision;
- any promotion approval or standing rule that widened its visibility; and
- retention and deletion status.

The destination audience must be a subset of the object's authorized audience. If it is not, Florence can redact, ask to disclose a minimum fact, or answer privately. It cannot reason that a helpful answer justifies disclosure.

### Group context stays group-local by default

A statement in a soccer-team chat should not silently become household memory, and a household calendar detail should not silently appear in the soccer chat.

Recommended defaults:

- Group messages may produce group-local decisions, dates, and open loops in trusted mode.
- A member may privately say “save that for me.” Florence can create a private pointer or derived object without copying unnecessary attributed discussion.
- Moving an item into a household requires an explicit request or a previously approved rule such as “copy confirmed practice schedule changes from this team chat to our household calendar.”
- Sharing household or private information back into an external group requires a separate disclosure decision.
- Raw messages are never treated as global personal memory simply because Florence saw them.

### Effective policy for a group response

Before retrieval or action, Florence should compute the intersection of:

1. the current verified participant set;
2. the chat's mode and capability policy;
3. the requester's identity and authority;
4. every candidate source object's visibility;
5. the strictest applicable participant disclosure rule;
6. the action's risk class and approval state; and
7. the intended destination and exact audience.

This policy check must happen before private retrieval, not after a model has already seen the data. Retrieval should begin with an audience-and-purpose envelope, then fetch only permitted context.

### Group tools are not personal tools

A person who can ask Florence a question in a group must not inherit every connected member's tool authority. Group-capable Florence should have a deliberately small capability set:

- answer and research;
- summarize allowed group context;
- propose group-local events, decisions, lists, and reminders;
- create or update a household object only under a verified promotion rule; and
- message only the current group or an authorized member DM.

It should not receive Gmail tokens, calendar credentials, arbitrary browser sessions, filesystem access, purchasing capability, or the ability to message outsiders. Workers should receive server-created, short-lived capabilities for one task. This is a product boundary, not just a prompt instruction.

## The family-context graph

The useful analogue to a knowledge platform is not “put every message in a vector database.” It is a source-backed graph that lets Florence answer: what is happening, who is affected, what changed, what remains unresolved, and who may know the answer.

### Core entities

- households and adult members;
- children as subjects, not v1 account holders;
- other people and their relationship to a household;
- chats and their exact participant history;
- schools, classrooms, teams, clubs, doctors, vendors, and recurring locations;
- connected accounts and source items;
- events, obligations, decisions, assignments, lists, projects, and reminders;
- preferences and learned rules;
- approvals, consent records, and audit events.

### Important edges

- `member_of`, `participant_in`, and `caregiver_for`;
- `attends`, `plays_for`, `scheduled_at`, and `applies_to`;
- `derived_from` and `supersedes`;
- `assigned_to`, `waiting_on`, and `completed_by`;
- `visible_to` and `approved_by`;
- `conflicts_with`, `duplicate_of`, and `updates`.

The graph should preserve disagreement. If an email says practice is at 5:00 and a coach chat says 5:30, Florence should record two claims, identify the newer or more authoritative source, and ask or reconcile. It should not overwrite history because the latest model call sounded confident.

### Memory layers

Hermes offers a useful distinction between bounded curated memory and searchable session history, and it can gate memory writes for approval. [Hermes memory](https://hermes-agent.nousresearch.com/docs/user-guide/features/memory/)

Florence needs stricter layers:

- **Raw evidence:** immutable source messages, emails, calendar events, attachments, and connector versions.
- **Extracted claims:** typed, source-linked candidate facts with confidence.
- **Canonical household objects:** reconciled events, obligations, people, decisions, and rules accepted by the application.
- **Working context:** a task-specific retrieval packet for an ephemeral worker.
- **Procedural learning:** a narrow rule or skill learned from corrections, versioned and reversible.

Only the application can promote a claim or rule. An agent summary, transcript, embedding hit, or skill file is evidence, not truth.

## The core interaction loops

### 1. Fragmented input to first value

1. A parent forwards Florence a real message, email, screenshot, PDF, form, link, or app notification—or adds Florence to a chat already coordinating a family, team, class, or event.
2. Florence explains the current audience and mode in one sentence.
3. It extracts the relevant change, date, people, obligation, and uncertainty from only the material it is allowed to process.
4. Florence returns a compact, source-linked interpretation and creates or proposes one useful handled object.
5. It states what it will carry forward and when it will return. In an external group, it offers trusted mode only after demonstrating value.

The first response should be useful even if nobody installs an app, creates a workspace, or connects Gmail. Manual share/forward is a first-class intake path, not a temporary demo mode.

Florence should not promise access to messages sent before it joined a chat. A participant can forward, quote, paste, or screenshot older context when it matters.

### 2. Private onboarding and source connection

1. A participant DMs Florence.
2. Florence verifies the phone identity and asks for the minimum profile: name, timezone, household role, partner/caregiver invitations, children, schools, and activities.
3. Gmail and Calendar authorization happen through secure browser handoffs initiated and completed in the DM.
4. Florence privately previews what it learned and asks which classes of information may become household objects.
5. The user can connect multiple accounts with human names such as “personal,” “Acme work,” or “school inbox.”

Onboarding remains an iMessage conversation even when OAuth necessarily opens a browser. There should be no dashboard scavenger hunt.

### 3. Ambient family follow-through

1. A message, email, attachment, calendar change, shared document, travel update, school/sports notification, or other authorized source item arrives.
2. Florence classifies it as irrelevant, private attention, candidate household logistics, or a group-local issue.
3. An ephemeral worker extracts dates, people, commitments, decisions, and ambiguity.
4. The orchestrator reconciles the proposal with existing objects and source versions.
5. Policy selects one of three outcomes: stay silent, send a private nudge, or propose/perform an authorized household/group action.
6. A timer triggers fresh reevaluation before any reminder is sent, including current location/travel state, active family season, completion evidence, and whether the useful action window moved.
7. The resulting correction, approval, or completion becomes evidence for a narrower future rule.

### 4. General-agent request

1. A person asks Florence anything within normal safety bounds.
2. Florence answers with general knowledge or runs bounded research.
3. Personal or family context is retrieved only if relevant and allowed in the current audience.
4. If the best answer would reveal private context, Florence offers to continue in DM or asks for a specific disclosure.
5. The answer does not become family memory unless the user explicitly saves it or it creates a real project/obligation.

This is how Florence stays a general agent without letting general breadth contaminate proactive judgment.

## Proactivity that feels like a Chief of Staff

The product should optimize for avoided mental load, not the number of proactive messages.

### Speak only when Florence can improve the state of the world

A proactive message should normally do at least one of these:

- reveal a new, time-sensitive fact;
- identify a real conflict or dropped obligation;
- ask for a decision that blocks progress;
- present work already prepared for approval;
- confirm that a previously open loop is now resolved; or
- intervene before the recipient's actual preparation or departure deadline.

Generic advice, topic recaps with no changed information, and reminders after the useful moment are failures even if they are factually correct.

### Choose the audience, not just the message

- A neutral logistical reminder may go to the group.
- A sensitive or person-specific issue goes to DM.
- A household disclosure derived from one adult's inbox should reveal only the minimum accepted meaning.
- Florence never assigns blame, speculates about why someone failed, or publicly scores family members.

### Approval should turn into precise standing authority

The desired learning loop is:

1. Florence proposes a behavior.
2. A member approves it once.
3. Florence asks whether that approval should become a rule when repetition is likely.
4. The rule records exact trigger, source scope, audience, action, exceptions, and revocation owner.
5. Future matches run without asking; near-matches do not.

“You approved a calendar event once” must never become “Florence may disclose anything from your email to every group.”

Town's per-routine and per-tool trust levels are a useful product reference: users can move a repeated behavior from read-only to approval-required to autonomous rather than grant undifferentiated access. [Town routines](https://www.town.com/docs/features/routines), [Town approval modes](https://www.town.com/docs/safety/modes-approvals)

## Orchestrator and ephemeral workers

Users should experience one Florence. They should not manage a visible org chart of agents.

The persistent product identity is the Chief of Staff. The durable system owns identity, permissions, context, obligations, approvals, timing, and audit history. For each bounded job, it can fan out to ephemeral workers such as:

- source classifier;
- schedule extractor;
- conflict checker;
- research specialist;
- meal planner;
- draft writer;
- relevance critic; or
- policy/adversarial reviewer.

Each worker receives only:

- a clear goal and stop condition;
- the minimum source excerpts it is allowed to see;
- a purpose and destination audience;
- short-lived tools that cannot widen their own authority;
- a schema for proposals; and
- no ability to message people or canonize memory.

Hermes's isolated child contexts and final-summary return are useful runtime patterns. Its documentation also makes the important distinction that ephemeral delegation is not the right durability mechanism for work that must survive interruption. [Hermes delegation](https://hermes-agent.nousresearch.com/docs/user-guide/features/delegation/)

For Florence, every durable job belongs in app-owned state. A worker can disappear and be recreated without losing the obligation or creating a duplicate side effect.

## The viral loop

The family reflex becomes a product-led distribution loop when it happens in a group, without referral spam:

1. **Add:** one user adds Florence to an existing coordination chat.
2. **Witness:** several people see “send it to Florence” resolve a real problem in place.
3. **Participate:** another member asks a follow-up without installing anything.
4. **Trust:** the group enables a richer mode or one participant DMs Florence.
5. **Personalize:** that participant creates or joins a household and connects private sources.
6. **Repeat:** they add Florence to another relevant chat.

Poke Recipes demonstrate a related distribution mechanic: a useful behavior can package its onboarding context, required integrations, and first action into a shareable setup. [Poke Recipes](https://poke.com/docs/creating-recipes)

Florence's shareable unit should eventually be a **group capability**, not an opaque prompt. Examples:

- “Turn coach messages into a verified team schedule.”
- “Track who is bringing what without nagging.”
- “Capture school dates and permission-slip deadlines.”
- “Keep this wedding-planning chat's decisions and open questions current.”

Installing one should show the sources it can read, objects it creates, audience it can message, and actions that require approval.

### K-factor metrics

The useful growth metrics are behavioral:

- households that route a second meaningful item through Florence within seven days;
- households that repeat the behavior across two or more source classes;
- active households that add Florence to at least one existing group;
- participants who interact before creating an account;
- group participants who complete a private Florence DM onboarding;
- new households created by participants who first saw Florence in a group;
- median time from group add to first useful handled object;
- groups that enable trusted mode after a guest interaction;
- groups that add Florence to a second context;
- helpful proactive interventions per active group versus corrections, mutes, and opt-outs.

Raw messages, invitations, and number of groups joined are dangerous vanity metrics. A product can increase them by being noisy while destroying the trust required for retention.

## Product risks and mitigations

| Risk | Why it matters here | Product response |
|---|---|---|
| **Privacy surprise** | People may not understand that adding an agent means a third-party service and model process chat content. | Visible guest/trusted mode, participant-level consent, plain-language source/audience explanations, and no hidden ambient mode. |
| **Cross-household leakage** | A school or team chat may contain several Florence households, each with private sources. | Group-local namespace; no household retrieval without explicit disclosure; authorization before retrieval; exact-audience checks on every send. |
| **One participant controls everyone** | The inviter may assume they can consent for the room or connect another adult's sources. | Independent identities; no silent permanent admin; group-wide policy loosening requires every affected participant. |
| **Membership churn** | A newly added person has not agreed to existing memory or behavior. | Exact live participant verification and automatic pause on every change. |
| **Prompt injection through social content** | Any group member, forwarded message, link, or attachment may manipulate a tool-enabled model. | Treat all content as untrusted; separate trigger auth from context visibility; minimal group tools; no credentials in worker context; typed proposals and deterministic policy checks. |
| **Creepy inference** | Florence may infer relationships, preferences, custody, health, or intent from casual conversation. | Source-linked claims, confidence, sensitive-category restrictions, no proactive sensitive inference, and user-visible correction/deletion. |
| **Wrong person or child** | Similar names and family relationships make entity-resolution mistakes socially costly. | Ask before ambiguous merges; preserve separate claims; never publicly attribute blame; require strong identity for assignments and disclosure. |
| **Stale learning** | Families, schools, teams, relationships, and preferences change. | Rules carry scope, provenance, last-confirmed time, expiry/review triggers, and easy revocation. |
| **Duplicate and conflicting firehose inputs** | The same practice change may arrive through email, a PDF, a coach chat, an app, and a partner's forward—with slightly different times. Treating each as a task creates noise; silently taking the latest creates errors. | Source fingerprints, claim-level provenance, canonical-object reconciliation, authority and recency rules, explicit conflict state, and one combined clarification. |
| **Season/travel miscalibration** | A normally correct school or commute assumption may be wrong during vacation, summer, illness, or a new sports season. | Versioned active-season and temporary-travel context, fresh evaluation at action time, quieting of routine noise, and deliberate return/transition resurfacing. |
| **Notification fatigue** | A group agent that comments often becomes socially unacceptable. | Mention-gated guest mode, group interruption budgets, deduplication, quiet hours, relevance thresholds, and one combined update when possible. |
| **Mental-load bias** | An assistant may reinforce the assumption that one parent—often the mother—is the default operator. | Equal adult membership, neutral language, explicit ownership, no blame, and workload visibility only when members request it. |
| **Child-data ambiguity** | Family logistics inevitably contain school, schedule, allergy, and location information. | Adult-only v1 accounts and direct interaction; minimum child data; no child profiling, location tracking, or independent messaging without a separate safety/legal launch. |
| **Channel/platform dependency** | iMessage transport policies, line health, deliverability, and WhatsApp APIs can change. | Stable internal identity and chat model, transport adapters, channel-independent consent, exportability, and degraded DM/web recovery paths. |
| **General-agent sprawl** | Broad questions can pull roadmap attention away from the family coordination wedge. | Let the model answer broad requests; invest proprietary product work in family context, permissions, timing, and follow-through. |
| **Marketing outruns reality** | Competitor pages promise assistants that “never forget,” while real systems make timing, recipient, and approval mistakes. | Measure accepted objects, corrected interventions, duplicate effects, late reminders, and privacy near-misses—not generated messages or model benchmark scores. |

## Recommended product sequence

### Layer 1: one household, private DMs, verified household group

Keep the current safety posture:

- adult-only onboarding in DM;
- source-agnostic manual intake for text, links, screenshots, photos, email forwards, and PDFs;
- independent Gmail/Calendar connections;
- private-by-source extraction;
- exact verified household group;
- neutral reminders and source-backed obligations;
- no external sends without approval;
- one Florence identity with ephemeral specialist workers.

This is the smallest setting in which real proactivity, timing, learning, and multi-adult permissions can be made trustworthy.

### Layer 2: external group guest mode

Let an onboarded user add Florence to an existing iMessage chat. Florence can:

- identify itself and explain mention-only behavior;
- answer broad questions;
- interpret explicitly addressed text, images, PDFs, and links;
- create a temporary summary or proposal for the same group; and
- invite any participant to continue privately.

It cannot ambiently ingest, use connected private context, create durable group memory, or proactively post.

### Layer 3: trusted group-local memory

Add private participant verification, unanimous ambient consent, exact membership monitoring, group-local objects, retention controls, and a small group tool profile. Start with small adult groups, not large school-wide communities.

The first capabilities should be logistics that are naturally shared by the whole room: dates, decisions, assignments, lists, changes, and unanswered questions.

### Layer 4: scoped bridges between group and household

Allow explicit or standing rules such as:

- bring confirmed team schedule changes into my household;
- privately remind me about obligations assigned to me here;
- share my household availability only as free/busy for this planning question; or
- send a household-approved RSVP back to this group.

Every bridge should specify source group, object class, destination, disclosure shape, and approving member.

### Layer 5: WhatsApp and vertical source adapters

Once policy semantics work on one transport, add WhatsApp and high-value sources such as school portals, sports systems, travel providers, and shared family tools. Until an adapter exists, forwarding, sharing, screenshotting, and attaching must remain respected intake paths. Every source adapter should create the same source-backed candidates; it should not introduce a second permission model or a new product identity.

## What not to build yet

- A visible multi-agent management console.
- A broad family dashboard as the required daily interface.
- Full-history ingestion of an existing group before participant consent.
- Automatic transfer of private Gmail or calendar details into external groups.
- Child accounts or direct child-agent interaction.
- Autonomous messaging to schools, coaches, vendors, or other parents.
- A global “learn from everything” memory bucket.
- A rule that treats every interest as a mandate for proactive research.

## Product decisions this research supports

1. **Own one family reflex.** “Send it to Florence” means put an item in once and trust Florence to interpret, coordinate, and close it.
2. **Make the reflex source-agnostic.** Gmail, iMessage, WhatsApp, PDFs, screenshots, school/sports apps, travel sources, and natural conversation are adapters into one loop.
3. **Keep Florence general in conversation.** Family scope governs initiative and privileged context, not the universe of questions it can answer.
4. **Make group chat the growth wedge.** It puts the product in the coordination stream and lets non-users witness the core behavior.
5. **Do not make ambient access the default growth mechanic.** Guest mode first; unanimous trusted mode for ambient context.
6. **Apply the strictest policy only within the shared audience.** Preserve each person's independent private Florence.
7. **Treat every group as its own context boundary.** It is not automatically a household and must never collapse multiple households into one tenant.
8. **Make permissions precede retrieval.** Redaction after an agent saw unauthorized data is not sufficient.
9. **Use app-owned, source-backed state.** Chat logs and agent memories are evidence; canonical objects and rules belong to Florence.
10. **Keep the orchestrator concept invisible.** Users talk to Florence; ephemeral workers fan out behind it.
11. **Measure timing, follow-through, and trust.** General answer quality is baseline; the product win is making parents feel ahead of their lives.
12. **Treat virality and privacy as one design problem.** The consent flow, group mode, and first useful response are the viral loop—not compliance added afterward.

## Open questions to validate with real families

These should be answered through product use rather than more abstract architecture work:

1. Do parents naturally say “send it to Florence” after one handled item, and which alternate words do they use without prompting?
2. Which first artifact—chat message, screenshot, email, PDF, or app notification—creates the fastest credible relief?
3. Does Florence's proposed action feel meaningfully better than a summary, and does it cause the household to route the next item through Florence?
4. Will small parent groups complete unanimous consent after one strong guest-mode result?
5. Which external group types feel acceptable for ambient Florence: close friends, sports teams, class parents, wedding planning, or none?
6. Is “mention-only but no memory” understandable, or do users assume Florence remembers the whole room?
7. Which group-derived facts may safely cross into a household under a standing rule: dates only, assignments to the user, decisions, or broader summaries?
8. How often is the best proactive destination the source group versus the household group versus a private DM?
9. Does visible provenance increase trust enough to justify the added message length?
10. What is the first group interaction compelling enough that another participant privately onboards Florence?
11. How should a participant who is not a Florence customer inspect, revoke, or delete their group data without creating a full account?
12. What group size makes unanimous trusted mode impractical, and should larger groups remain mention-only permanently?
13. Can Florence correctly lower and raise its initiative around vacation, school-year, and sports-season boundaries without a user reconfiguring it?
14. How much general-agent use improves retention versus distracting from the family promise?

The strongest immediate product experiment is not a new dashboard. It is a source-agnostic “send it to Florence” flow that turns a real message, screenshot, email, or PDF into one excellent handled object and earns a second routed item. The strongest growth experiment is the same behavior demonstrated through a safe guest-mode group add, followed by measuring whether another participant onboards privately or the group chooses ambient trust.

## Infrastructure feasibility note

Linq v3 treats group chats as first-class chat objects, supports participant changes, stable chat IDs over membership changes, message replies, reactions, and event webhooks. That makes the proposed interaction technically plausible on iMessage. [Linq introduction](https://docs.linqapp.com/), [group-chat FAQ](https://docs.linqapp.com/guides/resources/faq/), [group-agent guide](https://linqapp.com/blog/building-ai-agents-that-work-in-group-chats)

Linq is transport, not the trust system. Florence still has to own identity verification, consent history, exact audience, memory scope, connector authorization, action approval, and revocation.
