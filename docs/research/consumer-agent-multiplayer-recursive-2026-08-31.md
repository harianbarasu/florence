# Consumer-agent multiplayer, recursively researched

**Research date:** August 31, 2026  
**Recent-X window:** August 17–31, 2026, with older primary sources followed when a recent post pointed to them  
**Question:** What do the Rebecca Kaden and Scott Belsky threads actually point to, how do the named products handle solo and multiplayer use, and what can Florence do materially better for parents?

## Executive summary

The recursive branch resolves to **two products, not three**:

- **Ollie** is the direct competitor. `@blennon_` is Bill Lennon, Ollie's co-founder and CEO—not a separate product. Ollie already offers both a private one-person text and a shared household group text. Its multiplayer model is one Ollie service identity participating with several humans in an ordinary family thread.
- **Skydive** is the useful enterprise analogue. It supports several people sharing an agent and several specialist agents coordinating in workplace channels. Its public product contract is for teams and companies, not independently governed households.

The strongest conclusions are:

1. **“Family AI in a group text” is no longer empty positioning.** Ollie already markets and ships it. Skydive's official catalog also proposes parent jobs such as school-form completion, activity discovery, meal planning, family logistics, and emailing travel plans to a partner. Florence cannot differentiate merely by being available over iMessage, importing a flyer, or adding a spouse.
2. **Neither product publicly proves the hard household layer Florence is designed around.** There is no credible evidence of two independently verified adults, separately owned inbox/calendar scopes, private-to-household fact promotion, equal authority, conflict handling, adult removal, or a durable provenance trail for what became shared.
3. **The best public multiplayer failure is highly relevant.** A months-long Ollie user first praised its briefings and participation in work iMessage groups, then reported that Ollie repeatedly replied to everyone after being told to remain silent unless addressed. The same failure in a family thread would create more coordination friction, not less.
4. **The best public onboarding failure argues against literal text-only setup.** A launch user deleted Ollie after connecting several calendars and inboxes for himself and his wife required roughly 20–30 text exchanges; he explicitly asked for a web setup flow, and the founder acknowledged it. Ollie's current flow is now a compact web onboarding. The right Florence target is **text-led, not browser-free**: normal conversation and family questions in Messages, with a very small secure web handoff for disclosure, adult attestation, and OAuth.
5. **Recent launch attention is not the same as product love.** Much of Ollie's viral launch amplification is X-labeled paid partnership; much of Skydive's visible launch proof comes from founders, employees, makers, or its investor. The independent recent-use corpus is small.
6. **Skydive shows what not to copy architecturally.** Specialist handoffs, a visible shared channel, explicit identity, and scoped permissions are useful product lessons. An enterprise agent-workforce control plane, many independent memories, Slack orchestration, arbitrary agent building, terminals, and model selection are not Florence's parent wedge.
7. **Verified phone-call completion remains genuine whitespace.** Public Skydive and Ollie surfaces are messaging products; Skydive's underlying Linq transport does not itself originate or answer voice calls. Adjacent family organizer Nori claims in its App Store description that it can call a restaurant or plumber to make a booking, but no firsthand evidence of a completed call was found and its own feature-page example establishes only that a task became “scheduled.” Rebecca Kaden's second request—agents that can reliably make and finish calls—was not resolved by the evidence in any recursive branch.

The product conclusion is: **start every parent privately, prove value, and let the same Florence relationship expand into an independently consented household.** The differentiated multiplayer outcome is not “Florence nags your spouse.” It is “Florence neutrally owns the logistics, knows who is responsible, preserves each adult's private boundary, and closes the loop without turning the adults into each other's project managers.”

## Evidence discipline

This report uses three labels throughout:

- **Company claim** — official site, documentation, founder, employee, maker, investor, or vendor case study. Useful for the intended contract, not independent validation.
- **Firsthand report** — a person says they used the product and describes a behavior or failure. Commercial relationships are disclosed where found.
- **Inference** — a conclusion drawn from the public contract and reports. It is not represented as observed product behavior.

For X, the signed-in account was used to inspect the supplied posts, their quoted posts and replies, launch-week searches, author profiles, relationship disclosures, and adjacent products or infrastructure named in the threads. Search was deliberately biased toward concrete verbs and failure language, not engagement. Absence from X is not proof that a capability does not exist; it is evidence that no public proof was found in this bounded window.

## What “multiplayer” can mean

The conversation collapses four materially different products into one word:

| Model | What is shared | Public example | Parent-specific risk |
|---|---|---|---|
| Multi-human, one agent | Several humans talk to the same service identity and memory | Ollie family group; a shared Skydive agent | One person's private context can silently become everyone’s context |
| Multi-agent, one organization | Specialist agents have separate tools/memories and hand work to each other | Skydive agents in Slack | Complex control plane; workplace admin assumptions do not map to equal adults |
| Federated personal agents | Each person owns an agent that negotiates with other people’s agents | Rebecca Kaden's requested Instinct/Townie behavior | Requires identity, authority, disclosure, conflict, and audit semantics; no product here publicly proves it |
| Household-governed operator | One family operator serves private adult threads and one shared family layer with controlled promotion | Florence's target | Harder than a group chat, but directly addresses trust and relationship friction |

Scott Belsky sharpened Rebecca Kaden's request into agents handling reminders between partners so the humans do not have to nag one another ([Scott's post](https://x.com/scottbelsky/status/2094413908018594154); [Rebecca's original](https://x.com/rebeccakaden/status/2094411588950270409)). That is a real job, but “automated nagging” is the wrong product contract. The valuable behavior is neutral ownership, explicit responsibility, proportionate follow-up, and verified closure.

## Skydive: exact product and company

### Identity

**Company claim.** Skydive is a cloud agent platform launched August 20, 2026 by **Dhruv Amin and Marcus Lowe**. It is operated by **Create, Inc.**, the company behind the earlier Create app builder and its successor Anything. The founders' own history says they met at Google, built the company together, and raised an $11 million round in 2025 led by Footwork ([Skydive launch](https://www.skydive.com/blog/introducing-skydive/); [Anything launch history](https://www.anything.com/blog/anything-launch); [Anything financing/history](https://www.anything.com/blog/anything-series-a); [Skydive privacy policy](https://www.skydive.com/privacy)).

This relationship matters when weighing X evidence: Footwork investor Nikhil Basu Trivedi is a real user, but not an independent reviewer.

### Product contract

**Company claim.** A Skydive agent has its own identity, memory, cloud computer, browser, filesystem, channels, integrations, credentials, routines, and configurable permissions. The same agent can be reached on web, desktop, Slack, email, CLI, and iMessage. Multiple people can use a shared agent; multiple specialist agents can collaborate ([launch](https://www.skydive.com/blog/introducing-skydive/); [welcome](https://www.skydive.com/docs/get-started/welcome); [agent creation](https://www.skydive.com/docs/agents/creating-agents); [collaboration](https://www.skydive.com/features/collaboration)).

This is a build-your-own-agent platform, not a preconfigured family operator. The product language is overwhelmingly team/company/coworker language.

### Onboarding is web-first

**Company claim.** The documented quickstart is a five-minute browser flow:

1. Sign in at skydive.com with Google or Slack; Skydive creates a personal workspace.
2. Name an agent, write its goal, choose its character, optionally connect services, and select initial channels.
3. Send the first message in web chat.
4. Complete one-click OAuth when a task needs a service.
5. After the agent is useful, add Slack, email, text, or a schedule.

That sequence is explicit in the [official quickstart](https://www.skydive.com/docs/get-started/quickstart). iMessage is a downstream channel, not the setup surface. The August 29 [official X announcement](https://x.com/getskydive/status/2093724067845324845) said agents could “now” text, so the messaging channel is extremely new despite appearing in launch copy.

**Conclusion:** Skydive supplies no evidence that serious connected-agent onboarding should or can live entirely in ordinary texts. It supports the opposite pattern: provision and authorize securely in web, then move daily work into the channel the user already inhabits.

### Solo behavior

**Company claim.** A single person can create a personal workspace and private agents without inviting a teammate. The free plan permits up to two seats; paid Starter permits up to five ([pricing](https://www.skydive.com/docs/account/pricing)). Skydive is therefore inherently usable solo.

**Inference.** Skydive's solo behavior is technically broad but cognitively expensive for a parent: the user must decide what agents to create, define roles and boundaries, connect tools, and manage usage. “Can one person use it?” is yes. “Does it deliver a parent operator without configuration?” is no.

### Its two real multiplayer systems

#### 1. Several humans share one agent

**Company claim.** A workspace owner invites teammates by email or link. An invitee accepts, signs in, and creates an account. Teammates can then reach shared agents through their own web, Slack, email, or text identities ([teams documentation](https://www.skydive.com/docs/workspaces/teams); [chat anywhere](https://www.skydive.com/feature/chat-anywhere)).

The agent associates Slack, email, and phone identities with people, but **the memory belongs to the agent, not to each human**. Everyone who can message a shared agent contributes to and draws from the same memory. Skydive's own documentation advises keeping sensitive/private material out of a shared agent and using a separate private agent instead ([memory](https://www.skydive.com/docs/agents/memory); [permissions](https://www.skydive.com/docs/agents/permissions)). Workspace admins and owners can inspect and edit all agents.

That is ordinary organizational access control. It is not the same as two equal adults whose personal inbox and calendar meaning remains private until intentionally promoted into household truth.

#### 2. Several specialist agents coordinate

**Company claim.** Separate role agents have their own tools, memory, model, and persona. They collaborate mainly in shared Slack channels or through shared work records. An agent reads channel history, recognizes the human sender, and should respond only when addressed or triggered. Handoff conditions live in written persona instructions. Skydive distinguishes these independent role agents from subagents, which are isolated parallel copies of one agent sharing its identity and services ([agents working together](https://www.skydive.com/docs/workspaces/agent-teams); [subagents](https://www.skydive.com/docs/capabilities/subagents)).

The current [Slack product page](https://www.skydive.com/slack) is even more explicit: a shared agent acts on an `@mention` or DM and does not answer untagged channel chatter. That intended trigger discipline is exactly the property Ollie failed for one longitudinal user below. Skydive's [self-improvement page](https://www.skydive.com/features/self-improvement) simultaneously reiterates the limitation: memory is agent-level and shared among users, so private details belong in a separate agent rather than the shared one.

The most concrete official example is a customer-support front agent handing technical work to engineering or account specialists while retaining the customer-facing thread. The useful product principles are:

- one visible owner for the human relationship;
- specialists scoped by job and tools;
- explicit handoff triggers;
- shared records with a resolution state;
- a human able to review or redirect.

**Inference.** Florence can use those principles inside one durable household operator without exposing a “team of agents,” adding a new agent framework, or copying Skydive's workforce architecture.

### Skydive does explicitly imagine parent jobs

**Company claim.** Skydive's launch-day catalog of “231 ways” includes a personal section with an agent in a friends' group chat, meal planning, activities constrained by children's ages and nap schedules, filling school forms from prior PDFs, contractor quote collection, a family organizer that owns recurring logistics, and a trip planner that emails a partner. The post says these are uses the founder saw emerge across the company and 20 early companies, but it does not identify which personal examples were deployed, by whom, or with what outcome ([231 use cases](https://www.skydive.com/blog/231-agent-use-cases/)).

These examples prove that Skydive can be configured toward family jobs. They do **not** prove a functioning household authority model or sustained parent adoption. Signed-in X searches combining Skydive with family, parent, spouse, husband, wife, and kids found no firsthand household report in the two-week window.

### Recent firsthand X evidence

The usable recent corpus is small:

| Source | Relationship | What the source actually reports | Weight |
|---|---|---|---|
| [Nikhil Basu Trivedi's detailed Skydive article](https://x.com/nbt/status/2090557853744685428) and [Aug. 31 reply](https://x.com/nbt/status/2094439459869933797) | Footwork investor in Create/Anything | Built a writing agent and several reviewing agents in roughly a week; describes Slack debates, handoffs, specialist roles, a chief-of-staff agent with boundaries, and collaboration across his individual and team contexts | Detailed firsthand use, but commercially conflicted and workplace-only |
| [Taylor Desseyn](https://x.com/tdesseyn/status/2092039278855393512) and [his concrete follow-up](https://x.com/tdesseyn/status/2092048751078613209) | No company relationship found | Said Skydive and Instinct replaced another bot; Skydive filled two specific team gaps: project management and email copywriting; he contrasted that specialization with Instinct as a general EA | Strong independent evidence for focused workplace agents, not household use |
| [Ben Patton](https://x.com/benapatton/status/2092041043688518006) | No company relationship found | Liked Skydive but said it consumed tokens so quickly he became reluctant to use it again | Small but concrete independent cost failure |
| [Arnav Surve](https://x.com/1arnavsurve/status/2093139188652888225) | Skydive employee (“robot babysitter” in bio) | Connected/used agents with products such as Superhuman and Wispr Flow | Internal use, not independent validation |
| [Boondachshunds](https://x.com/boondachshunds/status/2093167711836905717) | Skydive/Anything employee | Connected a Skydive agent to ManaBox data and used it to recommend card-game decks from owned inventory | Concrete personal/internal use, not family coordination |
| Product Hunt maker comments on [the Skydive launch](https://www.producthunt.com/products/skydive) | Skydive makers/employees | Describe gym programming, Stripe-dispute automation, affiliate-payment discovery, and support agents | Company-operated examples |

Nikhil's report contains several useful operational details—bounded agents, human approval for most CRM writes, agents reviewing one another, and a visible chief-of-staff boundary—but it should not be presented as independent market validation. Taylor's account is the clearest independent positive signal and reinforces that Skydive's strength is **specific role coverage**, not a magical general household brain.

There was no credible public report of:

- two spouses each connecting personal accounts;
- private versus shared household facts;
- partner-specific authority or equal control;
- a shared family calendar;
- child/caregiver participation;
- spouse-to-spouse agent negotiation;
- a completed family task across multiple adults.

### Pricing and cost shape

**Company claim.** Skydive charges a subscription whose dollar value becomes an equal amount of usage credit: Free at $0 with up to two seats, Starter at $20/month with up to five, and Team at $200/month with unlimited seats. Models, compute, and connected services consume the balance. A dedicated iMessage line costs **$100 per month per agent**; shared-pool lines are free ([pricing](https://www.skydive.com/docs/account/pricing); [usage](https://www.skydive.com/docs/account/usage); [iMessage documentation](https://www.skydive.com/docs/channels/text)).

**Inference.** Variable token/compute accounting and per-agent dedicated-line fees are natural for a builder platform but poor parent-facing mental models. Ben Patton's complaint is exactly the failure mode: a useful product becomes emotionally unsafe to invoke because every message may carry opaque variable cost.

### Privacy and child/family fit

**Company claim.** The current [privacy policy](https://www.skydive.com/privacy) names Create, Inc. and covers transcripts, outputs, tool calls, memory, connected-account data, credentials, browser/sandbox data, files, and repositories. Model-improvement use is enabled by default with an organizational opt-out, subject to exclusions for certain connected-service data, secrets, and third-party restrictions. Administrators may access and manage workspace content. The service is for adults 18 and older and is not intended for children. The [security page](https://www.skydive.com/security) advertises single-tenant sandboxes, encrypted credentials, audit logs, and SOC 2 Type I.

Two documentation cautions matter:

- The privacy policy asks users not to submit sensitive data except where necessary and permitted; a family operator will inevitably encounter children's and household-sensitive information.
- The privacy policy text still describes SOC 2 work as being pursued while the security page claims Type I completion. This may be a stale-page issue, but it weakens confidence in the public trust contract.

**Inference.** Skydive's enterprise admin visibility and shared-agent memory are not a safe default model for a parental unit. A family cannot be treated as a small company with one omniscient administrator.

## Recursive infrastructure branch: Linq

Skydive credits **Linq** for iMessage group-chat delivery ([Mason Young's post](https://x.com/masony334/status/2093732505631285534)). Ollie's own vendor story also identifies Linq as its group-messaging infrastructure. That makes Linq useful evidence about what is transport-level versus product-level.

### What the transport really supports

**Company/vendor claim.** Linq provides iMessage, RCS, and SMS APIs with real group conversations, participant handles, reactions, rich media, and webhooks ([Linq](https://linqapp.com/); [iMessage API](https://linqapp.com/imessage-api); [key concepts](https://docs.linqapp.com/getting-started/key-concepts/); [group-chat guide](https://linqapp.com/blog/building-ai-agents-that-work-in-group-chats)). Group chats can contain up to 31 recipients, subject to fallback behavior.

Its open-source [AI group-chat example](https://github.com/linq-team/ai-agent-example) classifies each message into three behaviors: answer when directly invoked, react to positive feedback, or ignore human banter. That is implementation evidence that group etiquette is a distinct product problem rather than a free side effect of connecting a phone number.

**Inference for Florence.** Sender identity, the exact family thread, addressing, and “quiet unless useful” should be first-class behavior with focused verification. A vague prompt telling Florence not to interrupt is not enough; Ollie's real regression demonstrates that.

### Phone calls are not solved here

**Company/vendor claim.** Linq's [FAQ](https://docs.linqapp.com/guides/resources/faq/) says the API does not directly originate or answer calls. Inbound calls can be forwarded to a VoIP provider, and a separate provider such as Twilio can place outbound calls while presenting a Linq number. A Twilio employee replied in Scott Belsky's thread that Twilio is working on easier text/voice integrations ([reply](https://x.com/odower/status/2094449183902277986)).

**Conclusion.** A phone number and iMessage do not imply agent-run phone calls. Neither Skydive nor Ollie supplied public proof of booking, negotiating, waiting on hold, or closing a family job by voice during this research.

## Lennon resolves to Ollie

`@blennon_` is **Bill Lennon**, co-founder and CEO of **Ollie**. The other named co-founders are **Christy Shannon** and **Rushabh Doshi**; the legal operator is **Confabulation Corporation** ([about](https://ollie.ai/about/); [terms](https://ollie.ai/terms-of-service/)). The Scott Belsky reply naming Bill and `@heyollieai` therefore points to one product, not another adjacent company ([reply](https://x.com/bdistel/status/2094417512997818467)).

## Ollie: the direct parent competitor

### Product contract

**Company claim.** Ollie is a text-native family assistant reached through iMessage/SMS/RCS. It connects email and calendars; monitors school and household messages; produces morning/evening briefings; finds deadlines and conflicts; creates calendar events from flyers, photos, PDFs, or messages; persists reminders; manages shared lists; assists with meal/grocery planning; logs newborn/health information; and drafts emails ([home](https://ollie.ai/); [FAQ](https://ollie.ai/2026/03/10/family-assistant-faq/); [use cases](https://ollie.ai/explore/)).

The target explicitly includes single parents and households with two working parents ([who Ollie is for](https://ollie.ai/who-ollie-is-for/)). Public evidence is much stronger for **notice, summarize, remind, list, calendar, and draft** than for broad external execution. Bill Lennon has said Ollie does not make decisions for the user ([founder reply](https://x.com/blennon_/status/2062189005878239668)).

### Ollie's actual multiplayer model

**Company/vendor claim.** Ollie can begin in a private one-to-one text and later be added to a normal group with a spouse, nanny, home manager, caregiver, or children. One subscription covers the group. Its comparison pages describe household-level shared context, shared briefings, and a household memory graph ([home](https://ollie.ai/); [Ollie versus Poke](https://ollie.ai/vs/poke/); [pricing](https://ollie.ai/pricing/)).

Linq's vendor case study is unusually precise: Ollie is intended to be a real third participant in the same partner group, rather than a bot independently texting each parent ([Linq's Ollie case study](https://linqapp.com/blog/how-ollie-became-a-member-of-the-family-group-chat)).

That is **shared-thread multiplayer**. It is not:

- one agent owned by each spouse;
- agent-to-agent negotiation;
- proof that each adult is independently verified;
- proof of separate personal provider scopes;
- proof that private facts require promotion before entering shared memory;
- proof of adult removal, co-parent separation, or surprise/private planning;
- proof of per-person authority over external actions.

The [terms](https://ollie.ai/terms-of-service/) put the burden on the inviting user to represent that group participants and phone numbers are authorized, and warn that group messages can be seen by all participants. No public partner-verification handshake was found.

### Live onboarding, August 31

The current [Ollie onboarding](https://ollie.ai/onboarding/) was walked in-browser. Despite the landing copy “No app, no login, just a text,” the current path is a compact web flow:

1. choose the first job: calendar, email, reminders, or all;
2. choose a calendar provider;
3. choose an email provider;
4. optionally provide name and timezone;
5. choose writing tone;
6. select either a household group thread or private one-to-one use;
7. scan a QR code or text Ollie's shared phone number.

The household path instructs the user to add the same Ollie number to a group. It did not collect or verify the partner during the observed flow.

### Why the onboarding changed

**Firsthand report.** At launch, Car Dealership Guy said he deleted Ollie because connecting three or four calendars and email accounts across himself and his wife took around 20–30 text exchanges; he asked for setup on a website ([report](https://x.com/GuyDealership/status/2061973558083846383)). Bill Lennon acknowledged the feedback ([reply](https://x.com/blennon_/status/2062005167374123186)). Lennon also reported that X's embedded browser blocked the final onboarding step ([post](https://x.com/blennon_/status/2061930808794050572)).

**Conclusion.** “All setup in texts” is not automatically more conversational. Repeated provider selection, account linking, disclosure, and correction can become a tedious protocol disguised as chat. Florence should move Florence-owned questions out of the web wizard, but keep one tiny, durable, browser-compatible trust/OAuth surface.

### Firsthand use and the strongest multiplayer failure

| Source | What the source actually reports | Weight |
|---|---|---|
| [Christian / `@optionscjp`, July 6](https://x.com/optionscjp/status/2074308637317796347) | Months of unpaid use; connected email, calendars, and work iMessage groups; valued morning/evening briefs, group questions, and recaps | Strongest independent longitudinal positive report |
| [Same user, Aug. 19](https://x.com/optionscjp/status/2090207786089238773) | Ollie agreed to stay silent unless addressed but repeatedly answered messages directed at other people; founder asked to debug by DM | Strongest public multiplayer failure |
| [Cynthia Bell McGillis, June 11](https://x.com/cynthiamcgillis/status/2065184775745486987) and [Aug. 21](https://x.com/cynthiamcgillis/status/2090970074920386948) | Paying user; reports screenshot-to-calendar and personal email alerts working, and values time-sensitive inbox/calendar monitoring | Concrete independent repeated use |
| [Brooke Travis, Aug. 24](https://x.com/btravisNYC/status/2091936494889939283) | First 24 hours felt seamless; text and voice dictation fit juggling | Positive but no concrete completed task |
| [Brian Distelburger, Aug. 31](https://x.com/bdistel/status/2094417512997818467) | Calls Ollie the closest product he has seen to the requested family behavior | Referral/interest, not detailed use |
| [Pricing criticism](https://x.com/theaaron/status/2093084691297616180) | Objects to the price/value shape | Negative perception, not a capability test |

The longitudinal group-chat regression matters more than dozens of launch likes. Family threads have many messages not addressed to the assistant. An operator that interjects incorrectly creates embarrassment, noise, and new coordination labor. For parents, addressing discipline is a core correctness property.

### Launch virality was partly purchased

**Observed X metadata.** Ollie's June 2 [launch thread](https://x.com/blennon_/status/2061868938443550842) had roughly two million views and substantial replies, reposts, likes, and bookmarks when inspected. It demonstrated a vivid parent workload: many apps, school emails, group chats, calendars, overnight monitoring, meals, unpaid bills, and a shared partner brief.

However, several prominent positive amplifications were explicitly labeled **Paid partnership** by X ([Eli Weiss](https://x.com/eliweisss/status/2061885323571871931); [Jaymin Shah](https://x.com/JayminSOfficial/status/2061879706048508011); [Brett Dashevsky](https://x.com/BrettFromDJ/status/2062177684587683920); [EXM](https://x.com/EXM7777/status/2061893113770365102)). The demos can still describe the right parent jobs; their distribution should not be mistaken for independent satisfaction.

Organic objections focused on third-party data trust, Apple/local preference, and lock-in ([data-trust objection](https://x.com/DJJenkins/status/2061930703848374296); [local-control objection](https://x.com/jessegenet/status/2061904530372272415)). A secondary launch analysis argued that deliberately provocative “better parent/husband” framing helped drive attention ([analysis](https://x.com/RiverKhan/status/2063005585516159067)).

**Inference for Florence.** Sell relief and relationship protection, not parental inadequacy. “Florence owns the logistics so your family gets time back” is durable; “AI makes you a better parent than your spouse” is attention-hacking that raises the trust burden.

### Pricing

**Company claim.** Ollie currently offers Free with 50 monthly replies and a daily cap of 6, Everyday at $25/month with 150 and 10/day, and Always-On at $100/month with 1,000 and 15/day. A single subscription covers the family group; users can buy top-ups ([pricing](https://ollie.ai/pricing/)).

**Inference.** Charging per Ollie reply can discourage exactly the behavior a proactive group assistant needs: clarifying, acknowledging, following up, and coordinating with several people. A family should not wonder whether a useful reminder consumed scarce “replies.”

### Privacy and trust-contract inconsistencies

**Company claim.** Ollie's August 21 [privacy policy](https://ollie.ai/privacy-policy/) covers messages, media, group participation, email subject/sender/content, and calendar data. It names model and infrastructure subprocessors including Anthropic, OpenAI, Google, Groq, Composio, Linq, Raindrop, and commerce providers; it says data is not used for third-party model training. It permits service-improvement/human review and deidentified use. Messages/media may remain for up to 24 months, with different active, log, deletion, and backup periods for connected data. Ollie announced SOC 2 compliance on August 21 ([announcement](https://x.com/heyollieai/status/2090853850135801900); [trust center](https://trust.ollie.ai/)).

The public explanation is not fully coherent:

- Bill Lennon said Ollie does not index or store users' emails ([Aug. 22 post](https://x.com/blennon_/status/2091270163475919288)), while the FAQ and privacy policy describe storing some email-derived content and account data. The charitable interpretation is “no durable full-mail index,” but the public wording does not establish that precisely.
- The live setup says email reads only what the user asks about, while the product pages and briefings promise proactive inbox monitoring.
- The homepage invites adding children, while the terms say children may not independently use the service.

For a parent product, these are not copy nits. The user must be able to explain what is read, what is retained, what becomes household-visible, and what deletion does.

### Internal architecture claims

**Company claim.** Ollie's launch material describes an “army” of agents scanning, triaging, and planning. Its FAQ mentions OAuth, signed webhooks, and standard authentication controls. Linq is the confirmed messaging transport; Composio appears in the privacy subprocessor list and is likely part of connector execution.

**Inference.** There is no public source code, trace, eval, or architecture evidence verifying a multi-agent runtime. The externally meaningful product is one Ollie identity, one shared household context, and a set of connected jobs. Florence should compete on that lived behavior, not on the number of internal agents.

## Light adjacent watchlist from the Ollie branch

These are not broad text agents, but they demonstrate that family-specific calendar ingestion and multi-account UX are already a category.

### Domistiq

**Company claim.** [Domistiq](https://domistiq.com/) is an app-first US family organizer with multiple household accounts and profiles, Google/Apple/Microsoft calendar sync, US school data, email/photo/voice ingestion, reminders, and gamified chores. Its founder frames it explicitly as a dad-built response to family coordination overload ([founder rationale](https://domistiq.com/blog/2026/06/02/as-a-dad-why-did-i-build-domistiq)). Current early-bird pricing is $2.99/month or $19.99/year after a seven-day trial ([pricing](https://domistiq.com/pricing)).

Its [privacy policy](https://domistiq.com/privacy) covers parent-managed child profiles and within-family sharing, but no private-per-adult scope was documented. There is a public trust-copy mismatch: the homepage promises that family data is never shared and is instantly deleted, while the policy permits subprocessors and describes 30–90 day deletion periods and some 30-day photo retention.

**Evidence quality.** The [iOS listing](https://apps.apple.com/us/app/domistiq-family-organizer/id6755165065) has too few ratings to establish adoption, and no credible recent X/Reddit use was found. Even Domistiq's own comparisons acknowledge thin early feedback. Treat it as product-surface evidence, not validation.

### Domi Home

**Company claim.** [Domi Home](https://www.domi-home.co.uk/) is a UK app/web family hub. Onboarding creates a family and members; a partner receives a separate login. Google Calendar import is read-only. A parent can photograph a school letter, review the extracted dates, and approve them into the shared calendar; the product also includes tasks, shopping, meals, and a briefing ([guide](https://domi-home.co.uk/guide)). Pricing is free for the core product or £4.99/month/£39.99/year for Plus after a trial.

All family data appears shared; no private-parent source boundary was documented. Its [privacy policy](https://domi-home.co.uk/privacy-policy) names Anthropic, says scan images are not permanently stored, describes logs generally retained no more than 30 days, and leaves backup removal less concrete.

**Firsthand report.** The [UK App Store listing](https://apps.apple.com/gb/app/domi-home-family-organiser/id6763921282) had only four ratings when checked. June reviewers specifically mentioned two-parent/multi-device sharing and useful date extraction, which is directionally useful but far too small a sample. Its August 17 changelog also records a fix for an undismissable trial-expiry screen.

### FamilyIQ

**Company claim.** [FamilyIQ](https://familyiq.co.uk/) is a UK web app with multiple adult accounts, light child profiles, and Full/School/ReadOnly carer roles. It ingests forwarded email, photos, PDFs, and notes into child-aware calendars, tasks, and reminders. Its contract is still parent-omniscient: a parent sees everything, not an independently private adult scope. Pricing is £49/year or £4.90/month for a household after a seven-day trial.

Public copy is internally ambiguous: one surface says extracted items wait for approval, while its FAQ says there is no approval queue and the service acts automatically unless judgment is required. Its [privacy policy](https://familyiq.co.uk/privacy) does not name processors or give a concrete deletion window. No independent public use evidence was found.

### Nori: the action-receipt warning

**Company claim.** The distinct family product [Nori](https://www.heynori.com/) (Domus Next, not unrelated products with the same name) combines a Family Hub device with app/web access. It claims voice/photo/email capture and reminders delivered by call, text, or email. Its [iOS listing](https://apps.apple.com/us/app/nori-family-ai/id6753757891) also claims Nori can call a restaurant or plumber to make a booking. However, its [feature page](https://heynori.com/appfeatures) depicts “book a table” becoming “scheduled” without establishing that Nori actually placed or completed a call, and no firsthand call-completion report was found. The company homepage claims use by more than 200,000 families, while its [company history](https://shop.smart-domi.app/about-us) says more than 100,000 families were in private beta. The iOS listing showed roughly 1,200 ratings and a 4.8 average, making it the strongest adjacent adoption signal in this watchlist without treating company reach claims as audited counts.

**Firsthand report.** A July 8 iOS reviewer reported two concrete integrity failures: Nori said it deleted grocery items when it had not, and a doubled meal-plan request still produced quantities for only one week. One review cannot establish a rate, but it demonstrates the precise failure Florence must prevent: confident completion language without a provider-state receipt, and plausible-looking arithmetic without outcome verification.

**Conclusion.** The category is converging on **messy family input → structured commitment → shared visibility → reminder**. Calendar ingestion, school-email parsing, reminders, and shared lists are table stakes. Florence's moat must be cross-source investigation, independently private parents, consented household truth, broad action, and verified closure in normal texts—not another dashboard or the widest meal/chore checklist.

## What Florence should match, and what it should beat

### Match quickly because parents will expect it

- A private, immediately useful solo relationship.
- School-email and calendar monitoring.
- Flyer/photo/PDF/message to a confirmed calendar event.
- Persistent reminders to the right person.
- A compact daily family brief.
- Shared lists and ordinary group-text participation.
- Voice-dictated requests.
- One subscription/household mental model rather than per-agent infrastructure pricing.

These behaviors are visible and legible. They create the first-week “I would miss this if it disappeared” feeling.

### Beat Ollie and Instinct on the household layer

1. **Independently verify each adult.** An inviter cannot unilaterally authorize another person's phone, inbox, calendar, or private messages.
2. **Keep personal sources personal.** One parent's Gmail or private calendar fact does not become household truth merely because the system inferred it.
3. **Make promotion explicit and inspectable.** Florence can propose a useful family fact to its owner, then write it to the family calendar or shared memory only after intentional approval.
4. **Give both adults equal household authority.** No enterprise-style owner/admin who can silently inspect all personal agents.
5. **Know who is speaking and who is responsible.** Identity, addressee, assignee, and authority are separate facts.
6. **Be quiet with discipline.** In the family group, Florence responds when addressed, when a known obligation requires action, or when a genuinely material conflict arises—not to every line of human conversation.
7. **Resolve logistics neutrally.** Ask for or infer an owner, follow up proportionately, offer alternatives, and escalate ambiguity without casting one partner as the other's manager.
8. **Finish real work.** Research across public pages/PDFs, act through providers, survive multiple turns, and report success only after the provider confirms it.
9. **Make memory governable in parent language.** Show what Florence knows, where it came from, who can see it, and let either authorized adult correct or delete shared truth.
10. **Handle household change.** Invitation cancellation, wrong-recipient recovery, adult removal, co-parent transitions, calendar ACL cleanup, and deletion/reset must be designed before “add someone later” is called safe.

### Do not copy the wrong parts of Skydive

- Do not make parents design a roster of specialist agents.
- Do not expose model selection, token budgets, cloud computers, or per-agent phone-line pricing.
- Do not use one broad admin role as a substitute for household consent.
- Do not create separate workflow engines or memories for every family feature.
- Do not market internal agent count as the outcome.

The transferable Skydive ideas are narrow: one human-facing owner, bounded hidden specialists where genuinely useful, explicit handoff conditions, shared resolution records, and progress/approval visibility.

## Recommended onboarding flow

The ideal competitive response is **individual-first, household-expandable**. “Solo” is the current composition of a Florence relationship, not a separate product or permanent mode.

1. **A normal text is the front door.** Florence responds to what the parent actually wrote and begins any harmless available work. She briefly says they can start privately and add another parent or caregiver later.
2. **Collect only conversational minimums in text.** Learn the parent's preferred name and enough guardian/caregiver context to proceed. Do not front-load every child's age, grade, school, activity, partner, and ZIP.
3. **Use one micro-web trust boundary.** The page contains guardian/proactive-use attestation, a plain-language explanation of the initial Gmail/calendar review and retention, the exact connected account, Google OAuth, and a return-to-Messages confirmation. Provider authorization cannot responsibly be replaced with an improvised text protocol.
4. **Activate with a result.** Back in the private thread, Florence confirms the connection immediately, reviews the account, and surfaces one or two specific safe opportunities—ideally completing or preparing one real family chore.
5. **Offer household expansion after value.** Ask whether the user wants to remain private for now or invite another parent/caregiver. “Not now” completes onboarding and should not degrade the product or cause repeated prompting.
6. **Invite and verify the second adult independently.** Collect name/number in text, show the masked destination, obtain explicit send approval, and reveal no household detail in the invitation. The second adult receives their own trust/OAuth handoff.
7. **Promote, do not silently widen.** Before activation, show the first adult a compact profile of pre-existing facts/events that would become shared. Derived pre-join source context remains owner-private unless approved.
8. **Create the household activation moment.** Share the one Florence family calendar, create the exact family group, introduce Florence briefly, and complete or coordinate one useful joint job—not a generic welcome.

This is less complicated for users than either a permanent “solo/family mode” fork or an all-text authorization interview. Internally it preserves one household truth and one due-work path while allowing capabilities to turn on as independently verified adults join.

## Product priority implied by the evidence

The order should be:

1. **Participation discipline and identity** in the exact family group.
2. **Private solo activation** that produces a useful first result.
3. **Independent second-adult invitation, consent, and provider connection.**
4. **Private-to-shared provenance and equal authority.**
5. **Neutral responsibility, follow-up, and verified closure.**
6. **Broader action surface**, including local services and eventually phone calls.

Building agent-to-agent federation before those foundations would optimize the impressive demo while leaving the parent trust problem unsolved. If Florence later interoperates with a spouse's separate agent, the same household identity, authority, disclosure, and provenance layer will be the prerequisite.

## Evidence gaps to keep open

- Skydive launched only eleven days before this report; independent longitudinal evidence is not yet possible.
- No Skydive household deployment was found. The official family use cases may be real experiments, concept prompts, or a mixture; the post does not let us separate them.
- Ollie has no public technical proof for independent partner verification, per-adult provider ownership, shared-memory provenance, or agent architecture.
- Neither product publishes a convincing adult-removal/co-parent transition flow.
- Neither Ollie nor Skydive publicly demonstrates autonomous phone-call completion; Nori claims it, but no firsthand completion evidence was found.
- X is a biased sample, and sponsored labels plus founder/investor amplification materially contaminate raw engagement as a quality measure.

The highest-value follow-up research is not more generic praise. It is longitudinal parent interviews and two-phone testing of: wrong addressee, private surprise, conflicting calendars, an unresponsive assignee, a declined partner invite, adult removal, a school deadline discovered in only one inbox, and a task whose final step requires a phone call.
