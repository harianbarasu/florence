# Consumer onboarding patterns for Florence

Research current as of August 8, 2026.

## Executive decision

Florence should replace the current jump into **Family & people** or **Sources & routines** with one dedicated, responsive onboarding flow.

The first private link from iMessage should always resolve to the next unfinished onboarding step. Until the minimum setup is complete, the web app should show no sidebar, dashboard, group controls, source-management tables, or privacy-management navigation. After completion, Florence should unlock the existing control plane and take the person to **Today**.

The flow should feel conversational, but the right unit is **one decision per screen**, not mechanically one database field per screen. Typeform argues that one-question-at-a-time forms are easier to complete on mobile, while Android's current guidance warns against overdoing a separate screen for every individual input. The synthesis is to keep each screen about one coherent topic and group only tightly related inputs. [Typeform form guidance](https://www.typeform.com/guides/the-ultimate-guide-for-an-online-form-builder), [Android authentication and onboarding guidance](https://developer.android.com/design/ui/mobile/guides/patterns/onboarding)

The minimum Florence sequence should be:

1. confirm the person Florence is speaking with;
2. learn who else helps run the family;
3. learn the children and the minimum identifiers Florence needs to recognize them;
4. learn each child's school and activities, allowing explicit “not applicable” or “add later” answers;
5. offer a personal Google connection with a concrete explanation of what Gmail and Calendar unlock;
6. review the family setup and launch Florence.

Every answer should save immediately to app-owned canonical state. Returning from the same phone, another browser, an OAuth callback, or a new iMessage link should resume at the exact next unfinished step. Invited adults should get a shorter, relationship-specific path and should never be asked to re-enter household facts another member already supplied.

## Evidence standard

This note uses only first-party product materials, official help centers, and platform design guidance. These sources show what the product or platform intentionally supports; they are not independent proof of conversion or retention performance. Product recommendations below are explicitly marked as Florence inferences.

## Reference patterns

| Reference | First-party mechanism | Transferable mechanic for Florence | Do not copy |
| --- | --- | --- | --- |
| Android onboarding guidance | Separate what is truly required up front from what can be learned in context; organize required work into logical steps; collect only essential information; show progress; let long flows save and resume; avoid long scrolling forms. [Android Developers](https://developer.android.com/design/ui/mobile/guides/patterns/onboarding) | A gated flow is defensible because family identity and context are prerequisites to safe coordination. Show a real step indicator and save continuously. | Do not turn every field into a page or put a product tour before useful setup. |
| Apple Human Interface Guidelines | Keep prerequisite onboarding brief and interactive, postpone nonessential customization, ask for private-data permission when its benefit is understandable, and restore the previous state when an experience restarts. [Onboarding](https://developer.apple.com/design/human-interface-guidelines/onboarding), [Launching](https://developer.apple.com/design/human-interface-guidelines/launching/) | Ask people to build their real family model rather than read slides. Resume exactly where they stopped. Put later preferences in the unlocked control plane. | Do not teach Florence's agent architecture, privacy model, or every future feature during onboarding. |
| Typeform | Conversational questions, visible progress, prefilled known values, answer recall, and automatic save/resume reduce the burden of a long intake. Typeform stores unfinished browser answers for 15 days and can reuse prior answers in later copy. [Form guidance](https://www.typeform.com/guides/the-ultimate-guide-for-an-online-form-builder), [autosave](https://help.typeform.com/hc/en-us/articles/13064754462228-Enable-or-disable-autosave-progress-for-your-respondents), [answer recall](https://help.typeform.com/hc/en-us/articles/360052320011-Use-Recall-information-to-reference-form-answers-variables-and-more) | Prefill Hari's name from iMessage, address him by name, adapt later screens to prior answers, and make progress obvious. | Browser-local storage is insufficient for Florence. Sensitive family progress should be stored server-side under the verified person and household, not stranded in one browser. |
| Monarch | Its setup begins with the source that creates the product's value, then exposes a checklist for the remaining basics. It recommends connecting the most important accounts first and adding the long tail later. Household members have their own logins and receive invitations and acceptance confirmations. [Getting started](https://help.monarch.com/hc/en-us/articles/360048393272-Getting-Started-with-Monarch), [household members](https://help.monarch.com/hc/en-us/articles/360048393452-Add-Members-to-an-Existing-Account) | Ask only for sources Florence can immediately use and show a clear connected/syncing state. Treat every adult as an individual identity, even inside one family. | Monarch gives household members the same visibility. Florence must retain person-owned privacy and explicit bridges instead. |
| 1Password Families | The first person becomes an organizer; invitees create their own credentials; acceptance and organizer confirmation are separate states; private and shared information remain distinct; invitations expire and can be resent. [Getting started with Families](https://support.1password.com/explore/get-started-families/), [adding family members](https://support.1password.com/add-remove-family-members/) | Model a spouse or caregiver as **proposed → invited → identity confirmed → active**, and show that status plainly. Collecting someone's name does not activate or authorize them. | Do not make the founding parent a permanent superuser over another adult's private data. |
| YNAB Together | The manager enters their own name before inviting another person; invitees receive their own login; the inviter supplies a name and email plus an explicit sharing choice; invitations expire after 14 days and receive one reminder three days before expiration. Shared-plan onboarding progress syncs so another member sees completed steps rather than repeating them. [YNAB Together guide](https://support.ynab.com/en_us/ynab-together-B1nS78Cki) | Household facts should be completed once and reused. Person-private steps, such as connecting Gmail, remain individual. Invitations need status, expiry, and a bounded private reminder. | YNAB's group manager can access every member-owned plan; Florence's privacy model must not inherit that asymmetry. |
| Apple Family Sharing | Each family member uses their own Apple Account; an organizer sends an invitation; the recipient accepts it; invitation status is visible and can be resent; personal data is not shared merely because someone joins the family group. [Apple Family Sharing setup](https://support.apple.com/en-ie/108380), [adding a member](https://support.apple.com/en-gb/guide/iphone/iph8f958ab3f/26/ios/26) | Invitations should travel through a channel the recipient recognizes, and the recipient must claim their own identity. Florence can show the starter that an invitation is pending without exposing the recipient's private setup. | Do not equate family membership with universal data visibility or chat-write authority. |
| Google OAuth | Request the smallest scopes needed, ask in the context of the feature they unlock, handle partial or denied grants, and do not repeatedly prompt until the person again expresses intent. Google also requires web OAuth in a full browser rather than an embedded mobile webview. [Google OAuth best practices](https://developers.google.com/identity/protocols/oauth2/resources/best-practices) | Put Google near the end of onboarding, after Florence knows the family entities it will use to filter mail and calendar. Explain the payoff immediately before OAuth and return to the same step afterward. | Do not make a broad OAuth consent screen the first family experience, and do not trap someone in onboarding if they explicitly choose limited mode. |

## Recommended Florence flow

### Surface and interaction model

Use the same flow on mobile web and desktop web:

- a distraction-free page with no application navigation;
- Florence identity, a short human explanation, **Back**, and a real progress indicator;
- one topic per screen, with a narrow reading width on desktop and large tap targets on mobile;
- inline validation and immediate save after each Continue action;
- a generated handoff URL that resumes the verified person at the canonical next step;
- no raw technical terms such as “household epoch,” “source visibility,” “scopes,” or “private review.”

The flow should say what Florence will do with each answer. It should not make the person infer why school, activities, or Google access matter.

### Step 1 — You

**Prompt:** “First, a little about you.”

Prefill everything Florence already knows from iMessage and ask the person to confirm or edit:

- preferred name;
- relationship to the child or children: parent, guardian, or another caregiver;
- local time zone, shown as a confirmation rather than a technical input.

Do not ask for a phone number Florence already verified. Do not ask for biography, preferences, or a generic “tell us about yourself.”

### Step 2 — Who helps run the family

Avoid assuming every parent has a spouse. Ask:

> “Who else helps coordinate your family?”

Offer natural choices such as partner or co-parent, another caregiver, and “just me for now.” If the person adds someone, collect:

- name;
- relationship;
- mobile number or the exact contact mechanism needed for a private Florence invitation.

Then explain: “I won't share family information with Kendall until she confirms this is her.” Let the starter send the invitation now or after the final review. A pending invite is enough for the starter to finish onboarding; completion must not depend on another person's response.

### Step 3 — Children

Use a repeatable child card rather than one wide table.

For each child, collect only:

- name;
- nicknames or aliases, if any;
- birth year or age band;
- grade when applicable.

After saving one child, ask “Add another child?” This follows the same conditional repeated-field pattern Typeform recommends instead of showing an arbitrary number of empty rows. [Typeform repeated fields](https://help.typeform.com/hc/en-us/articles/360029670391-Use-repeated-fields-in-a-form)

Exact birth dates are not necessary for current coverage-loop matching and should not be collected by default.

### Step 4 — School and activities

Give each child a short, personalized substep:

> “What should I recognize for Violet?”

Collect:

- school, preschool, or daycare;
- grade, class, or teacher when useful;
- repeatable activities such as soccer, dance, camp, or tutoring;
- optional team, provider, or location names that help Florence match incoming information.

“Not applicable,” “I don't know yet,” and “Add later” are valid explicit answers. Completion means the question has been considered, not that every optional field contains data.

### Step 5 — Personal Google

Only now explain the connection in terms of the family Florence just learned:

> “Connect your personal Google account so I can privately find school, activity, and calendar updates for Violet and Theo. Nothing moves into the family unless your sharing rules allow it.”

Offer:

- **Connect Gmail & Calendar**;
- **Not now — use Florence without Google**.

If Google is already connected, show the account and its current state, then continue automatically. A background historical scan must not block completion. Work Google and additional accounts belong to later contextual setup unless the person explicitly says family logistics arrive there.

### Step 6 — Review and launch

Show a plain-language summary:

- the person Florence knows;
- the proposed or active co-parent/caregiver and invitation state;
- each child, school, and activities;
- personal Google connected, syncing, or skipped.

The primary action should be **Start using Florence**. Completing it should:

1. record the review against the current canonical setup state;
2. unlock the web control plane;
3. redirect to Today;
4. send a short private iMessage confirmation explaining what Florence is doing next.

The final screen should produce progress, not a feature tour. If Google is connected, Florence can say it is reviewing relevant recent mail and calendars. If Google is skipped, Florence can suggest one useful chat-native behavior.

## Completion and gating

### Derive completion from canonical setup state

Do not rely on a frontend-only `onboardingComplete` switch. The server should derive the person's current step from authoritative records, with an explicit final review fence so later edits do not accidentally reopen onboarding.

A household starter is complete when:

- their identity and relationship are confirmed;
- household composition has been explicitly answered, including “just me”;
- at least one child has the minimum identity fields required for the parent path;
- school and activities have each been answered for every current child, even if the answer is not applicable or deferred;
- the person has connected personal Google or explicitly chosen limited mode;
- the final summary has been confirmed against the current setup version.

The following must **not** block completion:

- a spouse or caregiver accepting their invitation;
- Google finishing its background synchronization;
- work email or additional calendar connections;
- routines, group classification, memory review, or advanced privacy customization.

### Branch by relationship

There is not one universal onboarding checklist.

- **Household starter:** completes the full family-context flow above.
- **Invited co-parent or partner:** confirms identity and relationship, reviews already-known household facts, fills only genuine gaps or conflicts, chooses their own Google connection, and finishes. They do not re-enter the children.
- **Invited grandparent, babysitter, or caregiver:** confirms identity, relationship, and their own participation preferences. They are not asked to define the entire household.

YNAB's shared onboarding is a useful precedent: work completed in the shared plan appears completed to other members, while each person retains their own account. [YNAB Together guide](https://support.ynab.com/en_us/ynab-together-B1nS78Cki)

### Web-app gate

Before completion:

- `/`, `/today`, `/people`, `/groups`, `/sources`, and `/privacy` should resolve to the canonical next onboarding step;
- no sidebar or bottom navigation should render;
- OAuth callbacks should return to onboarding;
- expired handoffs should offer a fresh private Florence link without losing saved progress.

After completion, the same routes become available. The finished onboarding remains editable later through a simple **Family setup** entry in the control plane.

This gate is a product-state gate, not an authorization shortcut. Completing onboarding does not activate an invited adult, promote private source content, or grant Florence permission to write in a group.

## Save, resume, and reconciliation

Typeform establishes the useful expectation that someone can leave and return without repeating answers, but its browser-local implementation is deliberately limited to the same browser and device. [Typeform autosave](https://help.typeform.com/hc/en-us/articles/13064754462228-Enable-or-disable-autosave-progress-for-your-respondents) Florence should go further:

- commit each accepted step through the authoritative application ingress;
- associate progress with the verified global person and relationship-local household;
- regenerate a short-lived handoff URL while keeping progress durable;
- return from OAuth to the same logical step;
- skip questions already answered by canonical state;
- surface true conflicts for confirmation rather than silently overwriting them;
- treat nicknames such as John, Johnny, and Jonathan as aliases to reconcile, not immediate conflicts.

No model conversation transcript or browser draft should be the onboarding system of record.

## Re-engagement by iMessage

Florence should privately follow up when onboarding is unfinished. The reminder should prove that progress was saved and offer a fresh link to the exact step:

> “I saved your setup after adding Violet. When you have two minutes, we can add her school and activities and then I’ll be ready to help: [Continue setup]”

Recommended initial policy:

1. send the onboarding link in the signup conversation;
2. if the person started but did not finish, send one private reminder at an appropriate local time the next day;
3. send one final private reminder roughly three days later;
4. after that, mention the unfinished setup only when the person next asks Florence for family help, unless they explicitly ask to resume;
5. immediately stop proactive reminders when the person says “not now,” “stop asking,” or equivalent.

This is intentionally bounded. YNAB's expiring invite includes one timed reminder, while Apple and 1Password expose a visible resend action rather than repeatedly contacting the recipient. [YNAB Together](https://support.ynab.com/en_us/ynab-together-B1nS78Cki), [Apple Family Sharing](https://support.apple.com/en-ie/108380), [1Password family invitations](https://support.1password.com/add-remove-family-members/)

Onboarding reminders must only go to the person's private Florence DM. They must never appear in an observe-only group or imply blame about another adult's incomplete setup.

## What Florence should avoid

- Do not send a new person directly to the existing dense People or Sources management pages.
- Do not expose the application's main navigation before the minimum setup is done.
- Do not lead with Google OAuth before Florence knows which family entities make that access useful.
- Do not ask a spouse to repeat the children, schools, or activities the starter already entered.
- Do not treat adding a spouse's name as consent, identity confirmation, household activation, or group-write authority.
- Do not wait for an invitee or a background sync before letting the starter finish.
- Do not make optional data look required; require an explicit decision instead.
- Do not ask for exact birth dates or other child data that the current product does not need.
- Do not repeat generic “finish setup” texts. Mention the saved step and the concrete value remaining.
- Do not block ordinary private conversation with Florence. The web control plane can be gated while Florence still answers general questions; family coordination and source-dependent promises remain bounded by the context and authority actually established.

## Product acceptance criteria

The onboarding redesign is successful when a fresh parent can:

1. text Florence naturally;
2. open one private link on a phone;
3. understand the value before entering data;
4. complete a responsive, non-scrolling sequence about themselves, their co-parent or caregivers, and their children;
5. leave midway and resume from a fresh iMessage link on another device without re-entry;
6. connect or explicitly defer Google and return to the same sequence;
7. finish without waiting for another adult or a background scan;
8. land in the unlocked web app with the data already represented correctly;
9. have the invited co-parent see existing household facts rather than repeat them; and
10. receive at most bounded, private, context-aware completion reminders.
