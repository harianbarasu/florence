# Parent app and activity coverage audit

_Research date: August 29, 2026_

## Decision

The existing Florence product direction is already substantially right. Parents do not need Florence to reproduce a school portal, sports app, meal planner, medical portal, invitation service, travel organizer, or home-services marketplace. They need one general agent that can understand the objective those systems collectively imply, do the next useful work, and remain responsible until the household knows the result.

This review therefore does **not** recommend a connector catalog, vertical routers, or another family dashboard. It adds three cross-domain requirements that are under-specified in `PLAN.md` and the existing research:

1. **Know what Florence has and has not actually observed.** A missing notification, empty search, stale calendar copy, or absent digest is not proof that nothing happened.
2. **Resolve the person or account that can perform the next action.** Shared family access does not imply equal permissions, and the right actor may be a particular parent, provider, organizer, purchaser, or administrator.
3. **Carry the objective through pending external states and downstream family consequences.** “Sent,” “submitted,” “scheduled,” and “reminded” are intermediate states; Florence should re-check, replan what the change affects, and close only on the real outcome.

These are product behaviors for the same broad agent, not new infrastructure projects.

## Evidence boundary

- **Product facts** below come from official vendor help or product pages. They establish what the vendor says the product does and where its boundaries lie; they do not establish user satisfaction or population-level adoption.
- **Parent reports** are first-person original forum posts. They are qualitative examples, not prevalence estimates.
- This document audits and sharpens the existing [`PLAN.md`](../../PLAN.md), [`parent-jobs-and-app-friction-2026-08-29.md`](./parent-jobs-and-app-friction-2026-08-29.md), and [`parents-source-to-outcome-opportunities-2026-08-29.md`](./parents-source-to-outcome-opportunities-2026-08-29.md). It intentionally does not repeat their full workflow catalog.

## Product facts: the same objective crosses many partial systems

| Recurring parent activity | What the official source shows | General Florence implication |
| --- | --- | --- |
| **School notices, forms, payments, and signups** | ParentSquare can combine messages, permission forms, payments, RSVPs, events, and appointment signups; its payment surface tracks whether a student/program payment is outstanding. MySchoolBucks separately exposes balances, invoices, forms, school purchases, low-balance notices, receipts, and AutoPay. A payment may take 24 hours or one to two business days to appear, and refunds or incorrect charges must be handled by the school or district. ([ParentSquare engagement](https://www.parentsquare.com/platform/parent-and-community-engagement/), [ParentSquare Pay](https://www.parentsquare.com/platform/payments/), [MySchoolBucks FAQ](https://www.myschoolbucks.com/ver2/help/gethelp.action)) | A school email is often the beginning of an obligation, not the result. Florence should join duplicate notices and graphics to the correct child, recover the form/fee/decision, use the authoritative source, and distinguish payment requested, submitted, posted, disputed, and refunded. |
| **Childcare and daily exceptions** | Brightwheel daily reports depend on account access, visible feed entries, an email address, and a recorded checkout; no report is sent if checkout was not recorded. Billing disputes, refunds, payer changes, and mandatory AutoPay can require the childcare program or its administrator rather than Brightwheel support. ([daily reports](https://help.mybrightwheel.com/en/articles/5243986-subscribe-for-daily-updates-on-your-child-s-activities), [billing responsibility](https://help.mybrightwheel.com/en/articles/4010032-who-should-families-contact-with-billing-questions)) | “No daily report” is not reliable negative evidence. Florence should know whether the source was complete and fresh, surface meaningful exceptions rather than the routine feed, and continue through the person who can resolve the issue. |
| **Sports, activities, transport, and volunteering** | TeamSnap schedules may be subscribed into personal calendars, but Google can take up to 24 hours to populate them and other personal calendars may take up to 24 hours to reflect later changes. Team assignments separately encode who owns a volunteer job; members without manager access can assign only themselves. ([calendar subscriptions](https://helpme.teamsnap.com/article/1245-subscribe-to-a-team-schedule), [assignments](https://helpme.teamsnap.com/article/387-create-team-assignments)) | A copied calendar is not necessarily the live source. Florence should re-open the current schedule, reconcile venue/time changes with both adults' availability, travel, equipment, carpools, siblings, and volunteer duties, then update the practical plan once. |
| **Health, records, appointments, and follow-up** | MyChart can expose children's appointments, medications, results, immunizations, growth charts, and school/daycare forms through family access. Each healthcare organization controls proxy permissions, what a proxy can do, and how access changes with a child's age; some require direct contact or a paper form. ([family access](https://www.mychart.org/l/en-us/features/family/), [proxy access](https://www.mychart.org/l/en-us/help/proxy/)) | Florence should retain the care objective across portal, phone, forms, visit, medication, referral, lab, and follow-up. If the connected adult cannot perform a step, Florence should identify the capable adult or provider path instead of concluding that the task is impossible. |
| **Meals, recipes, groceries, and delivery** | AnyList connects reusable recipes, family notes, meal-plan dates, household sharing, and grocery-list generation; moving a meal can also move the dates on its linked ingredients. Instacart lets family members collaborate on items, replacements, and shopper messages, but only the purchaser can cancel, reschedule, change the address, or adjust the tip. ([AnyList meal planning](https://www.anylist.com/meal-planning), [recipe and household workflow](https://help.anylist.com/articles/getting-started/), [linked ingredient dates](https://help.anylist.com/articles/release-notes-anylist-feb-2020/), [Instacart family collaboration](https://www.instacart.com/help/section/507104353/2469878137)) | The useful object is not a recipe or list in isolation. Florence should use durable family recipes and feedback to plan against the real week, derive the cart, respond to substitutions, and route purchaser-only actions to the right adult. |
| **Travel and disruption** | TripIt turns confirmation email into an itinerary and can surface schedule changes, check-in, gates, delays, cancellations, weather, and alternate flights at different points before and during travel. Choosing an alternative still requires contacting the airline or travel vendor. ([flight alerts](https://help.tripit.com/en/support/solutions/articles/103000063296-flight-alerts), [alternate flights](https://help.tripit.com/en/support/solutions/articles/103000063402-alternate-flights)) | Florence should derive route and current state from identifiers, monitor only meaningful changes, recompute downstream transport/lodging/childcare implications, and continue through the carrier or vendor rather than stopping at a list of alternatives. |
| **Invitations, parties, and social commitments** | Partiful can poll dates, collect RSVP-time questionnaire answers such as dietary restrictions, and convert the chosen time into RSVPs; reminders vary by RSVP status and account state. Evite similarly distinguishes guests added to its list from people reached only through a shareable link, who do not receive automatic RSVP reminders. ([Partiful polls](https://help.partiful.com/en-us/articles/15525422-can-i-poll-or-survey-my-guests), [Partiful reminders](https://help.partiful.com/en-us/articles/15525436-what-event-reminders-do-you-send), [Evite event pages](https://support.evite.com/products/invitations/create-and-edit/event-pages)) | An invitation is a changing commitment with availability, RSVP, attendees, dietary needs, contribution/gift, transport, and host updates. Florence should know whether the source will follow up and own the missing step when it will not. |
| **Household services and delegated help** | TaskRabbit spans repairs, cleaning, delivery, returns, errands, prescription pickup, and ongoing personal assistance. Its normal flow still requires describing the task, choosing a person, scheduling, communicating, and paying after completion. ([services](https://www.taskrabbit.com/services), [personal assistants](https://www.taskrabbit.com/services/personal-assistant)) | Finding a provider is not a solved household need. Florence should retain requirements, compare candidates, communicate scope, coordinate access and timing, track changes, confirm the work, and remember what succeeded for next time. |
| **Whole-family coordination and capture** | Google family calendars do not notify family members when another member creates, edits, or deletes an event. Skylight's family organizer imports events from email, paper schedules, pictures, and PDFs and combines calendars, chores, meals, tasks, and lists. ([Google family calendar](https://support.google.com/families/answer/7157782?hl=en-in), [Skylight Calendar](https://myskylight.com/calendar/)) | Florence must read beyond searchable message text, including images and forwarded artifacts, but should not become another organizer families must maintain. She should preserve the original source, use the existing family conversation as the control surface, and speak only when the reconciled plan needs attention. |

## Direct parent reports: where the mental work remains

These accounts are user reports, kept separate from the product facts above:

- A parent with four children described drowning in school emails, sports updates, appointments, bills, forms, newsletters, travel confirmations, and schedule changes while trying to maintain one usable family schedule. ([original post](https://www.reddit.com/r/workingmoms/comments/1td5dze/how_do_you_juggle_the_email_jungle/))
- A kindergarten parent reported district, school, and teacher email plus multiple apps and paper; commenters described duplicated notices, important information buried in graphics, and notification volume high enough that messages were ignored. ([original post](https://www.reddit.com/r/workingmoms/comments/1vnr34y/overwhelmed_by_school_emails/))
- One parent stopped receiving daycare-app notifications after an update while the other adult still received them but did not read them. The source existed, but neither app access nor notification delivery produced household ownership. ([original post](https://www.reddit.com/r/workingmoms/comments/1q429ad/the_mental_load_at_its_finest/))
- A parent using a shared recipe, meal-plan, and grocery app said the app helped but the planning and shopping mental load still remained mostly with her. ([original post](https://www.reddit.com/r/workingmoms/comments/1omyy8a/meal_planning_is_killing_me/))
- A parent coordinating care for three children described annual visits, phone holds, voicemail, follow-ups, labs, and imaging as a part-time job. ([original post](https://www.reddit.com/r/workingmoms/comments/1e4eh6w/managing_doctors_appointments_for_the_family_by/))
- A parent who had already hired a cleaner, mother's helper, and babysitter remained overwhelmed by communication, scheduling, lateness, rescheduling, and mismatched expectations. ([original post](https://www.reddit.com/r/workingmoms/comments/1r170qw/household_management_help/))
- A parent preparing family travel described the work as a dependency tree of laundry, packing, children, bags, and transport; dividing complete areas of ownership worked better than sharing each micro-step. ([original post](https://www.reddit.com/r/workingmoms/comments/17y8hwn/mental_load_preparing_for_travel/))

The recurring complaint is not simply “too many apps.” It is that the parent remains the integration layer, relevance filter, permission resolver, planner, and follow-up system even when every individual app works as documented.

## Audit against Florence's current contract

### Already covered; do not build again

`PLAN.md` and the recent research already establish the right foundations:

- one general objective-driven agent rather than domain routers;
- complete 90-day onboarding discovery without a memory-expiration rule;
- a household Vault with durable facts and reusable artifacts;
- source-linked Docket items with ownership, next action, dependency, and completion condition;
- dynamic tool composition across email, calendars, pages, PDFs, browser, calls, texts, and time;
- immediate, human-feeling conversational presence;
- selective, non-repetitive proactivity; and
- success only after the outside result is confirmed.

The app evidence does not justify another round of broad ingestion infrastructure or named provider workflows.

### Three requirements to make explicit in product behavior and live testing

#### 1. Coverage-aware source truth

Before Florence says “nothing needs attention,” she should know which sources, pages, accounts, attachments, images, and time ranges were actually observed; which may be stale; and whether the provider emits the expected signal at all. If coverage is incomplete, the useful answer is the exact uncertainty and the next way Florence can resolve it—not a false all-clear.

This is a general behavior across a Brightwheel report dependent on checkout, a TeamSnap calendar that may lag, an Evite guest who will not receive reminders, and a school detail embedded in an image. It should be expressed as agent judgment and source evidence, not as four provider-specific prompt branches.

#### 2. Capable-actor routing

For each next action, Florence should determine who can actually perform it through the relevant surface and who should own the household decision. That may differ from the message sender or the adult who first connected an account. She should do all reversible preparation first, then ask or hand off to the one capable person with the context already compressed.

This is the same behavior whether only the Instacart purchaser can reschedule, a healthcare proxy lacks a permission, a school controls a refund, a childcare administrator must change a payer, or a team member lacks manager access.

#### 3. Change propagation to verified closure

When an external state changes, Florence should update the objective and its practical household consequences—not merely repeat the changed fact. A moved activity may change travel, pickup, a sibling conflict, dinner, and a volunteer assignment. A submitted school payment may still be pending. A booked provider may still reschedule. A selected flight may still require the airline change.

One durable objective should carry the current source state, owner, next action, unresolved dependency, completion condition, last useful family update, and what durable artifact or preference should be remembered afterward. Florence should return to the family only for a real decision, material change, blocker, or verified result.

## Highest-value acceptance rehearsal

Use varied sources and wording to test this as one general behavior:

1. An obligation arrives through an email whose key details are in an image or attachment and whose live portal requires a particular adult account.
2. Florence identifies the child and consequence, joins duplicates, checks the family's real constraints, prepares every reversible step, and asks the capable adult one consequential question.
3. The first external action produces a pending state rather than completion. Florence waits or re-checks without spamming the family.
4. A later source change forces a downstream replan. Florence updates the household plan once, performs the reachable work, and closes only after the authoritative source or responsible person confirms the result.
5. The source, durable fact/artifact, decision, and outcome remain retrievable months later.

Vary the domain—school, childcare, sport, health, meal, trip, invitation, or household service—without changing the underlying reasoner path. Passing that rehearsal is more valuable than adding another provider name to a tool list.
