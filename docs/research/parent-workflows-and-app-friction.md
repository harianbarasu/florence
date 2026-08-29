# Parent workflows and app friction

_Research date: August 29, 2026_

## Executive finding

Parents do not need Florence to become another organizer they must maintain. The recurring unmet need is an agent that can absorb fragmented household inputs, decide what materially matters, reconcile them into one household picture, and carry the resulting work through without leaving one parent as the project manager.

Across school, childcare, activities, meals, health, travel, and home administration, the same general loop appears:

1. **Capture** a message, email, calendar change, form, receipt, photo, PDF, link, portal update, or conversation.
2. **Understand** the people, date, obligation, source, and household context.
3. **Triage** what is noise, useful reference, a decision, a deadline, or work Florence can remove.
4. **Reconcile** duplicates and changes into one household truth.
5. **Act** by preparing or completing the form, RSVP, calendar change, list, booking, purchase, call, text, or plan.
6. **Close the loop** with the responsible person after the outside result is known.
7. **Remember** durable context and reusable artifacts for the next situation.

That is one general household-agent behavior, not ten vertical features or a router over named parent workflows.

## Evidence boundary

- **Product facts** below come from first-party help centers, product documentation, or vendor pages. They establish what a product says it supports, not adoption, quality, or independent effectiveness.
- **Parent friction** comes from dated, first-person posts and comments. These are anecdotal and nonrepresentative; they should not be read as prevalence estimates. Repeated patterns across independent discussions are useful qualitative signals for product rehearsal.
- The implementation comparison is a point-in-time inspection of the Florence worktree on August 29, 2026. It distinguishes an available primitive from a polished end-to-end family experience.

## The stack parents actually describe using

The evidence does not show one winning family operating system. It shows a stack:

- a shared Google Calendar, Cozi, Skylight, a whiteboard, or some combination for household dates;
- ParentSquare, Schoology, PowerSchool, Remind, ClassDojo, Brightwheel, Procare, and school-specific portals for school or childcare;
- TeamSnap, SportsEngine, GameChanger, email, calendar subscriptions, and separate text threads for activities;
- AnyList, shared notes, grocery-store apps, and recipe sites for meals and shopping;
- MyChart plus calls, voicemail, paper forms, and a shared calendar for health;
- TripIt, airline apps, confirmation email, and shared itineraries for travel; and
- iMessage, WhatsApp, GroupMe, and shared documents to fill the coordination gaps between those systems.

One parent reported using ParentSquare, Schoology, PowerSchool, LinqConnect, Remind, ClassDojo, and GroupMe before even counting sports apps ([first-person thread, September 5, 2025](https://www.reddit.com/r/Parenting/comments/1n7y14x/how_many_apps_does_your_kids_school_expect_you_to/)). Another described Trello, a shared Google Calendar, a shared child email address, and a grocery app as the family's homegrown operating system ([first-person thread, April 7, 2024](https://www.reddit.com/r/workingmoms/comments/1bx9brf/tools_i_use_to_program_manage_our_life/)). These are anecdotes, but they illustrate why adding one more manually maintained destination is unlikely to remove mental load.

## Workflow evidence and implications

| Repeated coordination area | First-party product evidence | First-person friction evidence (anecdotal) | Product implication for Florence |
| --- | --- | --- | --- |
| **Family calendar and scheduling** | Google automatically creates an editable family calendar for a Google family group, but does not notify members when another member creates, changes, or deletes an event ([Google Families Help](https://support.google.com/families/answer/7157782?co=GENIE.Platform%3DDesktop&hl=en)). Skylight documents profiles, chores, lists, meal planning, calendar sync, and importing events from photos, emails, and PDFs ([Skylight Calendar features](https://skylight.zendesk.com/hc/en-us/articles/48778850390171-Calendar-Features)). | A parent comparing Cozi, Jam, and Maple said incomplete two-way Google sync forced duplicate entry of invitations ([August 27, 2024](https://www.reddit.com/r/workingmoms/comments/1f2j2jb)). Other parents describe manually importing school calendars, lunch menus, sports schedules, and event photos into a shared calendar ([August 18, 2025](https://www.reddit.com/r/workingmoms/comments/1mtrue9/how_do_you_keep_track_of_all_the_kidrelated_events/)). | Maintain one deduplicated household timeline across both adults and external sources. Detect changes from the live source rather than assuming exported calendars or notifications are current. Preserve the existing boundary: ask the owner before promoting an item found only on one person's calendar into household truth. |
| **School communications, forms, and fees** | ParentSquare combines messaging, digital forms and permission slips, calendars, RSVPs, appointment signup, and payments ([ParentSquare](https://www.parentsquare.com/platform/parent-and-community-engagement/)). ClassDojo documents two-way messaging, attendance alerts, calendars, reminders, and signups ([ClassDojo](https://www.classdojo.com/en-us/districts/solutions/family-engagement/)). | Parents describe irrelevant blasts, duplicated texts and emails, repeated annual forms, multiple logins, unreliable payment portals, and missing important items amid the noise. One parent missed a child's award; others describe daily manual calendar consolidation ([February 27, 2026](https://www.reddit.com/r/Parenting/comments/1rge5hi/anyone_else_struggling_at_keeping_up_with_the/), [August 17, 2025](https://www.reddit.com/r/firstworldproblems/comments/1mqw9z2/my_kids_school_requires_me_to_download_3/)). | Read broadly but surface selectively. Convert child-specific deadlines, requested supplies, schedule changes, forms, RSVPs, and payments into one source-linked action with an owner and exact due date. Deduplicate repeated notices, retain the source, and offer to complete the next step rather than merely summarize it. |
| **Daycare and childcare handoffs** | Brightwheel's activity feed and daily reports can include check-in/out, food, naps, toileting, learning, notes, photos, and incidents; its messaging supports absences and pickup changes ([Brightwheel activity feed](https://help.mybrightwheel.com/en/articles/942392-view-your-child-s-activity-feed), [Brightwheel messaging](https://help.mybrightwheel.com/en/articles/2098452-start-messaging-in-brightwheel)). Procare documents similar attendance, messaging, daily activity, incident, and parent-payment functions ([Procare engagement overview](https://www.procaresupport.com/procare-online/docs/engagement-functionality-overview), [Procare parent payments](https://www.procaresupport.com/procare-online/docs/parent-payments-via-the-child-care-mobile-app)). | Parents value concrete handoff facts—feeding, sleep, diapers, mood, incidents, tomorrow's needs—but report delayed or inaccurate feed entries and teachers burdened by data entry. Photo volume matters less than reliable exceptions ([May 31, 2023](https://www.reddit.com/r/workingmoms/comments/13wkm78/whats_a_normal_amount_of_communication_from/), [January 6, 2026](https://www.reddit.com/r/toddlers/comments/1q54l4i/daily_daycare_updates/)). One parent said daycare continued contacting the mother although the father handled pickups and administration ([July 8, 2023](https://www.reddit.com/r/workingmoms/comments/14tdvt4/daycare_only_communicates_with_me/)). | Produce an exception-oriented handoff, not a feed recap: what changed, what the child needs, and who is handling pickup or follow-up. Route to the adult who actually owns that responsibility rather than defaulting to the mother, sender, or most active user. Preserve routine details for later questions without flooding the family chat. |
| **Activities and sports** | TeamSnap documents schedules, availability, volunteer assignments, carpools, messaging, invoices, and automatic pre-event reminders ([TeamSnap features](https://www.teamsnap.com/teams/features), [TeamSnap messages](https://www.teamsnap.com/teams/features/messages)). Its support documentation warns that subscribed Google calendars can lag changes by up to 24 hours ([TeamSnap sync troubleshooting](https://helpme.teamsnap.com/article/1446-troubleshooting-schedule-assignments-and-availability)). SportsEngine similarly documents a unified multi-athlete schedule, chat, calendar sync, and notifications ([SportsEngine mobile app](https://www.sportsengine.com/hq/features/mobile-app/), [parent help](https://help.sportsengine.com/en/collections/3502726-for-parents)). | Parents and coaches report missed app messages, separate text threads for urgent changes, and transportation conflicts around working-parent schedules ([July 25, 2025](https://www.reddit.com/r/Homeplate/comments/1m971y5/), [May 17, 2026](https://www.reddit.com/r/workingmoms/comments/1tfwrig/)). | Treat the live source as authoritative when available. Reconcile time and venue changes with both adults' calendars, travel time, availability, equipment, snacks, volunteer duties, fees, and carpool gaps. The output should be the changed plan and next useful action, not “there is a game.” |
| **Meals, recipes, and groceries** | AnyList supports shared lists, recipe import and sharing, meal planning, moving meals when plans change, and adding recipe ingredients to a grocery list ([AnyList features](https://www.anylist.com/features), [AnyList meal planning](https://anylist.net/meal-planning), [shared recipes](https://help.anylist.com/articles/share-recipe/)). Cozi documents a meal planner connected to the family calendar and shared shopping lists ([Cozi meal planner](https://www.cozi.com/blog/cozi-meal-planner/), [Cozi uses](https://www.cozi.com/ways-to-use-cozi/)). | Parents say the hardest work is deciding: checking inventory and specials, fitting meals around activities and energy, rotating accepted recipes, and building the list. Useful systems remember family-approved meals and adapt when the week changes ([December 15, 2025](https://www.reddit.com/r/Parenting/comments/1pn0g0e/how_does_meal_planning_actually_work_in_your/), [February 9, 2026](https://www.reddit.com/r/workingmoms/comments/1r09tgr/)). | Keep durable, revisable recipes with ingredients or canonical source, method, substitutions, preferences, and family feedback. Compose a plan against the actual week, reuse leftovers, change it when reality changes, and derive the collaborative grocery list or cart. A recipe, plan, and list should be linked transformations of household knowledge, not isolated trivia. |
| **Health, appointments, and medications** | MyChart family access brings children's appointments, immunizations, medications, results, and school/daycare forms under a caregiver account ([MyChart family access](https://www.mychart.org/l/en-us/features/family/), [proxy access](https://www.mychart.org/l/en-us/help/proxy/)). The American Academy of Pediatrics recommends retaining medical history, medications, specialists, notes, referrals, and follow-ups ([AAP appointment guidance](https://www.healthychildren.org/English/family-life/health-management/Pages/making-the-most-of-your-childs-health-appointments-parent-tips.aspx)). Apple distinguishes medication reminders from recording taken or skipped doses and can follow up when a dose is unlogged ([Apple medication tracking](https://support.apple.com/en-euro/guide/iphone/iph811670c81/26/ios/26)). | A parent managing three children's doctors described scheduling, holds, voicemail, annual visits, follow-ups, labs, and imaging as a part-time job. Commenters split specialties between adults and rely on shared calendars ([July 16, 2024](https://www.reddit.com/r/workingmoms/comments/1e4eh6w/managing_doctors_appointments_for_the_family_by/)). | Track the entire loop: find and schedule around both adults, prepare questions and forms, remind the right person, record completion, and carry referrals, results, medication logging, or follow-up forward. Preserve stable child context beyond the 90-day onboarding window. A reminder firing is not evidence that the task or dose happened. |
| **Travel** | TripIt turns confirmation emails into itineraries and documents calendar sync, sharing, travel documents, directions, real-time flight alerts, and alternate-flight tools ([TripIt](https://www.tripit.com/web), [TripIt feature comparison](https://help.tripit.com/en/support/solutions/articles/103000063396-tripit-or-tripit-pro-), [sharing](https://help.tripit.com/en/support/solutions/articles/103000063347-trip-sharing-in-the-tripit-app)). | Parents describe one adult owning itinerary, reservations, packing, snacks, navigation, and contingencies; one called herself the family's “planner, packer, mental-load handler” ([June 23, 2025](https://www.reddit.com/r/workingmoms/comments/1li84bz)). A family-packing request illustrates the usefulness of organizing by day, activity, climate, and a first-day bag ([first-person thread](https://www.reddit.com/r/adhdwomen/comments/1tv8rbj/please_help_me_pack_prepare_for_my_family/)). | Parse confirmations automatically; enrich identifiers such as flight numbers into route and status; assemble a shared itinerary; monitor material disruptions; find workable alternatives; and generate reusable destination- and activity-aware preparation lists. Do not make the parent restate information available in the confirmation. |
| **Household chores and services** | Tody documents a prioritized cleaning plan, recurring assignments, rotation, household sync, and a “FairShare” view intended to keep the plan from living in one person's head ([Tody method](https://todyapp.com/method)). Greenlight Family Hub supports recurring chores, rewards, child completion, parent approval, and chores that are “up for grabs” ([Greenlight chores](https://help.greenlight.com/hc/en-us/articles/52141594156571-How-do-Chores-work-on-Greenlight-Family-Hub)). Taskrabbit documents repair, cleaning, laundry, shopping, errands, personal assistance, and recurring cleaning ([Taskrabbit services](https://www.taskrabbit.com/services), [recurring cleaning](https://support.taskrabbit.com/hc/en-us/articles/46260411649307-How-Do-I-Book-a-Recurring-Cleaning-Task)). | Parents report that chores apps and charts often last only a week or two when they are busy or over-gamified ([first-person chore-app account](https://www.reddit.com/r/ParentingTech/comments/1oiefns/we_tried_every_chore_app_chart_and_allowance/)). Outsourcing can help, but researching, vetting, briefing, scheduling, monitoring, and switching providers becomes another job ([August 6, 2023](https://www.reddit.com/r/workingmoms/comments/15jzlna/hire_a_house_cleaner_they_said/), [June 2, 2026](https://www.reddit.com/r/workingmoms/comments/1tuph9f/managing_the_household/)). | Own outcomes rather than emit chores. Notice what is due, identify the responsible person, gather requirements, compare options, ask only for the real choice, schedule or arrange the service, track changes, and close the loop. Avoid building a new gamified household board unless a family explicitly asks for one. |
| **Shopping and payments** | Instacart Family Carts let household members add or remove items and receive order updates while one checkout member remains responsible for payment, replacements, and shopper communication ([Instacart Family Carts](https://www.instacart.com/help/section/xqnziiiby/2080088020)). Apple Cash Family supports recurring allowances and parent-visible child transactions ([Apple Cash Family](https://support.apple.com/en-la/105010)). ParentSquare's platform includes school payments alongside forms and RSVPs ([ParentSquare](https://www.parentsquare.com/platform/parent-and-community-engagement/)). | One parent rejected “make me a list” as help because noticing what is low, checking inventory, meal planning, and creating the list are the actual mental work; shared lists helped when everyone contributed continuously ([June 5, 2023](https://www.reddit.com/r/workingmoms/comments/140y6bf)). Parents also describe broken school payment apps and repeated account setup ([August 17, 2025](https://www.reddit.com/r/firstworldproblems/comments/1mqw9z2/my_kids_school_requires_me_to_download_3/)). | Remember staples, sizes, preferences, prior successful choices, and the accountable purchaser. Build carts collaboratively, resolve replacements or price decisions, and ask before checkout or payment. The useful unit is the resolved household purchase, not a detached list. |
| **Family messaging** | iMessage supports inline replies that preserve context and Tapbacks as lightweight acknowledgements ([Apple inline replies](https://support.apple.com/en-mide/guide/iphone/iph82fb73ba3/ios), [Apple Tapbacks](https://support.apple.com/en-mide/guide/iphone/iph018d3c336/ios)). Brightwheel deliberately supports staff-parent messaging but not parent-parent direct messages ([Brightwheel messaging](https://help.mybrightwheel.com/en/articles/2098452-start-messaging-in-brightwheel)). | Parents use WhatsApp, iMessage, and shared documents for playdates, class parties, and gifts when school/daycare products do not support parent coordination, but some describe the extra channel as another noisy obligation ([July 4, 2024](https://www.reddit.com/r/workingmoms/comments/1duf6l1/daycare_room_parent_chats/)). Others report ignoring daycare apps because 95% of notifications feel like noise, while the other adult assumes they are handling it ([January 4, 2026](https://www.reddit.com/r/workingmoms/comments/1q429ad/the_mental_load_at_its_finest/)). | Do not create another social feed. Use the family's existing iMessage surface, reply in context, use reactions for genuinely low-content acknowledgement, target the relevant adult, and give immediate acknowledgement, meaningful progress, and one natural closure. Digest low-priority source traffic rather than reproducing it. |

## Comparison with Florence today

### The controlling product direction is already right

The current [product contract](../../PLAN.md#product-contract) already says Florence is a general household agent rather than a workflow catalog, judges success by lived outcome, reads sources before asking, avoids duplicate reminders, and treats proactivity as the next useful action rather than repeated facts. Its [Google and Vault contract](../../PLAN.md#google-review-and-household-knowledge) also correctly separates complete 90-day onboarding discovery from permanent memory, makes the parental unit the knowledge unit, and includes reusable recipes, lists, plans, notes, and references.

The implementation has substantial primitives already:

- a broad tool-composition surface for history, Vault, retained sources, public pages, reminders, the family calendar, calls, SMS, browser work, maps, weather, flights, and Google Workspace ([reasoner tool registry](../../apps/api/src/reasoner.ts#L5383));
- durable work state with browser, phone, text, continuation, steering, progress, and terminal-result state ([`FamilyWorkStateV1`](../../packages/database/src/store.ts#L663));
- a household docket with urgency, due time, and whether an answer is needed ([`SharedBriefingCandidate`](../../packages/database/src/store.ts#L345)); and
- a household Vault presentation model for facts, preferences, routines, and reusable artifacts, including recipes, lists, plans, notes, and references ([memory contract](../../packages/contracts/src/index.ts#L124), [Vault library UI](../../apps/web/src/App.tsx#L2303)).

This means the primary gap is not a missing catalog of parent apps. It is making those general capabilities produce the observable household loop reliably.

## Highest-leverage observable product gaps

### 1. Turn fragmented inputs into one source-linked household action

Florence should be able to receive or discover the same obligation through email, a calendar feed, a forwarded text, a screenshot, a PDF, a link, or a portal and create one reconciled item—not six notices. The item should answer:

- What materially changed or needs doing?
- Which child or household member does it concern?
- Who currently owns the next step?
- What is the exact deadline or trigger?
- What can Florence do now?
- What source can Florence reopen if challenged?

The current docket record has category, summary, urgency, due time, and `needsAnswer`, but does not itself expose an owner, next action, lifecycle state, or source ([database type](../../packages/database/src/store.ts#L345), [Vault contract](../../packages/contracts/src/index.ts#L237)). The product need is broader than adding fields: the message must make the decision or work legible and then keep its state through completion.

**Observable bar:** forward Florence the same school form as an email and screenshot, then receive one docket item with the correct child, deadline, owner, source, and offer to finish it.

### 2. Make the docket an action queue, not a recap

Parents' complaint is not just information fragmentation; it is still having to notice, decide, delegate, check, and follow up. A useful docket should rank by consequence and readiness, then let Florence compose her existing browser, call, text, calendar, reminder, Gmail, and search capabilities to complete the next step.

The expected conversational shape is:

> Theo's field-trip form and $18 fee are due Thursday. Jackson handled the last school payment; I can open the form and prepare it now, then ask him only if the portal needs a final payment confirmation.

It is not:

> There is a field trip on Thursday.

**Observable bar:** every surfaced item ends in a concrete outcome, an immediately useful partial result plus one genuinely blocking choice, or a clear reason no path remains.

### 3. Link and transform reusable household artifacts

Florence can already store generic artifacts. The higher-value behavior is to use and transform them across situations:

- recipe → week plan → grocery list or cart;
- activity event → travel time → equipment, snack, volunteer, and pickup plan;
- trip confirmations → itinerary → weather-aware packing and contingency list;
- health appointment → preparation notes and forms → medication, referral, or follow-up work; and
- successful household service → retained provider, requirements, price context, and a future rebooking path.

These should remain generic source-linked artifacts, not new domain runtimes. The artifact model's `details` and tags can hold useful content today ([memory contract](../../packages/contracts/src/index.ts#L137)); the product rehearsal should prove Florence retrieves the right artifact, revises it when corrected, derives the next useful artifact, and remembers the real outcome.

**Observable bar:** ask Florence to plan three dinners during a busy activity week; she recalls accepted recipes and substitutions, checks the week, proposes a realistic plan, turns it into one list, adapts after a schedule change, and keeps the revised artifacts in the Vault.

### 4. Preserve ownership, handoffs, changes, and closure

The parental unit should be the knowledge unit, but that does not mean every input should be broadcast to both adults. Florence needs to remember responsibility and route the next step accordingly: pickup, school payments, a medical specialty, team snacks, a contractor visit, or grocery checkout. She should update the group only when the conclusion is useful to both, privately ask the responsible adult when appropriate, and allow ownership to move without losing state.

Family plans are mutable. A changed game venue, delayed flight, low-energy meal substitution, different pickup adult, or unacknowledged medication is normal. Monitoring should speak on material change or required attention and stop after resolution.

**Observable bar:** one adult cannot make pickup; Florence notices the conflict, asks the other adult or helps arrange the handoff, updates the household plan once, and closes the loop without repeating stale reminders.

## Broad product rehearsals, not hardcoded workflows

Use these to evaluate the same general agent loop across different evidence and actions:

1. **School:** find an unanswered form and fee across email and a linked portal; identify the child, due date, owner, and complete as much as possible.
2. **Activity change:** notice a changed venue in the live team source; reconcile the calendar, route, equipment, volunteer duty, and pickup plan.
3. **Meals:** plan around the real week and family preferences; reuse recipes, generate the list or cart, and adapt when plans change.
4. **Health:** arrange an appointment around both adults' schedules and travel time; carry forms, preparation, and follow-up through.
5. **Travel:** turn confirmations into an itinerary, enrich identifiers, monitor material changes, find alternatives, and prepare the family.
6. **Home service:** understand the repair need, gather and compare workable providers, contact them, ask the parent only for the real choice, schedule, and verify the result.

The evaluator should vary source type, wording, household member, provider, and desired result. A passing implementation composes tools from the objective; it does not recognize a scenario name.

## What this evidence argues against

- **Another parent dashboard requiring manual upkeep.** Florence's iMessage conversation and automatically maintained Vault/docket should remain the primary experience.
- **Per-app or per-domain workflow routers.** Most parent products already handle their own vertical. Florence's advantage is judgment and work across them.
- **A notification firehose or “everything we found” digest.** Complete reading and selective surfacing are different jobs.
- **Source summaries without an owner and next action.** That moves information but leaves the mental load intact.
- **A new family social feed.** Existing messaging is sufficient; Florence should use it more naturally and selectively.
- **Gamification or chore mechanics by default.** The evidence is mixed, and the stronger need is outcome ownership and low upkeep.
- **A 90-day memory lifetime.** Ninety days bounds onboarding discovery. Schools, doctors, allergies, routines, recipes, providers, and preferences must survive until corrected or forgotten.

## Product priority

The highest-leverage next product tranche is a **source-to-outcome household docket**: ingest a broad source, reconcile it with household context and duplicate evidence, present only the prioritized action to the right adult, compose existing tools to finish it, then retain the useful result.

That one behavior addresses the most repeated complaints across school, childcare, sports, meals, health, travel, services, shopping, and messaging while preserving Florence as a general-purpose agent.

