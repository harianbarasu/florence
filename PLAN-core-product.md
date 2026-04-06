# Florence Core Product Plan

## Product Thesis

Florence is a Hermes-powered generalized household agent.

The product target is Ollie-like breadth with one important difference:

- Florence is multiplayer-first
- the parent group chat is the primary product surface
- DM is the private sidecar for sensitive or individually scoped work

The web app is support infrastructure:

- onboarding
- management
- billing
- trust controls
- other computer-native tasks

## Wedge User

First wedge:

- two-parent family
- school-age kids
- heavy school and activity email volume
- important household context spread across both parents

Florence should reduce hidden mental load without narrating or diagnosing family dynamics.

## Core Product Loops

### 1. Inbox -> Plan

Florence reads family-relevant Gmail and calendar signals, then turns them into a short operational plan.

This is the first trust engine.

Desired activation:

- Google sync completes
- Florence sends a DM-first operational brief to the setup actor
- the user can promote the shared version into the parent group

### 2. Capture -> Handled

The user texts Florence anything household-related and Florence turns it into structure.

This includes:

- mental dumps
- screenshots
- flyers
- photos
- school documents
- reminders
- plans
- meals
- pantry or fridge understanding
- grocery lists

### 3. Briefs -> Stay Ahead

Florence proactively keeps the household legible.

Default-on behavior:

- daily brief
- weekly brief
- operational nudges
- review prompts

Routing:

- first sync brief: DM-first
- ongoing daily and weekly briefs: group by default once the parent group exists

## Visibility Model

Group by default:

- shared logistics
- calendar items
- reminders
- meals and grocery
- household tasks
- briefings

DM by default:

- private mental load
- emotional support
- health-sensitive content
- individually scoped synthesis

Promotion model:

- private input stays private by default
- Florence can extract structured household state from it
- the individual can promote the structured result into the shared parent group

## Product Boundary

In scope:

- household coordination
- school and activity email triage
- planning
- reminders
- lists
- meals
- grocery
- travel
- "what am I forgetting?"
- daily and weekly briefs

Hard stop:

- no contacting third parties autonomously

Sensitive domains:

- Florence can summarize, organize, compare, remind, and help prepare questions
- Florence should not present itself as professional authority

## Engine Strategy

Hermes core is the platform Florence runs on.

Policy:

- reuse Hermes core instead of rebuilding generalized-agent behavior in Florence
- keep Florence-specific behavior in household state, policies, prompts, messaging surfaces, and web support
- minimize divergence in agent-core files so upstream Hermes improvements remain ingestible

Current repo state after fetch:

- Florence is ahead of `upstream/main`
- Florence is also behind `upstream/main`
- regular Hermes sync remains part of the product strategy

## Implementation Phases

### Phase 1: First Trust Loop

Goal:

- replace plumbing-style sync completion messaging with a real operational brief
- make the first sync brief DM-first
- support promotion from DM into the household group
- send ongoing scheduled briefs to the group once the group exists

Backend work:

- build a post-sync operational brief renderer
- persist promotable brief metadata on assistant messages
- teach DM ingress to handle `share` / `send to group`
- prefer group channel routing for scheduled daily and weekly briefs

### Phase 2: Capture -> Handled

Goal:

- make Florence reliable for text dumps, screenshots, flyers, photos, and meal/grocery tasks

Backend work:

- expand DM and group chat behavior toward generalized household capture
- add household-state creation paths for reminders, tasks, meals, and shopping items
- add structured handling for school documents and photos

### Phase 3: Broader Household Agent Behavior

Goal:

- deepen memory, follow-up, and proactive household management

Backend work:

- richer memoryful check-ins
- better promotion from private context into shared household state
- stronger meal, pantry, and grocery workflows
- tighter Hermes-core alignment where generalized-agent improvements can be reused directly

## Immediate Slice To Implement

This patch should do only the first backend slice:

1. DM-first post-sync operational brief
2. DM reply affordance to promote that brief into the parent group
3. scheduled daily and weekly briefs prefer the parent group when it exists

## Acceptance Criteria For Immediate Slice

- sync completion no longer leads with `First sync complete`
- the setup actor receives a short operational brief in DM
- if a household group already exists, the DM includes a share affordance
- replying with `share` from DM posts the shared version into the group
- scheduled briefs route to the group when a group channel exists
