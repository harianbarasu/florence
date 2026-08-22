import type {
  FamilyMemberInput,
  FamilyMemberMutationInput,
  FamilyMemberProfile,
  PatchFamilyMemberInput,
} from "@florence/contracts";
import { Check, Plus, X } from "lucide-react";
import { type FormEvent, useRef, useState } from "react";

type MemberEditorProps = {
  member?: FamilyMemberProfile;
  isSaving: boolean;
  onCancel: () => void;
  onSave: (memberId: string, input: FamilyMemberMutationInput) => Promise<unknown>;
};

export function MemberEditor({ member, isSaving, onCancel, onSave }: MemberEditorProps) {
  const [error, setError] = useState<string | null>(null);
  const memberId = useRef(member?.id ?? crypto.randomUUID());

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    const data = new FormData(event.currentTarget);
    const input = member ? memberPatch(data, member) : newChildInput(data);
    if (input === null) {
      onCancel();
      return;
    }
    try {
      await onSave(memberId.current, input);
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : "Florence could not save this person.");
    }
  }

  return (
    <form className="member-editor" onSubmit={(event) => void submit(event)}>
      <div className="editor-heading">
        <div>
          <p className="eyebrow">{member ? "Edit family member" : "Add child"}</p>
          <h2>{member?.displayName ?? "Who should Florence know?"}</h2>
        </div>
        <button
          className="icon-button"
          type="button"
          onClick={onCancel}
          aria-label="Close family member form"
        >
          <X size={19} />
        </button>
      </div>

      <div className="form-grid">
        <Field
          label="First name"
          name="firstName"
          defaultValue={member?.firstName}
          autoComplete="given-name"
          required
        />
        <Field
          label="Last name"
          name="lastName"
          defaultValue={member?.lastName ?? undefined}
          autoComplete="family-name"
          required={member?.kind === "adult"}
        />
        {(member?.kind ?? "child") === "child" && (
          <Field
            label="School or daycare"
            name="school"
            defaultValue={member?.school}
            placeholder="School or daycare"
          />
        )}
        <Field
          label="Activities"
          name="activities"
          defaultValue={member?.activities?.join(", ")}
          placeholder="Soccer, piano, robotics…"
        />
      </div>
      {member?.status === "verified" && (
        <p className="authority-note">
          This changes how they appear in your family. They still control their own Florence settings.
        </p>
      )}
      {error && (
        <p className="form-error" role="alert">
          {error}
        </p>
      )}
      <div className="form-actions">
        <button className="secondary-button" type="button" onClick={onCancel}>
          Cancel
        </button>
        <button className="primary-button" type="submit" disabled={isSaving}>
          {member ? <Check size={17} /> : <Plus size={17} />}
          {isSaving ? "Saving…" : member ? "Save changes" : "Add child"}
        </button>
      </div>
    </form>
  );
}

function Field({
  label,
  name,
  ...input
}: React.InputHTMLAttributes<HTMLInputElement> & { label: string; name: string }) {
  return (
    <label className="field">
      <span>{label}</span>
      <input name={name} {...input} />
    </label>
  );
}

function required(data: FormData, key: string): string {
  const value = optional(data, key);
  if (!value) throw new Error(`${key} is required`);
  return value;
}

function optional(data: FormData, key: string): string | undefined {
  const value = data.get(key);
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

function newChildInput(data: FormData): FamilyMemberInput {
  const activities = list(data, "activities");
  return {
    kind: "child",
    firstName: required(data, "firstName"),
    lastName: optional(data, "lastName") ?? null,
    ...optionalString(data, "school"),
    ...(activities.length ? { activities } : {}),
  };
}

function memberPatch(data: FormData, member: FamilyMemberProfile): PatchFamilyMemberInput | null {
  const patch: PatchFamilyMemberInput = {};
  const firstName = required(data, "firstName");
  const lastName = optional(data, "lastName") ?? null;
  const activities = list(data, "activities");
  const school = optional(data, "school") ?? null;

  if (member.kind === "adult" && lastName === null) throw new Error("lastName is required");
  if (firstName !== member.firstName) patch.firstName = firstName;
  if (lastName !== member.lastName) patch.lastName = lastName;
  if (member.kind === "child" && school !== (member.school ?? null)) patch.school = school;
  if (!sameList(activities, member.activities ?? [])) patch.activities = activities;
  return Object.keys(patch).length ? patch : null;
}

function optionalString(data: FormData, key: string): Record<string, string> {
  const value = optional(data, key);
  return value ? { [key]: value } : {};
}

function list(data: FormData, key: string): string[] {
  return (
    optional(data, key)
      ?.split(",")
      .map((value) => value.trim())
      .filter(Boolean) ?? []
  );
}

function sameList(left: readonly string[], right: readonly string[]): boolean {
  return left.length === right.length && left.every((value, index) => value === right[index]);
}
