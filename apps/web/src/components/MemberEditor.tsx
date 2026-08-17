import type { FamilyMemberInput, FamilyMemberProfile } from "@florence/contracts";
import { Check, Plus, X } from "lucide-react";
import { type FormEvent, useRef, useState } from "react";

type MemberEditorProps = {
  member?: FamilyMemberProfile;
  isSaving: boolean;
  onCancel: () => void;
  onSave: (memberId: string, input: FamilyMemberInput) => Promise<unknown>;
};

export function MemberEditor({ member, isSaving, onCancel, onSave }: MemberEditorProps) {
  const [kind, setKind] = useState<"adult" | "child">(member?.kind ?? "child");
  const [error, setError] = useState<string | null>(null);
  const memberId = useRef(member?.id ?? crypto.randomUUID());

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setError(null);
    const data = new FormData(event.currentTarget);
    const currentGrade = optional(data, "currentGrade");
    const academicYear = optional(data, "academicYear");
    const gradeEffectiveFrom = optional(data, "gradeEffectiveFrom");
    if (
      [currentGrade, academicYear, gradeEffectiveFrom].some(Boolean) &&
      ![currentGrade, academicYear, gradeEffectiveFrom].every(Boolean)
    ) {
      setError("Grade, academic year, and effective date belong together.");
      return;
    }

    const birthYearText = optional(data, "birthYear");
    const input: FamilyMemberInput = {
      kind,
      role: kind === "child" ? "dependent" : (data.get("role") as "steward" | "caregiver"),
      displayName: required(data, "displayName"),
      relationship: required(data, "relationship"),
      ...arrayField(data, "aliases"),
      ...(birthYearText ? { birthYear: Number(birthYearText) } : {}),
      ...optionalField(data, "school"),
      ...optionalField(data, "currentGrade"),
      ...optionalField(data, "academicYear"),
      ...optionalField(data, "gradeEffectiveFrom"),
      ...arrayField(data, "activities"),
    };
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
          <p className="eyebrow">{member ? "Edit family member" : "Add to your family"}</p>
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

      {!member && (
        <fieldset className="segmented-control">
          <legend>Person type</legend>
          <button className={kind === "child" ? "active" : ""} type="button" onClick={() => setKind("child")}>
            Child
          </button>
          <button className={kind === "adult" ? "active" : ""} type="button" onClick={() => setKind("adult")}>
            Adult
          </button>
        </fieldset>
      )}

      <div className="form-grid">
        <Field label="Name" name="displayName" defaultValue={member?.displayName} required />
        <Field
          label="Relationship"
          name="relationship"
          defaultValue={member?.relationship}
          placeholder="Child, co-parent, caregiver…"
          required
        />
        {kind === "adult" ? (
          <label className="field">
            <span>Household role</span>
            <select name="role" defaultValue={member?.role === "caregiver" ? "caregiver" : "steward"}>
              <option value="steward">Steward</option>
              <option value="caregiver">Caregiver</option>
            </select>
          </label>
        ) : (
          <Field
            label="Birth year"
            name="birthYear"
            type="number"
            min="1900"
            max="2100"
            defaultValue={member?.birthYear?.toString()}
          />
        )}
        <Field
          label="Nicknames"
          name="aliases"
          defaultValue={member?.aliases?.join(", ")}
          placeholder="Comma separated"
        />
      </div>

      {kind === "child" && (
        <section className="editor-section">
          <div>
            <p className="eyebrow">School</p>
            <p className="section-note">Grade facts stay dated so Florence can correct them next year.</p>
          </div>
          <div className="form-grid grade-grid">
            <Field label="School or daycare" name="school" defaultValue={member?.school} />
            <Field
              label="Grade"
              name="currentGrade"
              defaultValue={member?.currentGrade}
              placeholder="3rd grade"
            />
            <Field
              label="Academic year"
              name="academicYear"
              defaultValue={member?.academicYear}
              placeholder="2026–27"
            />
            <Field
              label="Effective from"
              name="gradeEffectiveFrom"
              type="date"
              defaultValue={member?.gradeEffectiveFrom}
            />
          </div>
        </section>
      )}

      <Field
        label="Activities"
        name="activities"
        defaultValue={member?.activities?.join(", ")}
        placeholder="Soccer, piano, robotics…"
      />
      {member?.status === "verified" && (
        <p className="authority-note">
          This edits their household profile only. Their verified identity and consent remain independent.
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
          {isSaving ? "Saving…" : member ? "Save changes" : "Add family member"}
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

function optionalField(data: FormData, key: string): Record<string, string> {
  const value = optional(data, key);
  return value ? { [key]: value } : {};
}

function arrayField(data: FormData, key: string): Record<string, string[]> {
  const values = optional(data, key)
    ?.split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  return values?.length ? { [key]: values } : {};
}
