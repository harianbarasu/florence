import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { MemberEditor } from "./MemberEditor";

describe("MemberEditor", () => {
  it("keeps a child's grade as one dated fact", async () => {
    const user = userEvent.setup();
    const onSave = vi
      .fn()
      .mockRejectedValueOnce(new Error("Temporary network failure"))
      .mockResolvedValueOnce(undefined);
    render(<MemberEditor isSaving={false} onCancel={() => undefined} onSave={onSave} />);

    await user.type(screen.getByLabelText("Name"), "Maya");
    await user.type(screen.getByLabelText("Relationship"), "Child");
    await user.type(screen.getByLabelText("Grade"), "3rd grade");
    await user.click(screen.getByRole("button", { name: "Add family member" }));

    expect(screen.getByRole("alert")).toHaveTextContent("Grade, academic year, and effective date");
    expect(onSave).not.toHaveBeenCalled();

    await user.type(screen.getByLabelText("Academic year"), "2026-27");
    await user.type(screen.getByLabelText("Effective from"), "2026-08-17");
    await user.click(screen.getByRole("button", { name: "Add family member" }));

    expect(onSave).toHaveBeenCalledOnce();
    expect(onSave.mock.calls[0]?.[1]).toMatchObject({
      kind: "child",
      role: "dependent",
      displayName: "Maya",
      currentGrade: "3rd grade",
      academicYear: "2026-27",
      gradeEffectiveFrom: "2026-08-17",
    });

    await screen.findByText("Temporary network failure");
    await user.click(screen.getByRole("button", { name: "Add family member" }));
    expect(onSave).toHaveBeenCalledTimes(2);
    expect(onSave.mock.calls[1]?.[0]).toBe(onSave.mock.calls[0]?.[0]);
    expect(onSave.mock.calls[1]?.[1]).toMatchObject({
      commandId: onSave.mock.calls[0]?.[1].commandId,
      occurredAt: onSave.mock.calls[0]?.[1].occurredAt,
    });
  });
});
