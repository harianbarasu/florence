import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  type CreateHouseholdInput,
  createHousehold,
  createSession,
  deleteSession,
  disconnectGoogleConnection,
  getHousehold,
  getSession,
  listGoogleConnections,
  listHouseholds,
  startGoogleConnection,
  type UpsertMemberInput,
  upsertFamilyMember,
} from "./api";

export const queryKeys = {
  session: ["session"] as const,
  households: ["households"] as const,
  household: (householdId: string) => ["households", householdId] as const,
  googleConnections: (householdId: string) => ["households", householdId, "google-connections"] as const,
};

export function useSession() {
  return useQuery({ queryKey: queryKeys.session, queryFn: getSession, retry: false });
}

export function useCreateSession() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: createSession,
    onSuccess: async () => queryClient.invalidateQueries({ queryKey: queryKeys.session }),
  });
}

export function useDeleteSession() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: deleteSession,
    onSuccess: async () => queryClient.clear(),
  });
}

export function useHouseholds() {
  return useQuery({ queryKey: queryKeys.households, queryFn: listHouseholds });
}

export function useHousehold(householdId: string | null) {
  return useQuery({
    queryKey: queryKeys.household(householdId ?? "none"),
    queryFn: () => getHousehold(householdId as string),
    enabled: householdId !== null,
  });
}

export function useGoogleConnections(householdId: string | null) {
  return useQuery({
    queryKey: queryKeys.googleConnections(householdId ?? "none"),
    queryFn: () => listGoogleConnections(householdId as string),
    enabled: householdId !== null,
  });
}

export function useStartGoogleConnection(householdId: string) {
  return useMutation({ mutationFn: () => startGoogleConnection(householdId) });
}

export function useDisconnectGoogleConnection(householdId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (connectionId: string) => disconnectGoogleConnection(householdId, connectionId),
    onSuccess: async () =>
      queryClient.invalidateQueries({ queryKey: queryKeys.googleConnections(householdId) }),
  });
}

export function useCreateHousehold() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async (input: CreateHouseholdInput) => {
      const result = await createHousehold(input);
      const households = await waitForAppliedState(listHouseholds, (profiles) =>
        profiles.some((profile) => profile.householdId === result.householdId),
      );
      queryClient.setQueryData(queryKeys.households, households);
      return result;
    },
  });
}

export function useUpsertMember(householdId: string) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: async ({ memberId, input }: { memberId: string; input: UpsertMemberInput }) => {
      const receipt = await upsertFamilyMember(householdId, memberId, input);
      const household = await waitForAppliedState(
        () => getHousehold(householdId),
        (profile) =>
          memberMatches(
            profile.members.find((member) => member.id === memberId),
            input,
          ),
      );
      queryClient.setQueryData(queryKeys.household(householdId), household);
      return receipt;
    },
  });
}

async function waitForAppliedState<T>(load: () => Promise<T>, isApplied: (value: T) => boolean): Promise<T> {
  const deadline = Date.now() + 10_000;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      const value = await load();
      if (isApplied(value)) return value;
    } catch (error) {
      lastError = error;
    }
    await new Promise((resolve) => setTimeout(resolve, 200));
  }
  throw new Error("Florence saved this change, but the household worker has not applied it yet.", {
    cause: lastError,
  });
}

function memberMatches(
  member: Awaited<ReturnType<typeof getHousehold>>["members"][number] | undefined,
  input: UpsertMemberInput,
): boolean {
  if (!member) return false;
  return (
    member.kind === input.kind &&
    member.role === input.role &&
    member.displayName === input.displayName &&
    member.relationship === input.relationship &&
    sameStrings(member.aliases, input.aliases) &&
    member.birthYear === input.birthYear &&
    member.school === input.school &&
    member.currentGrade === input.currentGrade &&
    member.academicYear === input.academicYear &&
    member.gradeEffectiveFrom === input.gradeEffectiveFrom &&
    sameStrings(member.activities, input.activities)
  );
}

function sameStrings(left: readonly string[] | undefined, right: readonly string[] | undefined): boolean {
  const first = left ?? [];
  const second = right ?? [];
  return first.length === second.length && first.every((value, index) => value === second[index]);
}
