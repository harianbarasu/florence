import type {
  CompleteFamilyOnboardingInput,
  FamilyMemberMutationInput,
  PatchFactInput,
  PatchWatchInput,
  PreferencesInput,
  SessionInput,
} from "@florence/contracts";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  completeFamilyOnboarding,
  createSession,
  deleteGoogleDerivedData,
  deleteSession,
  deleteVaultFact,
  deleteVaultWatch,
  disconnectGoogleConnection,
  getFamilyCalendarMonth,
  getSession,
  getWorkspace,
  patchVaultFact,
  patchVaultWatch,
  putFamilyMember,
  putPreferences,
  startGoogleConnection,
} from "./api";

export const queryKeys = {
  session: ["session"] as const,
  workspace: ["workspace"] as const,
  calendar: (month: string) => ["calendar", month] as const,
};

export function useSession(enabled = true) {
  return useQuery({ queryKey: queryKeys.session, queryFn: getSession, retry: false, enabled });
}

export function useCreateSession() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: SessionInput) => createSession(input),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: queryKeys.session });
      await queryClient.invalidateQueries({ queryKey: queryKeys.workspace });
    },
  });
}

export function useDeleteSession() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: deleteSession,
    onSuccess: async () => queryClient.clear(),
  });
}

export function useWorkspace(enabled = true) {
  return useQuery({ queryKey: queryKeys.workspace, queryFn: getWorkspace, enabled });
}

export function useFamilyCalendarMonth(month: string) {
  return useQuery({
    queryKey: queryKeys.calendar(month),
    queryFn: () => getFamilyCalendarMonth(month),
    retry: false,
  });
}

export function useCompleteFamilyOnboarding() {
  return useMutation({
    mutationFn: (input: CompleteFamilyOnboardingInput) => completeFamilyOnboarding(input),
  });
}

export function usePutMember() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ memberId, input }: { memberId: string; input: FamilyMemberMutationInput }) =>
      putFamilyMember(memberId, input),
    onSuccess: (workspace) => queryClient.setQueryData(queryKeys.workspace, workspace),
  });
}

export function usePatchFact() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ factId, input }: { factId: string; input: PatchFactInput }) =>
      patchVaultFact(factId, input),
    onSuccess: (workspace) => queryClient.setQueryData(queryKeys.workspace, workspace),
  });
}

export function useDeleteFact() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (factId: string) => deleteVaultFact(factId),
    onSuccess: (workspace) => queryClient.setQueryData(queryKeys.workspace, workspace),
  });
}

export function usePatchWatch() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ workId, input }: { workId: string; input: PatchWatchInput }) =>
      patchVaultWatch(workId, input),
    onSuccess: (workspace) => queryClient.setQueryData(queryKeys.workspace, workspace),
  });
}

export function useDeleteWatch() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (workId: string) => deleteVaultWatch(workId),
    onSuccess: (workspace) => queryClient.setQueryData(queryKeys.workspace, workspace),
  });
}

export function usePutPreferences() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: PreferencesInput) => putPreferences(input),
    onSuccess: (workspace) => queryClient.setQueryData(queryKeys.workspace, workspace),
  });
}

export function useStartGoogleConnection() {
  return useMutation({ mutationFn: startGoogleConnection });
}

export function useDisconnectGoogleConnection() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (connectionId: string) => disconnectGoogleConnection(connectionId),
    onSuccess: (result) => queryClient.setQueryData(queryKeys.workspace, result.workspace),
  });
}

export function useDeleteGoogleDerivedData() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: deleteGoogleDerivedData,
    onSuccess: (result) => queryClient.setQueryData(queryKeys.workspace, result.workspace),
  });
}
