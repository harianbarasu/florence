import type {
  CompleteFamilyOnboardingInput,
  FamilyMemberInput,
  PatchFactInput,
  PreferencesInput,
  SetupSessionInput,
} from "@florence/contracts";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  completeFamilyOnboarding,
  createSession,
  deleteSession,
  deleteVaultFact,
  disconnectGoogleConnection,
  getSession,
  getWorkspace,
  patchVaultFact,
  putFamilyMember,
  putPreferences,
  startGoogleConnection,
} from "./api";

export const queryKeys = {
  session: ["session"] as const,
  workspace: ["workspace"] as const,
};

export function useSession(enabled = true) {
  return useQuery({ queryKey: queryKeys.session, queryFn: getSession, retry: false, enabled });
}

export function useCreateSession() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (input: SetupSessionInput) => createSession(input),
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

export function useCompleteFamilyOnboarding() {
  return useMutation({
    mutationFn: (input: CompleteFamilyOnboardingInput) => completeFamilyOnboarding(input),
  });
}

export function usePutMember() {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ memberId, input }: { memberId: string; input: FamilyMemberInput }) =>
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
    onSuccess: (workspace) => queryClient.setQueryData(queryKeys.workspace, workspace),
  });
}
