import { auth } from "@/auth";
import { GoogleSignInCard } from "@/components/auth/google-sign-in-card";
import { ChatScreen } from "@/components/chat/chat-screen";

export default async function ChatPage() {
  const session = await auth();
  if (!session?.user?.email) {
    return <GoogleSignInCard redirectTo="/chat" />;
  }

  return <ChatScreen />;
}
