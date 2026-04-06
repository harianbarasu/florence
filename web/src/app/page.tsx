import { auth } from "@/auth";
import { ExplorePage } from "@/components/marketing/explore-page";

export default async function HomePage() {
  const session = await auth();
  if (session?.user?.email) {
    return <ExplorePage />;
  }
  return <ExplorePage />;
}
