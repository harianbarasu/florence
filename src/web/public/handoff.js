(() => {
  const root = document.querySelector("[data-handoff-token]");
  const form = document.querySelector("form");
  const button = document.querySelector("button");
  const status = document.querySelector("[data-status]");
  const token = root?.getAttribute("data-handoff-token");
  const purpose = root?.getAttribute("data-handoff-purpose");
  history.replaceState({}, "", "/sign-in");
  if (!form || !button || !status || !token) return;
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    button.disabled = true;
    status.textContent = purpose === "google_connect" ? "Opening Google…" : "Opening Florence…";
    try {
      const response = await fetch("/auth/consume", {
        method: "POST",
        credentials: "same-origin",
        headers: { "Content-Type": "application/json", Accept: "application/json" },
        body: JSON.stringify({ token }),
      });
      const body = await response.json();
      if (!response.ok) throw new Error(body.error || "This link could not be used.");
      location.replace(body.redirect || "/home");
    } catch (error) {
      status.textContent = error instanceof Error ? error.message : "This link could not be used.";
      button.disabled = false;
    }
  });
})();
