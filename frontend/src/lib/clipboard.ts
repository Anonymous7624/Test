/**
 * Clipboard write with graceful fallback (non-secure contexts, missing API, denied permission).
 */
export async function copyTextSafe(text: string): Promise<{ ok: boolean; message: string }> {
  try {
    if (typeof navigator === "undefined" || typeof navigator.clipboard?.writeText !== "function") {
      return {
        ok: false,
        message: "Clipboard is not available in this browser or context (try HTTPS or localhost).",
      };
    }
    await navigator.clipboard.writeText(text);
    return { ok: true, message: "Copied to clipboard." };
  } catch {
    return {
      ok: false,
      message: "Could not copy automatically — select the text and copy it manually.",
    };
  }
}
