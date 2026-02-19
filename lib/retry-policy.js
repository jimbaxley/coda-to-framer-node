export function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function parseIntEnv(value, fallback, min, max) {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.max(min, Math.min(Math.trunc(parsed), max));
}

export function computeBackoffMs(baseDelayMs, attempt, maxDelayMs = 10000) {
  const safeBase = Math.max(0, baseDelayMs);
  const raw = safeBase * Math.max(1, attempt);
  const jitter = Math.floor(Math.random() * Math.max(1, Math.floor(safeBase * 0.2)));
  return Math.min(raw + jitter, Math.max(0, maxDelayMs));
}

export function isRetryableHttpStatus(status) {
  return status === 408 || status === 409 || status === 425 || status === 429 || (status >= 500 && status <= 599);
}

function getErrorCode(error) {
  if (!error || typeof error !== "object") return "";
  const maybeError = error;
  return String(maybeError.code || "").toUpperCase();
}

function getErrorMessage(error) {
  if (!error) return "";
  if (error instanceof Error) return String(error.message || "");
  return String(error || "");
}

export function isLikelyNetworkError(error) {
  const code = getErrorCode(error);
  if (["ECONNRESET", "ETIMEDOUT", "EAI_AGAIN", "ENOTFOUND", "ECONNREFUSED", "UND_ERR_CONNECT_TIMEOUT", "UND_ERR_HEADERS_TIMEOUT", "UND_ERR_SOCKET"].includes(code)) {
    return true;
  }

  const message = getErrorMessage(error).toLowerCase();
  return (
    message.includes("socket hang up") ||
    message.includes("network") ||
    message.includes("fetch failed") ||
    message.includes("connection timed out") ||
    message.includes("upstream request timeout")
  );
}

function getStatusFromError(error) {
  if (!error || typeof error !== "object") return null;
  const maybeError = error;
  if (Number.isFinite(maybeError.status)) return Number(maybeError.status);
  if (Number.isFinite(maybeError.statusCode)) return Number(maybeError.statusCode);

  const message = getErrorMessage(error);
  const match = message.match(/\b(408|409|425|429|5\d\d)\b/);
  if (match) return Number(match[1]);
  return null;
}

export function isRetryableCodaError(error) {
  const status = getStatusFromError(error);
  if (status !== null && isRetryableHttpStatus(status)) return true;
  return isLikelyNetworkError(error);
}

export function isRetryableFramerError(error) {
  const code = getErrorCode(error);
  const message = getErrorMessage(error).toLowerCase();

  if (code === "PROJECT_CLOSED" || message.includes("session expired")) {
    return true;
  }

  const status = getStatusFromError(error);
  if (status !== null && isRetryableHttpStatus(status)) return true;

  if (message.includes("operation timed out") || message.includes("publishproject")) {
    return true;
  }

  return isLikelyNetworkError(error);
}

function isTransientMissingSlugWarning(warning) {
  if (typeof warning !== "string") return false;
  if (!warning.includes("slug extraction failed")) return false;

  return (
    warning.includes('slugFieldValue=""') ||
    warning.includes("slugFieldValue=null") ||
    warning.includes("slugFieldValue=undefined") ||
    warning.includes('"value":""')
  );
}

export function shouldRetryForTransientCodaWarnings(warnings) {
  if (!Array.isArray(warnings) || warnings.length === 0) return false;
  return warnings.some(isTransientMissingSlugWarning);
}