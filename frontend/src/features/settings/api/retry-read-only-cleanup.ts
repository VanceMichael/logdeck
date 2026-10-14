import { authenticatedFetch, readJson } from "@/lib/api-client";
import { API_BASE_URL } from "@/types/api";

import type { ReadOnlyCleanupReport } from "../types";

const ENDPOINT = `${API_BASE_URL}/api/v1/settings/read-only/cleanup`;

export interface RetryCleanupResult {
	readOnly: boolean;
	cleanup: ReadOnlyCleanupReport;
}

/** Re-attempts tearing down exec sessions left over by a read-only switch. */
export async function retryReadOnlyCleanup(): Promise<RetryCleanupResult> {
	const response = await authenticatedFetch(ENDPOINT, { method: "POST" });

	if (!response.ok) {
		const message = await response.text();
		throw new Error(message || "Failed to retry cleanup");
	}

	return readJson<RetryCleanupResult>(response);
}
