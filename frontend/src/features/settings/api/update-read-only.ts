import { authenticatedFetch, readJson } from "@/lib/api-client";
import { API_BASE_URL } from "@/types/api";

import type { ReadOnlyCleanupReport } from "../types";

const ENDPOINT = `${API_BASE_URL}/api/v1/settings/read-only`;

export interface UpdateReadOnlyResult {
	message: string;
	readOnly: boolean;
	/** Present when enabling: how the exec-session cleanup went. */
	cleanup?: ReadOnlyCleanupReport;
}

export async function updateReadOnly(value: boolean): Promise<UpdateReadOnlyResult> {
	const response = await authenticatedFetch(ENDPOINT, {
		method: "PUT",
		headers: { "Content-Type": "application/json" },
		body: JSON.stringify({ value }),
	});

	if (!response.ok) {
		const message = await response.text();
		throw new Error(message || "Failed to update read-only mode");
	}

	return readJson<UpdateReadOnlyResult>(response);
}
