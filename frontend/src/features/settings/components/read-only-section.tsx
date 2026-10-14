import { toast } from "sonner";

import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";

import {
	useRetryReadOnlyCleanup,
	useUpdateReadOnly,
} from "../hooks/use-settings";
import type { ReadOnlyConfig } from "../types";
import { EnvBadge, SettingsSection } from "./settings-ui";

interface ReadOnlySectionProps {
	config: ReadOnlyConfig;
}

export function ReadOnlySection({ config }: ReadOnlySectionProps) {
	const isEnv = config.source === "env";
	const mutation = useUpdateReadOnly();
	const retryMutation = useRetryReadOnlyCleanup();

	const pending = config.cleanupPending ?? [];

	const handleRetry = () => {
		retryMutation.mutate(undefined, {
			onSuccess: (result) => {
				if (result.cleanup.pending.length === 0) {
					toast.success("All open terminals and commands have been closed");
				} else {
					toast.warning(
						`${result.cleanup.pending.length} session(s) are still unresponsive and remain blocked`,
					);
				}
			},
			onError: (error: Error) => toast.error(error.message),
		});
	};

	return (
		<SettingsSection
			title="Read-only mode"
			description="Disables start, stop, restart and remove for everyone. Logs stay readable. Enabling it closes every open terminal and running command first."
			badge={isEnv && <EnvBadge />}
			action={
				<Switch
					id="read-only"
					aria-label="Enable read-only mode"
					checked={config.value}
					onCheckedChange={(checked) =>
						mutation.mutate(checked, {
							onSuccess: (result) => {
								toast.success(result.message);
								if (result.cleanup?.timedOut) {
									toast.warning(
										`Read-only is on, but ${result.cleanup.pending.length} session(s) did not stop in time and can be retried below`,
									);
								}
							},
							onError: (error: Error) => toast.error(error.message),
						})
					}
					disabled={isEnv || mutation.isPending}
					className="relative after:absolute after:-inset-x-1 after:-inset-y-3"
				/>
			}
		>
			{config.value && pending.length > 0 && (
				<div className="flex flex-wrap items-center gap-3 rounded-lg border border-amber-500/40 bg-amber-500/10 px-4 py-3">
					<p className="text-sm text-amber-700 dark:text-amber-300">
						{pending.length} terminal/command
						{pending.length === 1 ? " session" : " sessions"} could not be
						stopped (an unresponsive Docker daemon, for example). New commands
						stay blocked; retry the cleanup without leaving read-only mode.
					</p>
					<Button
						type="button"
						variant="outline"
						size="sm"
						onClick={handleRetry}
						disabled={retryMutation.isPending}
						className="ml-auto shrink-0"
					>
						{retryMutation.isPending ? "Retrying..." : "Retry cleanup"}
					</Button>
				</div>
			)}
		</SettingsSection>
	);
}
