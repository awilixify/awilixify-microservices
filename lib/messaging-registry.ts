import type { AfterInit, BeforeShutdown, OnShutdown } from "awilixify";
import type {
	MessageHandler,
	MessagingAdapter,
	MessagingOptions,
} from "./adapter.js";
import type { MessageConstructor } from "./message.js";

type Registration = {
	message: MessageConstructor;
	options: unknown;
	handler: MessageHandler;
};

export class MessagingRegistry<TConnection = unknown>
	implements AfterInit, BeforeShutdown, OnShutdown
{
	private readonly adapter: MessagingAdapter<TConnection>;
	private readonly registrations: Registration[] = [];
	private readonly pending = new Set<Promise<unknown>>();
	private state: "idle" | "running" | "draining" | "stopped" | "closed" =
		"idle";

	constructor(
		private readonly messagingOptions: MessagingOptions<TConnection>,
	) {
		this.adapter = messagingOptions.adapter.create();
	}

	get connection(): TConnection {
		return this.adapter.connection;
	}

	assertTransport(message: MessageConstructor): void {
		if (message.binding.transport !== this.messagingOptions.adapter.transport) {
			throw new Error(
				`Message transport "${message.binding.transport}" does not match adapter "${this.messagingOptions.adapter.transport}"`,
			);
		}
	}

	register(
		message: MessageConstructor,
		options: unknown,
		handler: MessageHandler,
	): void {
		if (this.state !== "idle")
			throw new Error("Message registration has finished");
		this.assertTransport(message);
		this.registrations.push({ message, options, handler });
	}

	publish(
		message: MessageConstructor,
		payload: unknown,
		options: unknown,
	): Promise<unknown> {
		if (this.state !== "running" && this.state !== "draining")
			throw new Error("Messaging is not running");
		this.assertTransport(message);
		const publication = this.adapter.publish(message.binding, payload, options);
		this.pending.add(publication);
		publication.then(
			() => this.pending.delete(publication),
			() => this.pending.delete(publication),
		);
		return publication;
	}

	async afterInit(): Promise<void> {
		this.state = "running";
		try {
			for (const { message, options, handler } of this.registrations) {
				this.adapter.subscribe(
					message.binding,
					options,
					async (payload, context) => {
						await this.messagingOptions.validate?.(message, payload);
						await handler(payload, context);
					},
				);
			}
		} catch (error) {
			await this.beforeShutdown().catch(() => {});
			await this.onShutdown().catch(() => {});
			throw error;
		}
	}

	async beforeShutdown(): Promise<void> {
		if (this.state === "stopped" || this.state === "closed") return;
		this.state = "draining";
		try {
			await this.adapter.stop();
		} finally {
			while (this.pending.size) await Promise.allSettled([...this.pending]);
			this.state = "stopped";
		}
	}

	async onShutdown(): Promise<void> {
		if (this.state === "closed") return;
		this.state = "closed";
		await this.adapter.close();
	}
}
