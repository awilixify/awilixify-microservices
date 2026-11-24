import type {
	MessageConstructor,
	PayloadOf,
	PublishOptionsOf,
	PublishResultOf,
} from "./message.js";
import type { MessagingRegistry } from "./messaging-registry.js";

export class MessagingClient<
	TMessages extends Record<string, MessageConstructor>,
	TConnection = unknown,
> {
	readonly messages: Readonly<TMessages>;
	private readonly allowed: ReadonlySet<MessageConstructor>;
	constructor(
		private readonly messagingRegistry: MessagingRegistry<TConnection>,
		publishableMessages: TMessages,
	) {
		this.messages = Object.freeze({ ...publishableMessages });
		this.allowed = new Set(Object.values(publishableMessages));
	}

	/** Native connection for status/events and unmanaged operations. Do not close it yourself. */
	get connection(): TConnection {
		return this.messagingRegistry.connection;
	}

	publish<M extends TMessages[keyof TMessages]>(
		message: M,
		payload: PayloadOf<NoInfer<M>>,
		options?: PublishOptionsOf<NoInfer<M>>,
	): Promise<PublishResultOf<M>> {
		if (!this.allowed.has(message))
			throw new Error("Message is not publishable in this module");
		return this.messagingRegistry.publish(message, payload, options) as Promise<
			PublishResultOf<M>
		>;
	}
}
