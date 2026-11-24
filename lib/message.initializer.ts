import { Initializer, type InitializerContext } from "awilixify";
import type { MessageConstructor } from "./message.js";
import type { MessagingRegistry } from "./messaging-registry.js";
import { ON_MESSAGE_TOKEN } from "./on-message.decorator.js";

export class MessageInitializer extends Initializer<typeof ON_MESSAGE_TOKEN> {
	readonly token = ON_MESSAGE_TOKEN;
	constructor(
		private readonly messagingRegistry: MessagingRegistry,
		private readonly consumableMessages: readonly MessageConstructor[],
	) {
		super();
	}

	initialize(context: InitializerContext<typeof ON_MESSAGE_TOKEN>): void {
		const metadata = context.metadata;
		if (!metadata) return;
		if (!this.consumableMessages.includes(metadata.message)) {
			throw new Error(
				`Message is not consumable in module "${context.moduleName}"`,
			);
		}
		this.messagingRegistry.register(
			metadata.message,
			metadata.options,
			async (payload, messageContext) => {
				await context.invoke(payload, messageContext);
			},
		);
	}
}
