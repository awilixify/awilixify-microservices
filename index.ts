export type {
	AdapterDefinition,
	MessageHandler,
	MessagingAdapter,
	MessagingOptions,
} from "./lib/adapter.js";
export {
	type ConsumerOptionsOf,
	type ContextOf,
	defineMessage,
	type MessageBinding,
	type MessageConstructor,
	type PayloadOf,
	type PublishOptionsOf,
	type PublishResultOf,
} from "./lib/message.js";
export {
	createMessagingModuleFactory,
	type MessagingModuleConfig,
} from "./lib/messaging.module.js";
export { MessagingClient } from "./lib/messaging-client.js";
export { onMessage } from "./lib/on-message.decorator.js";
