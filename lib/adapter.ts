import type { MessageBinding, MessageConstructor } from "./message.js";

export type MessageHandler = (
	payload: unknown,
	context: unknown,
) => Promise<void>;

/** Runtime boundary. Each adapter validates its own bindings and options. */
export interface MessagingAdapter<TConnection = unknown> {
	/** Native client. The integration owns its lifecycle. */
	readonly connection: TConnection;
	publish(
		binding: MessageBinding,
		payload: unknown,
		options: unknown,
	): Promise<unknown>;
	/** Register setup without waiting for a broker connection. Handle setup errors internally. */
	subscribe(
		binding: MessageBinding,
		options: unknown,
		handler: MessageHandler,
	): void;
	/** Stop delivery and await handlers, keeping publishing available until close. */
	stop(): Promise<void>;
	close(): Promise<void>;
}

export type AdapterDefinition<
	TConnection = unknown,
	TTransport extends string = string,
	TClientKey extends string = string,
> = {
	readonly transport: TTransport;
	/** Default exported dependency key for feature-module clients. */
	readonly clientKey: TClientKey;
	/** Create a native client immediately; establish the connection in the background. */
	create(): MessagingAdapter<TConnection>;
};

export type MessagingOptions<
	TConnection = unknown,
	TTransport extends string = string,
	TDefaultKey extends string = string,
	TKey extends string = TDefaultKey,
> = {
	adapter: AdapterDefinition<TConnection, TTransport, TDefaultKey>;
	/** Exported dependency name. Defaults to the adapter's clientKey. */
	clientKey?: TKey;
	/** Throw to reject an incoming payload before its handler runs. */
	validate?: (
		message: MessageConstructor,
		payload: unknown,
	) => void | Promise<void>;
};
