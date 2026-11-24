/** Type-only transport capabilities. Adapters attach these to their bindings. */
export declare const bindingTypes: unique symbol;

export type MessageBinding<
	TConsumer = unknown,
	TContext = unknown,
	TPublish = unknown,
	TResult = unknown,
> = {
	readonly transport: string;
	readonly [bindingTypes]?: {
		consumer: TConsumer;
		context: TContext;
		publish: TPublish;
		result: TResult;
	};
};

export type MessageConstructor = {
	new (payload: never): { readonly payload: unknown };
	readonly binding: MessageBinding;
};

export function defineMessage<TPayload>() {
	return <const TBinding extends MessageBinding>(binding: TBinding) =>
		class {
			static readonly binding = binding;
			constructor(public readonly payload: TPayload) {}
		};
}

export type PayloadOf<M extends MessageConstructor> =
	InstanceType<M>["payload"];
export type ConsumerOptionsOf<M extends MessageConstructor> = NonNullable<
	M["binding"][typeof bindingTypes]
>["consumer"];
export type ContextOf<M extends MessageConstructor> = NonNullable<
	M["binding"][typeof bindingTypes]
>["context"];
export type PublishOptionsOf<M extends MessageConstructor> = NonNullable<
	M["binding"][typeof bindingTypes]
>["publish"];
export type PublishResultOf<M extends MessageConstructor> = NonNullable<
	M["binding"][typeof bindingTypes]
>["result"];
