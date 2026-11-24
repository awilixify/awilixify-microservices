import {
	type AnyModule,
	createModule,
	type Module,
	type ModuleDef,
} from "awilixify";
import type { MessagingOptions } from "./adapter.js";
import { MessageInitializer } from "./message.initializer.js";
import type { MessageConstructor } from "./message.js";
import { MessagingClient } from "./messaging-client.js";
import { MessagingRegistry } from "./messaging-registry.js";

type RootDef<TConnection> = ModuleDef<{
	providers: {
		messagingOptions: MessagingOptions<TConnection>;
		messagingRegistry: MessagingRegistry<TConnection>;
	};
	exportKeys: ["messagingRegistry"];
}>;

type FeatureDef<
	TMessages extends Record<string, MessageConstructor>,
	TConnection,
	TKey extends string,
> = {
	providers: {
		publishableMessages: TMessages;
		consumableMessages: readonly MessageConstructor[];
	} & { [K in TKey]: MessagingClient<TMessages, TConnection> };
	imports: [Module<RootDef<TConnection>>];
	initializers: { onMessage: typeof MessageInitializer };
	exports: Record<TKey, MessagingClient<TMessages, TConnection>>;
	exportKeys: [TKey];
	initializerExportKeys: ["onMessage"];
};

export type MessagingModuleConfig<TTransport extends string = string> = {
	publishable: Record<
		string,
		MessageConstructor & {
			readonly binding: { readonly transport: TTransport };
		}
	>;
	consumable: readonly (MessageConstructor & {
		readonly binding: { readonly transport: TTransport };
	})[];
};

let nextId = 0;

export function createMessagingModuleFactory<
	TConnection,
	const TTransport extends string,
	const TDefaultKey extends string,
	const TKey extends string = TDefaultKey,
>(options: MessagingOptions<TConnection, TTransport, TDefaultKey, TKey>) {
	const clientKey = (options.clientKey ?? options.adapter.clientKey) as TKey;
	if (
		typeof clientKey !== "string" ||
		!clientKey.trim() ||
		[
			"publishableMessages",
			"consumableMessages",
			"messagingRegistry",
			"messagingOptions",
		].includes(clientKey)
	) {
		throw new Error("Invalid messaging clientKey");
	}
	const root = createModule<RootDef<TConnection>>({
		name: `MessagingRootModule_${++nextId}`,
		providers: {
			messagingOptions: options,
			messagingRegistry: {
				useClass: MessagingRegistry<TConnection>,
				lifetime: "SINGLETON",
				eager: true,
			},
		},
		exports: ["messagingRegistry"],
	});

	return function MessagingModule<
		const TConfig extends MessagingModuleConfig<TTransport>,
	>(config: TConfig) {
		for (const message of [
			...Object.values(config.publishable),
			...config.consumable,
		]) {
			if (message.binding.transport !== options.adapter.transport)
				throw new Error(
					`Message transport "${message.binding.transport}" does not match adapter "${options.adapter.transport}"`,
				);
		}
		const module: AnyModule = {
			name: `MessagingModule_${++nextId}`,
			imports: [root],
			providers: {
				publishableMessages: config.publishable,
				consumableMessages: [...config.consumable],
				[clientKey]: {
					useClass: MessagingClient<TConfig["publishable"], TConnection>,
					lifetime: "SINGLETON",
				},
			},
			exports: [clientKey],
			initializers: { onMessage: MessageInitializer },
			initializerExports: config.consumable.length ? ["onMessage"] : [],
		};
		// Computed provider keys widen to string; preserve the inferred client key in the public module type.
		return module as unknown as Module<
			FeatureDef<TConfig["publishable"], TConnection, TKey>
		>;
	};
}
