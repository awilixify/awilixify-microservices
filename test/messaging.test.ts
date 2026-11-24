import { EventEmitter } from "node:events";
import { createModule, DIContext, type ModuleDef } from "awilixify";
import { afterEach, describe, expect, it, vi } from "vitest";
import {
	createMessagingModuleFactory,
	defineMessage,
	type MessageBinding,
	type MessagingAdapter,
	type MessagingClient,
	onMessage,
	type PayloadOf,
} from "../index.js";
import { MessagingRegistry } from "../lib/messaging-registry.js";
import { defineRabbitMessage } from "../lib/rabbitmq/index.js";
import { required } from "./assert.js";

class OrderCreated extends defineRabbitMessage<{ id: string }>()({
	type: "order.created",
	exchange: { name: "orders", type: "topic" },
	routingKey: "order.created",
}) {}
class PaymentReceived extends defineRabbitMessage<{ id: string }>()({
	type: "payment.received",
	exchange: { name: "payments", type: "topic" },
	routingKey: "payment.received",
}) {}
class KafkaOrder extends defineMessage<{ id: string }>()({
	transport: "kafka",
} as MessageBinding<
	{ groupId: string },
	{ partition: number },
	{ key: string },
	{ offset: string }
> & { transport: "kafka" }) {}

function fakeAdapter<
	const TTransport extends string = "rabbitmq",
	const TKey extends string = "rabbitMq",
>(
	transport: TTransport = "rabbitmq" as TTransport,
	clientKey: TKey = "rabbitMq" as TKey,
) {
	const subscriptions: {
		binding: MessageBinding;
		options: unknown;
		handler: (payload: unknown, context: unknown) => Promise<void>;
	}[] = [];
	const connection = Object.assign(new EventEmitter(), {
		isConnected: vi.fn(() => false),
	});
	const adapter: MessagingAdapter<typeof connection> = {
		connection,
		publish: vi.fn(async () => undefined),
		subscribe: vi.fn((binding, options, handler) => {
			subscriptions.push({ binding, options, handler });
		}),
		stop: vi.fn(async () => {}),
		close: vi.fn(async () => {}),
	};
	return {
		adapter,
		subscriptions,
		definition: { transport, clientKey, create: vi.fn(() => adapter) },
	};
}
const apps: ReturnType<typeof DIContext.create>[] = [];
function app(config: Omit<Parameters<typeof DIContext.create>[0], "name">) {
	const application = DIContext.create({ name: "Test", ...config });
	apps.push(application);
	return application;
}
afterEach(async () => {
	await Promise.allSettled(
		apps.splice(0).map((application) => application.close()),
	);
});

describe("factory and feature modules", () => {
	it("uses the adapter's default client key and exposes its native connection", async () => {
		const rabbit = fakeAdapter();
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbit.definition,
		});
		const feature = MessagingModule({
			publishable: { OrderCreated },
			consumable: [OrderCreated],
		});
		const received = vi.fn();
		class Controller {
			@onMessage(OrderCreated, { queueName: "billing" })
			handle(payload: PayloadOf<typeof OrderCreated>) {
				received(payload);
			}
		}
		const application = app({ imports: [feature], controllers: [Controller] });
		await application.init();
		const client =
			application.scope.resolve<
				ModuleDef<{ imports: [typeof feature] }>["deps"]["rabbitMq"]
			>("rabbitMq");
		expect(client.connection).toBe(rabbit.adapter.connection);
		expect(client.connection.isConnected()).toBe(false);
		const disconnected = vi.fn();
		client.connection.on("disconnect", disconnected);
		rabbit.adapter.connection.emit("disconnect", { err: new Error("lost") });
		expect(disconnected).toHaveBeenCalledOnce();
		await client.publish(OrderCreated, { id: "1" }, { persistent: true });
		expect(rabbit.adapter.publish).toHaveBeenCalledWith(
			OrderCreated.binding,
			{ id: "1" },
			{ persistent: true },
		);
		await required(rabbit.subscriptions[0]).handler({ id: "2" }, {});
		expect(received).toHaveBeenCalledWith({ id: "2" });
		await application.close();
		expect(rabbit.adapter.stop).toHaveBeenCalledOnce();
		expect(rabbit.adapter.close).toHaveBeenCalledOnce();
		expect(() => client.publish(OrderCreated, { id: "3" })).toThrow(
			"not running",
		);
	});

	it("allows differently keyed clients for separate brokers in the same module", async () => {
		const orders = fakeAdapter();
		const analytics = fakeAdapter();
		const Orders = createMessagingModuleFactory({ adapter: orders.definition });
		const Analytics = createMessagingModuleFactory({
			adapter: analytics.definition,
			clientKey: "analyticsMessaging",
		});
		const application = app({
			imports: [
				Orders({ publishable: { OrderCreated }, consumable: [] }),
				Analytics({ publishable: { OrderCreated }, consumable: [] }),
			],
		});
		await application.init();
		const first =
			application.scope.resolve<
				MessagingClient<{ OrderCreated: typeof OrderCreated }>
			>("rabbitMq");
		const second =
			application.scope.resolve<
				MessagingClient<{ OrderCreated: typeof OrderCreated }>
			>("analyticsMessaging");
		expect(first.connection).toBe(orders.adapter.connection);
		expect(second.connection).toBe(analytics.adapter.connection);
		await first.publish(OrderCreated, { id: "1" });
		await second.publish(OrderCreated, { id: "2" });
		expect(orders.adapter.publish).toHaveBeenCalledWith(
			OrderCreated.binding,
			{ id: "1" },
			undefined,
		);
		expect(analytics.adapter.publish).toHaveBeenCalledWith(
			OrderCreated.binding,
			{ id: "2" },
			undefined,
		);
	});

	it("runs different transports through separate feature modules", async () => {
		const rabbit = fakeAdapter();
		const kafka = fakeAdapter("kafka", "kafka");
		const Rabbit = createMessagingModuleFactory({ adapter: rabbit.definition });
		const Kafka = createMessagingModuleFactory({ adapter: kafka.definition });
		class RabbitController {
			@onMessage(OrderCreated, { queueName: "billing" }) handle() {}
		}
		class KafkaController {
			@onMessage(KafkaOrder, { groupId: "billing" }) handle() {}
		}
		const rabbitFeature = Rabbit({
			publishable: {},
			consumable: [OrderCreated],
		});
		const kafkaFeature = Kafka({ publishable: {}, consumable: [KafkaOrder] });
		const RabbitFeature = createModule<
			ModuleDef<{ imports: [typeof rabbitFeature] }>
		>({
			name: "RabbitFeature",
			imports: [rabbitFeature],
			controllers: [RabbitController],
		});
		const KafkaFeature = createModule<
			ModuleDef<{ imports: [typeof kafkaFeature] }>
		>({
			name: "KafkaFeature",
			imports: [kafkaFeature],
			controllers: [KafkaController],
		});
		const application = app({ imports: [RabbitFeature, KafkaFeature] });
		await application.init();
		expect(rabbit.subscriptions).toHaveLength(1);
		expect(kafka.subscriptions).toHaveLength(1);
	});

	it("shares one root between feature modules but keeps publishers restricted", async () => {
		const rabbit = fakeAdapter();
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbit.definition,
		});
		const clients: MessagingClient<
			Record<string, typeof OrderCreated | typeof PaymentReceived>
		>[] = [];
		function feature(message: typeof OrderCreated | typeof PaymentReceived) {
			class Capture {
				constructor(rabbitMq: MessagingClient<Record<string, typeof message>>) {
					clients.push(rabbitMq);
				}
			}
			const messagingModule = MessagingModule({
				publishable: { message },
				consumable: [],
			});
			return createModule<
				ModuleDef<{
					imports: [typeof messagingModule];
					providers: { capture: Capture };
				}>
			>({
				name: message.binding.type,
				imports: [messagingModule],
				providers: { capture: { useClass: Capture, eager: true } },
			});
		}
		const application = app({
			imports: [feature(OrderCreated), feature(PaymentReceived)],
		});
		await application.init();
		expect(rabbit.definition.create).toHaveBeenCalledOnce();
		expect(clients).toHaveLength(2);
		const client = required(
			clients.find((entry) => entry.messages.message === OrderCreated),
		);
		expect(() => client.publish(PaymentReceived, { id: "1" })).toThrow(
			"not publishable",
		);
		class Impostor extends OrderCreated {}
		expect(() => client.publish(Impostor, { id: "1" })).toThrow(
			"not publishable",
		);
		await client.publish(OrderCreated, { id: "1" });
	});

	it("makes the native connection available during onInit", async () => {
		const rabbit = fakeAdapter();
		const observed = vi.fn();
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbit.definition,
		});
		class Observer {
			constructor(
				private readonly rabbitMq: MessagingClient<
					Record<never, never>,
					typeof rabbit.adapter.connection
				>,
			) {}
			onInit() {
				this.rabbitMq.connection.on("error", observed);
			}
		}
		const feature = MessagingModule({ publishable: {}, consumable: [] });
		const application = app({
			imports: [feature],
			providers: { observer: { useClass: Observer, eager: true } },
		});
		await application.init();
		rabbit.adapter.connection.emit("error", new Error("failed"));
		expect(observed).toHaveBeenCalledOnce();
	});

	it("resolves scoped controllers separately for each delivery", async () => {
		const rabbit = fakeAdapter();
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbit.definition,
		});
		let nextId = 0;
		const instances: number[] = [];
		class Consumer {
			readonly id = ++nextId;
			@onMessage(OrderCreated, { queueName: "billing" }) handle() {
				instances.push(this.id);
			}
		}
		const application = app({
			imports: [
				MessagingModule({ publishable: {}, consumable: [OrderCreated] }),
			],
			controllers: [{ useClass: Consumer, lifetime: "SCOPED" }],
		});
		await application.init();
		const subscription = required(rabbit.subscriptions[0]);
		await subscription.handler({ id: "1" }, {});
		await subscription.handler({ id: "2" }, {});
		expect(instances).toEqual([1, 2]);
	});

	it("rejects handlers outside their consumable scope", async () => {
		const rabbit = fakeAdapter();
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbit.definition,
		});
		class Controller {
			@onMessage(OrderCreated, { queueName: "billing" }) handle() {}
		}
		const application = app({
			imports: [
				MessagingModule({ publishable: {}, consumable: [PaymentReceived] }),
			],
			controllers: [Controller],
		});
		await expect(application.init()).rejects.toThrow("not consumable");
	});

	it("creates a separate adapter for each application using the same factory", async () => {
		const rabbit = fakeAdapter();
		rabbit.definition.create.mockImplementation(() => fakeAdapter().adapter);
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbit.definition,
		});
		const feature = MessagingModule({
			publishable: { OrderCreated },
			consumable: [],
		});
		const first = app({ imports: [feature] });
		const second = app({ imports: [feature] });
		await first.init();
		await second.init();
		expect(rabbit.definition.create).toHaveBeenCalledTimes(2);
		await first.close();
		expect(
			required(rabbit.definition.create.mock.results[0]).value.close,
		).toHaveBeenCalledOnce();
		expect(
			required(rabbit.definition.create.mock.results[1]).value.close,
		).not.toHaveBeenCalled();
	});

	it("rejects transport mismatches at runtime as well as in types", () => {
		const rabbit = fakeAdapter();
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbit.definition,
		});
		expect(() =>
			MessagingModule({
				publishable: { KafkaOrder: KafkaOrder as never },
				consumable: [],
			}),
		).toThrow("does not match");
		expect(() =>
			MessagingModule({ publishable: {}, consumable: [KafkaOrder as never] }),
		).toThrow("does not match");
	});
});

describe("registry lifecycle", () => {
	it("validates payloads before invoking handlers", async () => {
		const rabbit = fakeAdapter();
		const handler = vi.fn();
		const registry = new MessagingRegistry({
			adapter: rabbit.definition,
			validate: () => {
				throw new Error("invalid");
			},
		});
		registry.register(OrderCreated, {}, handler);
		await registry.afterInit();
		await expect(
			required(rabbit.subscriptions[0]).handler({}, {}),
		).rejects.toThrow("invalid");
		expect(handler).not.toHaveBeenCalled();
		await registry.beforeShutdown();
		await registry.onShutdown();
	});

	it("cleans up after a synchronous consumer configuration error", async () => {
		const rabbit = fakeAdapter();
		vi.mocked(rabbit.adapter.subscribe).mockImplementation(() => {
			throw new Error("invalid configuration");
		});
		const registry = new MessagingRegistry({ adapter: rabbit.definition });
		registry.register(OrderCreated, {}, async () => {});
		await expect(registry.afterInit()).rejects.toThrow("invalid configuration");
		expect(rabbit.adapter.stop).toHaveBeenCalledOnce();
		expect(rabbit.adapter.close).toHaveBeenCalledOnce();
	});

	it("allows a draining handler to publish and waits before closing", async () => {
		const rabbit = fakeAdapter();
		let release!: () => void;
		const gate = new Promise<void>((resolve) => {
			release = resolve;
		});
		const registry = new MessagingRegistry({ adapter: rabbit.definition });
		vi.mocked(rabbit.adapter.stop).mockImplementation(async () => {
			await gate;
			await registry.publish(OrderCreated, { id: "last" }, undefined);
		});
		await registry.afterInit();
		const stopping = registry.beforeShutdown();
		expect(rabbit.adapter.close).not.toHaveBeenCalled();
		release();
		await stopping;
		await registry.onShutdown();
		expect(rabbit.adapter.publish).toHaveBeenCalledOnce();
		expect(rabbit.adapter.close).toHaveBeenCalledOnce();
	});
});
