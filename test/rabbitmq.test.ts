import { EventEmitter } from "node:events";
import type { Channel, ConsumeMessage } from "amqplib";
import { DIContext } from "awilixify";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
	createMessagingModuleFactory,
	type MessagingAdapter,
	onMessage,
} from "../index.js";
import { defineRabbitMessage, rabbitmq } from "../lib/rabbitmq/index.js";
import { required } from "./assert.js";

const mock = vi.hoisted(() => ({ connect: vi.fn() }));
vi.mock("amqp-connection-manager", () => ({ connect: mock.connect }));

function nativeChannel() {
	return {
		assertExchange: vi.fn(async () => ({})),
		assertQueue: vi.fn(async () => ({})),
		bindQueue: vi.fn(async () => ({})),
		prefetch: vi.fn(async () => ({})),
		consume: vi.fn(
			async (
				_queue: string,
				_handler: (message: ConsumeMessage | null) => void,
				_options: unknown,
			) => ({ consumerTag: "consumer" }),
		),
		ack: vi.fn(),
		nack: vi.fn(),
		cancel: vi.fn(async () => ({})),
	};
}
function wrapper() {
	const channel = nativeChannel();
	const setups: ((channel: Channel) => Promise<void>)[] = [];
	return Object.assign(new EventEmitter(), {
		channel,
		setups,
		waitForConnect: vi.fn(async () => {}),
		addSetup: vi.fn(async (setup: (channel: Channel) => Promise<void>) => {
			setups.push(setup);
			await setup(channel as unknown as Channel);
		}),
		publish: vi.fn(async () => true),
		close: vi.fn(async () => {}),
	});
}
function connection() {
	const wrappers: ReturnType<typeof wrapper>[] = [];
	return Object.assign(new EventEmitter(), {
		wrappers,
		connect: vi.fn(async () => {}),
		close: vi.fn(async () => {}),
		createChannel: vi.fn((_options: unknown) => {
			const result = wrapper();
			wrappers.push(result);
			return result;
		}),
	});
}
class Order extends defineRabbitMessage<{ id: string }>()({
	type: "order.created",
	exchange: { name: "orders", type: "topic" },
	routingKey: "order.created",
}) {}
let broker: ReturnType<typeof connection>;
let adapter: MessagingAdapter;
let onError: ReturnType<typeof vi.fn<(error: unknown) => void>>;

beforeEach(async () => {
	broker = connection();
	mock.connect.mockReturnValue(broker);
	onError = vi.fn();
	adapter = await rabbitmq({ urls: ["amqp://localhost"], onError }).create();
});
afterEach(async () => {
	await adapter.stop();
	await adapter.close();
	vi.restoreAllMocks();
});
function delivery(content = '{"id":"1"}') {
	return {
		content: Buffer.from(content),
		fields: { routingKey: "order.created" },
		properties: { type: "order.created" },
	} as ConsumeMessage;
}
async function subscribe(...args: Parameters<MessagingAdapter["subscribe"]>) {
	adapter.subscribe(...args);
	await required(required(broker.wrappers[1]).addSetup.mock.results[0]).value;
}

async function deliver(message: ConsumeMessage | null) {
	required(required(broker.wrappers[1]).channel.consume.mock.calls[0])[1](
		message,
	);
	await Promise.resolve();
	await Promise.resolve();
	await Promise.resolve();
}

describe("RabbitMQ adapter", () => {
	it("lazily creates a connection and uses publisher confirms", async () => {
		const definition = rabbitmq({
			urls: ["amqp://other"],
		});
		expect(definition.clientKey).toBe("rabbitMq");
		expect(mock.connect).toHaveBeenCalledTimes(1);
		expect(broker.connect).not.toHaveBeenCalled();
		expect(required(broker.wrappers[0]).waitForConnect).not.toHaveBeenCalled();
		expect(broker.createChannel).toHaveBeenCalledWith({
			confirm: true,
			publishTimeout: 10_000,
		});
		await adapter.publish(Order.binding, { id: "1" }, { expiration: "1000" });
		await adapter.publish(Order.binding, { id: "2" }, undefined);
		const publisher = required(broker.wrappers[0]);
		expect(publisher.channel.assertExchange).toHaveBeenCalledOnce();
		expect(publisher.publish).toHaveBeenCalledWith(
			"orders",
			"order.created",
			Buffer.from('{"id":"1"}'),
			{
				persistent: true,
				expiration: "1000",
				timeout: expect.any(Number),
				type: "order.created",
				contentType: "application/json",
				mandatory: false,
			},
		);
	});

	it("propagates publish failures and rejects non-JSON payloads", async () => {
		required(broker.wrappers[0]).publish.mockRejectedValueOnce(
			new Error("nack"),
		);
		await expect(
			adapter.publish(Order.binding, { id: "1" }, undefined),
		).rejects.toThrow("nack");
		await expect(
			adapter.publish(Order.binding, undefined, undefined),
		).rejects.toThrow("JSON serializable");
		const circular: { self?: unknown } = {};
		circular.self = circular;
		await expect(
			adapter.publish(Order.binding, circular, undefined),
		).rejects.toThrow();
	});

	it("creates a dedicated consumer channel and acks only after handler success", async () => {
		let finish!: () => void;
		const gate = new Promise<void>((resolve) => {
			finish = resolve;
		});
		const handler = vi.fn(async () => gate);
		await subscribe(
			Order.binding,
			{
				queueName: "billing",
				prefetch: 5,
				queueOptions: { arguments: { "x-dead-letter-exchange": "dead" } },
			},
			handler,
		);
		const channel = required(broker.wrappers[1]).channel;
		expect(channel.assertQueue).toHaveBeenCalledWith("billing", {
			durable: true,
			arguments: { "x-dead-letter-exchange": "dead" },
		});
		expect(channel.bindQueue).toHaveBeenCalledWith(
			"billing",
			"orders",
			"order.created",
			undefined,
		);
		expect(channel.prefetch).toHaveBeenCalledWith(5);
		const message = delivery();
		await deliver(message);
		expect(handler).toHaveBeenCalledWith({ id: "1" }, { message, channel });
		expect(channel.ack).not.toHaveBeenCalled();
		finish();
		await adapter.stop();
		expect(channel.cancel).toHaveBeenCalledWith("consumer");
		expect(channel.ack).toHaveBeenCalledWith(message);
		expect(required(broker.wrappers[1]).close).not.toHaveBeenCalled();
	});

	it.each([false, true])(
		"nacks handler failures with requeue=%s",
		async (requeueOnError) => {
			const error = new Error("handler failed");
			await subscribe(
				Order.binding,
				{ queueName: "billing", requeueOnError },
				async () => {
					throw error;
				},
			);
			const message = delivery();
			await deliver(message);
			const channel = required(broker.wrappers[1]).channel;
			expect(channel.nack).toHaveBeenCalledWith(message, false, requeueOnError);
			expect(channel.ack).not.toHaveBeenCalled();
			expect(onError).toHaveBeenCalledWith(error);
		},
	);

	it("rejects malformed JSON without calling the handler and ignores cancellation notifications", async () => {
		const handler = vi.fn();
		await subscribe(Order.binding, { queueName: "billing" }, handler);
		const message = delivery("not-json");
		await deliver(message);
		await deliver(null);
		expect(handler).not.toHaveBeenCalled();
		expect(required(broker.wrappers[1]).channel.nack).toHaveBeenCalledWith(
			message,
			false,
			false,
		);
	});

	it("reruns topology setup on reconnection and acknowledges on the delivery's channel", async () => {
		let finish!: () => void;
		const gate = new Promise<void>((resolve) => {
			finish = resolve;
		});
		await subscribe(Order.binding, { queueName: "billing" }, async () => gate);
		const consumer = required(broker.wrappers[1]);
		const message = delivery();
		await deliver(message);
		const reconnected = nativeChannel();
		try {
			await required(consumer.setups[0])(reconnected as unknown as Channel);
		} finally {
			finish();
		}
		await adapter.stop();
		expect(reconnected.bindQueue).toHaveBeenCalledWith(
			"billing",
			"orders",
			"order.created",
			undefined,
		);
		expect(consumer.channel.ack).toHaveBeenCalledWith(message);
		expect(reconnected.ack).not.toHaveBeenCalled();
		expect(reconnected.cancel).toHaveBeenCalledWith("consumer");
	});

	it("does not resubscribe once shutdown begins", async () => {
		await subscribe(Order.binding, { queueName: "billing" }, async () => {});
		await adapter.stop();
		const channel = nativeChannel();
		await required(required(broker.wrappers[1]).setups[0])(
			channel as unknown as Channel,
		);
		expect(channel.consume).not.toHaveBeenCalled();
		const message = delivery();
		await deliver(message);
		expect(required(broker.wrappers[1]).channel.nack).toHaveBeenCalledWith(
			message,
			false,
			true,
		);
	});

	it("rejects duplicate queues and invalid prefetch synchronously", async () => {
		await subscribe(Order.binding, { queueName: "billing" }, async () => {});
		expect(() =>
			adapter.subscribe(
				Order.binding,
				{ queueName: "billing" },
				async () => {},
			),
		).toThrow("Duplicate");
		expect(() =>
			adapter.subscribe(
				Order.binding,
				{ queueName: "other", prefetch: 0 },
				async () => {},
			),
		).toThrow("positive integer");
	});

	it("propagates topology errors", async () => {
		required(broker.wrappers[0]).channel.assertExchange.mockRejectedValueOnce(
			new Error("exchange conflict"),
		);
		await expect(adapter.publish(Order.binding, {}, undefined)).rejects.toThrow(
			"exchange conflict",
		);
	});

	it("reports background setup failures without rejecting initialization", async () => {
		const background = connection();
		const create = required(background.createChannel.getMockImplementation());
		background.createChannel.mockImplementation((options) => {
			const result = create(options);
			result.channel.assertQueue.mockRejectedValueOnce(
				new Error("queue conflict"),
			);
			return result;
		});
		mock.connect.mockReturnValueOnce(background);
		const client = rabbitmq({ urls: ["amqp://unavailable"], onError }).create();
		try {
			expect(() =>
				client.subscribe(
					Order.binding,
					{ queueName: "billing" },
					async () => {},
				),
			).not.toThrow();
			await vi.waitFor(() =>
				expect(onError).toHaveBeenCalledWith(
					expect.objectContaining({ message: "queue conflict" }),
				),
			);
		} finally {
			await client.stop();
			await client.close();
		}
	});

	it("registers setup immediately and lets later lifecycle hooks run while the broker is unavailable", async () => {
		const background = connection();
		const create = required(background.createChannel.getMockImplementation());
		background.createChannel.mockImplementation((options) => {
			const result = create(options);
			result.waitForConnect.mockImplementation(() => new Promise(() => {}));
			result.addSetup.mockImplementation(async (setup) => {
				result.setups.push(setup);
				await new Promise(() => {});
			});
			return result;
		});
		mock.connect.mockReturnValueOnce(background);
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbitmq({ urls: ["amqp://unavailable"], onError }),
		});
		class Consumer {
			@onMessage(Order, { queueName: "billing" }) handle() {}
		}
		const afterInit = vi.fn();
		const app = DIContext.create({
			name: "Background",
			imports: [MessagingModule({ publishable: {}, consumable: [Order] })],
			controllers: [Consumer],
			providers: { http: { useFactory: () => ({ afterInit }), eager: true } },
		});
		try {
			await app.init();
			expect(afterInit).toHaveBeenCalledOnce();
			expect(required(background.wrappers[1]).addSetup).toHaveBeenCalledOnce();
			expect(
				required(background.wrappers[1]).waitForConnect,
			).not.toHaveBeenCalled();
			const error = new Error("unreachable");
			background.emit("connectFailed", { err: error });
			expect(onError).toHaveBeenCalledWith(error);
		} finally {
			await app.close();
		}
	});

	it("bounds publication topology setup and does not send after that timeout", async () => {
		let finish!: () => void;
		const publisher = required(broker.wrappers[0]);
		publisher.addSetup.mockImplementationOnce(
			() =>
				new Promise<void>((resolve) => {
					finish = resolve;
				}),
		);
		await expect(
			adapter.publish(Order.binding, { id: "1" }, { timeout: 5 }),
		).rejects.toThrow("publish timed out");
		finish();
		await Promise.resolve();
		await Promise.resolve();
		expect(publisher.publish).not.toHaveBeenCalled();
	});
});
