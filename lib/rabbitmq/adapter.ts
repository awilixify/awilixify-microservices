import {
	type AmqpConnectionManager,
	type AmqpConnectionManagerOptions,
	type ChannelWrapper,
	type ConnectionUrl,
	connect,
} from "amqp-connection-manager";
import type { Channel, ConsumeMessage } from "amqplib";
import type {
	AdapterDefinition,
	MessageHandler,
	MessagingAdapter,
} from "../adapter.js";
import type { MessageBinding } from "../message.js";
import type {
	RabbitBinding,
	RabbitConsumerOptions,
	RabbitPublishOptions,
} from "./message.js";

export type RabbitMqOptions = {
	urls: ConnectionUrl | ConnectionUrl[];
	connectionOptions?: AmqpConnectionManagerOptions;
	/** Bounds each publication, including exchange setup. Defaults to 10 seconds. */
	publishTimeoutMs?: number;
	/** Error observer. Throwing from it never changes delivery handling. */
	onError?: (error: unknown) => void;
};

type Subscription = {
	wrapper: ChannelWrapper;
	channel?: Channel;
	consumerTag?: string;
};

export function rabbitmq(
	options: RabbitMqOptions,
): AdapterDefinition<AmqpConnectionManager, "rabbitmq", "rabbitMq"> {
	if (!options.urls || (Array.isArray(options.urls) && !options.urls.length))
		throw new Error("RabbitMQ requires at least one URL");
	if (
		options.publishTimeoutMs !== undefined &&
		(!Number.isFinite(options.publishTimeoutMs) ||
			options.publishTimeoutMs <= 0)
	) {
		throw new Error("RabbitMQ publishTimeoutMs must be positive");
	}
	return {
		transport: "rabbitmq",
		clientKey: "rabbitMq",
		create() {
			const connection = connect(options.urls, options.connectionOptions);
			try {
				return new RabbitAdapter(connection, options);
			} catch (error) {
				void connection
					.close()
					.catch((cleanupError) => report(options, cleanupError));
				throw error;
			}
		},
	};
}

class RabbitAdapter implements MessagingAdapter<AmqpConnectionManager> {
	private readonly publisher: ChannelWrapper;
	private readonly exchanges = new Map<string, Promise<void>>();
	private readonly subscriptions: Subscription[] = [];
	private readonly queues = new Set<string>();
	private readonly pending = new Set<Promise<void>>();
	private stopping = false;

	constructor(
		readonly connection: AmqpConnectionManager,
		private readonly options: RabbitMqOptions,
	) {
		this.publisher = connection.createChannel({
			confirm: true,
			publishTimeout: options.publishTimeoutMs ?? 10_000,
		});
		this.publisher.on("error", (error) => report(options, error));
		connection.on("error", (error) => report(options, error));
		connection.on("connectFailed", ({ err }) => report(options, err));
		connection.on("disconnect", ({ err }) => {
			if (err) report(options, err);
		});
	}

	async publish(
		binding: MessageBinding,
		payload: unknown,
		options: unknown,
	): Promise<void> {
		const rabbit = this.binding(binding);
		const publishOptions = options as RabbitPublishOptions | undefined;
		const timeout =
			publishOptions?.timeout ?? this.options.publishTimeoutMs ?? 10_000;
		if (!Number.isFinite(timeout) || timeout <= 0)
			throw new Error("RabbitMQ publish timeout must be positive");
		const json = JSON.stringify(payload);
		if (json === undefined)
			throw new Error("RabbitMQ payload must be JSON serializable");
		const started = Date.now();
		await this.withPublishTimeout(this.ensureExchange(rabbit), timeout);
		await this.publisher.publish(
			rabbit.exchange.name,
			rabbit.routingKey,
			Buffer.from(json),
			{
				persistent: true,
				...publishOptions,
				timeout: Math.max(1, timeout - (Date.now() - started)),
				type: rabbit.type,
				contentType: "application/json",
				mandatory: false,
			},
		);
	}

	subscribe(
		binding: MessageBinding,
		options: unknown,
		handler: MessageHandler,
	): void {
		const rabbit = this.binding(binding);
		const consumer = options as RabbitConsumerOptions;
		if (!consumer?.queueName)
			throw new Error("RabbitMQ consumer requires queueName");
		if (this.queues.has(consumer.queueName))
			throw new Error(
				`Duplicate RabbitMQ consumer queue "${consumer.queueName}"`,
			);
		if (
			consumer.prefetch !== undefined &&
			(!Number.isInteger(consumer.prefetch) || consumer.prefetch < 1)
		) {
			throw new Error("RabbitMQ prefetch must be a positive integer");
		}
		this.queues.add(consumer.queueName);
		const wrapper = this.connection.createChannel({ confirm: false });
		wrapper.on("error", (error) => report(this.options, error));
		const subscription: Subscription = { wrapper };
		this.subscriptions.push(subscription);
		void wrapper
			.addSetup(async (channel: Channel) => {
				if (this.stopping) return;
				await this.assertExchange(channel, rabbit);
				await channel.assertQueue(consumer.queueName, {
					durable: true,
					...consumer.queueOptions,
				});
				await channel.bindQueue(
					consumer.queueName,
					rabbit.exchange.name,
					rabbit.routingKey,
					consumer.bindingArguments,
				);
				await channel.prefetch(consumer.prefetch ?? 1);
				if (this.stopping) return;
				const result = await channel.consume(
					consumer.queueName,
					(message) => {
						if (!message) return;
						const task = this.deliver(channel, message, consumer, handler);
						this.pending.add(task);
						void task.then(() => this.pending.delete(task));
					},
					{ ...consumer.consumeOptions, noAck: false },
				);
				subscription.channel = channel;
				subscription.consumerTag = result.consumerTag;
				if (this.stopping) await channel.cancel(result.consumerTag);
			})
			.catch((error) => report(this.options, error));
	}

	private async deliver(
		channel: Channel,
		message: ConsumeMessage,
		options: RabbitConsumerOptions,
		handler: MessageHandler,
	): Promise<void> {
		if (this.stopping) {
			try {
				channel.nack(message, false, true);
			} catch (error) {
				report(this.options, error);
			}
			return;
		}
		try {
			const payload: unknown = JSON.parse(message.content.toString("utf8"));
			await handler(payload, { message, channel });
			channel.ack(message);
		} catch (error) {
			try {
				channel.nack(message, false, options.requeueOnError ?? false);
			} catch (ackError) {
				report(this.options, ackError);
			}
			report(this.options, error);
		}
	}

	async stop(): Promise<void> {
		this.stopping = true;
		const results = await Promise.allSettled(
			this.subscriptions.map(async ({ channel, consumerTag }) => {
				if (channel && consumerTag) await channel.cancel(consumerTag);
			}),
		);
		while (this.pending.size) await Promise.allSettled([...this.pending]);
		for (const result of results)
			if (result.status === "rejected") report(this.options, result.reason);
	}

	async close(): Promise<void> {
		this.stopping = true;
		const results = await Promise.allSettled([
			this.publisher.close(),
			...this.subscriptions.map(({ wrapper }) => wrapper.close()),
		]);
		results.push(...(await Promise.allSettled([this.connection.close()])));
		const errors = results
			.filter((result) => result.status === "rejected")
			.map((result) => result.reason);
		if (errors.length)
			throw new AggregateError(errors, "RabbitMQ cleanup failed");
	}

	private binding(binding: MessageBinding): RabbitBinding {
		const rabbit = binding as RabbitBinding;
		if (
			rabbit.transport !== "rabbitmq" ||
			!rabbit.type ||
			!rabbit.exchange?.name ||
			typeof rabbit.routingKey !== "string"
		) {
			throw new Error("Invalid RabbitMQ message binding");
		}
		return rabbit;
	}

	private ensureExchange(binding: RabbitBinding): Promise<void> {
		const key = JSON.stringify(binding.exchange);
		let setup = this.exchanges.get(key);
		if (!setup) {
			setup = this.publisher.addSetup((channel: Channel) =>
				this.assertExchange(channel, binding),
			);
			this.exchanges.set(key, setup);
			void setup.catch(() => {
				if (this.exchanges.get(key) === setup) this.exchanges.delete(key);
			});
		}
		return setup;
	}

	private async assertExchange(
		channel: Channel,
		binding: RabbitBinding,
	): Promise<void> {
		await channel.assertExchange(binding.exchange.name, binding.exchange.type, {
			durable: true,
			...binding.exchange.options,
		});
	}

	private async withPublishTimeout<T>(
		promise: Promise<T>,
		timeout: number,
	): Promise<T> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				promise,
				new Promise<never>((_, reject) => {
					timer = setTimeout(
						() => reject(new Error("RabbitMQ publish timed out")),
						timeout,
					);
				}),
			]);
		} finally {
			clearTimeout(timer);
		}
	}
}

function report(options: RabbitMqOptions, error: unknown): void {
	try {
		if (options.onError) options.onError(error);
		else console.error("[RabbitMQ]", error);
	} catch (observerError) {
		console.error("[RabbitMQ] error observer failed", observerError);
	}
}
