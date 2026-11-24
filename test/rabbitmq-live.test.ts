import { randomUUID } from "node:crypto";
import { DIContext } from "awilixify";
import { expect, it } from "vitest";
import {
	createMessagingModuleFactory,
	type MessagingClient,
	onMessage,
} from "../index.js";
import { defineRabbitMessage, rabbitmq } from "../lib/rabbitmq/index.js";

it.skipIf(!process.env.RABBITMQ_URL)(
	"publishes and consumes JSON through a real RabbitMQ broker",
	async () => {
		const name = `awilixify-test-${randomUUID()}`;
		class Order extends defineRabbitMessage<{ id: string }>()({
			type: "order.created",
			exchange: {
				name,
				type: "topic",
				options: { durable: false, autoDelete: true },
			},
			routingKey: "orders.created",
		}) {}
		const MessagingModule = createMessagingModuleFactory({
			adapter: rabbitmq({
				urls: [process.env.RABBITMQ_URL ?? ""],
			}),
		});
		const received: unknown[] = [];
		class Consumer {
			@onMessage(Order, {
				queueName: name,
				queueOptions: { durable: false, exclusive: true, autoDelete: true },
			})
			handle(payload: { id: string }) {
				received.push(payload);
			}
		}
		const app = DIContext.create({
			name: "LiveTest",
			imports: [
				MessagingModule({ publishable: { Order }, consumable: [Order] }),
			],
			controllers: [Consumer],
		});
		try {
			await app.init();
			const client =
				app.scope.resolve<MessagingClient<{ Order: typeof Order }>>("rabbitMq");
			// Startup is non-blocking, so retry until consumer topology exists.
			await expect
				.poll(
					async () => {
						await client.publish(Order, { id: "1" });
						return received;
					},
					{ timeout: 10_000 },
				)
				.toContainEqual({ id: "1" });
		} finally {
			await app.close();
		}
	},
	15_000,
);
