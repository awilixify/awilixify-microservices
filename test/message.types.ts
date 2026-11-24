import type { ModuleDef } from "awilixify";
import {
	type ContextOf,
	createMessagingModuleFactory,
	defineMessage,
	type MessageBinding,
	type MessagingClient,
	onMessage,
	type PayloadOf,
} from "../index.js";
import {
	defineRabbitMessage,
	type RabbitContext,
	rabbitmq,
} from "../lib/rabbitmq/index.js";

class Order extends defineRabbitMessage<{ id: string }>()({
	type: "order",
	exchange: { name: "orders", type: "topic" },
	routingKey: "order",
}) {}
class Other extends defineRabbitMessage<{ id: string }>()({
	type: "other",
	exchange: { name: "orders", type: "topic" },
	routingKey: "other",
}) {}
class Kafka extends defineMessage<{ id: number }>()({
	transport: "kafka",
} as MessageBinding<
	{ groupId: string },
	{ partition: number },
	{ key: string },
	{ offset: string }
> & { transport: "kafka" }) {}

class Controller {
	@onMessage(Order, { queueName: "billing" })
	handle(_payload: PayloadOf<typeof Order>, _context: RabbitContext) {}
	@onMessage(Kafka, { groupId: "billing" })
	kafka(_payload: { id: number }, _context: ContextOf<typeof Kafka>) {}
	@onMessage(Order, { queueName: "billing" })
	payloadOnly(_payload: { id: string }) {}
	@onMessage(Order, { queueName: "billing" })
	noArguments() {}
	// @ts-expect-error Wrong payload
	@onMessage(Order, { queueName: "billing" })
	wrongPayload(_payload: { id: number }) {}
	// @ts-expect-error Wrong context
	@onMessage(Order, { queueName: "billing" })
	wrongContext(_payload: { id: string }, _context: { partition: number }) {}
}
void Controller;
// @ts-expect-error Kafka options cannot be used for RabbitMQ
onMessage(Order, { groupId: "billing" });
// @ts-expect-error Rabbit options cannot be used for Kafka
onMessage(Kafka, { queueName: "billing" });
// @ts-expect-error queueName is required
onMessage(Order, {});

async function publish(
	client: MessagingClient<{ Order: typeof Order; Kafka: typeof Kafka }>,
) {
	const result: Awaited<ReturnType<() => Promise<void>>> = await client.publish(
		Order,
		{ id: "1" },
		{ persistent: true },
	);
	const kafkaResult: { offset: string } = await client.publish(
		Kafka,
		{ id: 1 },
		{ key: "1" },
	);
	// @ts-expect-error Payload must match selected message
	client.publish(Order, { id: 1 });
	// @ts-expect-error Options must match selected message
	client.publish(Order, { id: "1" }, { key: "1" });
	// @ts-expect-error Undeclared message, even with the same payload
	client.publish(Other, { id: "1" });
	// @ts-expect-error Adapter owns the wire type
	client.publish(Order, { id: "1" }, { type: "wrong" });
	void result;
	void kafkaResult;
}
void publish;
const MessagingModule = createMessagingModuleFactory({
	adapter: rabbitmq({ urls: ["amqp://localhost"] }),
});
const feature = MessagingModule({ publishable: { Order }, consumable: [] });
type Deps = ModuleDef<{ imports: [typeof feature] }>["deps"];
function injected(client: Deps["rabbitMq"]) {
	client.publish(Order, { id: "1" });
	// @ts-expect-error Feature module preserves permissions
	client.publish(Other, { id: "1" });
}
void injected;

const Analytics = createMessagingModuleFactory({
	adapter: rabbitmq({ urls: ["amqp://analytics"] }),
	clientKey: "analyticsMessaging",
});
const analytics = Analytics({ publishable: { Order }, consumable: [] });
type AnalyticsDeps = ModuleDef<{ imports: [typeof analytics] }>["deps"];
function native(client: AnalyticsDeps["analyticsMessaging"]) {
	const connection: import("amqp-connection-manager").AmqpConnectionManager =
		client.connection;
	connection.on("disconnect", (_event) => {});
	const connected: boolean = connection.isConnected();
	// @ts-expect-error No unrestricted fallback key
	const oldKey: AnalyticsDeps["messaging"] = client;
	void connected;
	void oldKey;
}
void native;
// @ts-expect-error Factory only accepts its own transport's messages
MessagingModule({ publishable: { Kafka }, consumable: [] });
// @ts-expect-error Factory only accepts its own transport's consumers
MessagingModule({ publishable: {}, consumable: [Kafka] });
// @ts-expect-error startup timeout is not supported
rabbitmq({ urls: ["amqp://localhost"], startupTimeoutMs: 100 });
const removedConnection: import("../lib/rabbitmq/index.js").RabbitMessageDefinition =
	// @ts-expect-error Message contracts don't select infrastructure connections
	{ ...Order.binding, connectionName: "other" };
void removedConnection;
