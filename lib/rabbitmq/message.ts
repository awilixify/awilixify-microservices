import type { Options as ManagedOptions } from "amqp-connection-manager";
import type { Channel, ConsumeMessage, Options } from "amqplib";
import { defineMessage, type MessageBinding } from "../message.js";

export type RabbitContext = {
	readonly message: ConsumeMessage;
	/** Native channel for inspection. Delivery acknowledgements are managed by the adapter. */
	readonly channel: Channel;
};
export type RabbitConsumerOptions = {
	queueName: string;
	queueOptions?: Options.AssertQueue;
	bindingArguments?: Record<string, unknown>;
	consumeOptions?: Omit<Options.Consume, "noAck">;
	prefetch?: number;
	/** Defaults to false. Configure a dead-letter exchange to retain rejected messages. */
	requeueOnError?: boolean;
};
export type RabbitPublishOptions = Omit<
	ManagedOptions.Publish,
	"type" | "contentType" | "mandatory"
>;
export type RabbitMessageDefinition = {
	type: string;
	exchange: {
		name: string;
		type: "direct" | "topic" | "fanout" | "headers";
		options?: Options.AssertExchange;
	};
	routingKey: string;
};
export type RabbitBinding = RabbitMessageDefinition &
	MessageBinding<
		RabbitConsumerOptions,
		RabbitContext,
		RabbitPublishOptions,
		void
	> & {
		readonly transport: "rabbitmq";
	};

export function defineRabbitMessage<TPayload>() {
	return <const TDefinition extends RabbitMessageDefinition>(
		definition: TDefinition,
	) =>
		defineMessage<TPayload>()({
			...definition,
			transport: "rabbitmq",
		} as TDefinition & RabbitBinding);
}
