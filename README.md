# @awilixify/microservices

Typed, module-scoped messaging for Awilixify. One `onMessage` decorator and scoped publisher, with transport-specific message bindings, consumer options, contexts, and publish results.

RabbitMQ is the first adapter. Kafka, Redis, MQTT, NATS, and gRPC are not implemented. The shared API accepts additional adapters without installing RabbitMQ dependencies.

## Install

```sh
npm install @awilixify/microservices awilixify awilix
npm install amqp-connection-manager amqplib
```

Node.js 22 or newer is required. RabbitMQ clients are optional peer dependencies, loaded when the RabbitMQ adapter subpath is imported. The main package entry point does not load them.

## Factory and feature modules

Each factory owns one adapter and one logical broker connection. Feature modules created by that factory share the connection, but each receives its own restricted publisher. The adapter supplies the default client key, `rabbitMq` for RabbitMQ. Override it with `clientKey` when needed.

```ts
import { DIContext, type ModuleDef } from "awilixify";
import {
  createMessagingModuleFactory,
  onMessage,
  type PayloadOf,
  type ContextOf,
} from "@awilixify/microservices";
import {
  defineRabbitMessage,
  rabbitmq,
} from "@awilixify/microservices/rabbitmq";

class OrderCreated extends defineRabbitMessage<{ id: string }>()({
  type: "order.created",
  exchange: { name: "orders", type: "topic" },
  routingKey: "orders.created",
}) {}

const MessagingModule = createMessagingModuleFactory({
  adapter: rabbitmq({ urls: "amqp://localhost:5672" }),
});

const OrdersMessaging = MessagingModule({
  publishable: { OrderCreated },
  consumable: [OrderCreated],
});

type OrdersDeps = ModuleDef<{
  imports: [typeof OrdersMessaging];
}>["deps"];

class OrdersController {
  @onMessage(OrderCreated, {
    queueName: "billing.orders",
    prefetch: 8,
  })
  async handle(
    payload: PayloadOf<typeof OrderCreated>,
    context: ContextOf<typeof OrderCreated>,
  ) {
    console.log(payload.id, context.message.fields.redelivered);
  }
}

const app = DIContext.create({
  name: "Orders",
  imports: [OrdersMessaging],
  controllers: [OrdersController],
});
await app.init();

const rabbitMq = app.scope.resolve<OrdersDeps["rabbitMq"]>("rabbitMq");
await rabbitMq.publish(OrderCreated, { id: "123" }, { persistent: true });

// During application shutdown:
await app.close();
```

In a larger application, import `OrdersMessaging` into the feature module declaring `OrdersController`. Inject `OrdersDeps["rabbitMq"]` as a constructor dependency named `rabbitMq` in that module's services. Do not register one unrestricted global publisher.

- `publishable` exposes classes through `rabbitMq.messages` and permits `publish()` for those classes only.
- `consumable` permits local `@onMessage` handlers. Producer-only and consumer-only modules use an empty list or object for the other direction. Producer-only modules export no consumer initializer. Keep consumer integrations in separate feature modules when using multiple factories; Awilixify does not merge their common decorator initializers in one module.
- Permissions use exact class identity at runtime. Reusing a wire message name does not grant permission to a different class.
- The selected class determines payload, consumer options, handler context, publish options, and publish result types. Method parameter annotations are still explicit; decorators check them rather than infer them.
- A standalone decorator cannot statically know its containing module. Consumable permissions are checked at initialization; publishable permissions are checked both by types and at runtime.
- Register scoped controllers with `{ useClass: OrdersController, lifetime: "SCOPED" }` to resolve a controller per delivery. Invocation and scope disposal go through Awilixify's initializer context.
- Factories share configuration, not live clients across applications. Each DI application owns its connections.

## Separate brokers and transports

Use a separate factory for each unrelated broker, with distinct client keys:

```ts
const OrdersModule = createMessagingModuleFactory({
  adapter: rabbitmq({ urls: "amqp://orders:5672" }),
});
const AnalyticsModule = createMessagingModuleFactory({
  adapter: rabbitmq({ urls: "amqp://analytics:5672" }),
  clientKey: "analyticsMessaging",
});
```

A feature importing both producer modules can inject `rabbitMq` and `analyticsMessaging`. Literal client keys are preserved in `ModuleDef` dependency types, including the adapter's native connection type. Publishing through a client selects the broker; message contracts contain no connection names. A factory rejects contracts from another transport at compile time and at runtime.

Multiple URLs on one RabbitMQ adapter are alternative endpoints for the same logical broker or cluster, not a way to publish to unrelated brokers. Both a single URL and an array are accepted.

## Native connection and startup

Connection management starts in the background when Awilixify resolves the factory's shared registry. Consumer `addSetup()` callbacks are registered during `afterInit` without waiting for connectivity or broker setup. HTTP and later lifecycle hooks can start independently. Invalid local configuration and module permissions still fail initialization. Broker-side setup failures are reported through `onError`, not thrown from `app.init()`.

The native connection is available through the scoped client, including during a service's `onInit`:

```ts
class MessagingObserver {
  constructor(private readonly rabbitMq: OrdersDeps["rabbitMq"]) {}

  onInit() {
    this.rabbitMq.connection.on("connectFailed", ({ err }) => {
      console.error("RabbitMQ connection attempt failed", err);
    });
    this.rabbitMq.connection.on("disconnect", ({ err }) => {
      console.error("RabbitMQ disconnected", err);
    });
  }

  isConnected() {
    return this.rabbitMq.connection.isConnected();
  }
}
```

The manager also emits `connect`, `error`, `blocked`, and `unblocked`. Connection errors are not all emitted as `error`; observe `connectFailed` and `disconnect` too. `isConnected()` reports broker connectivity, not successful setup of every consumer. There is no additional status wrapper or public readiness wait.

RabbitMQ reconnects indefinitely until shutdown. The driver's default retry delay is 5 seconds. Configure it through `connectionOptions.reconnectTimeInSeconds`. Do not close the native manager yourself; the integration owns its lifecycle. Native operations bypass scoped message permissions and managed publishing options.

## RabbitMQ behavior

Messages use JSON. Runtime payload validation is optional through the factory's `validate(messageClass, payload)` callback. It runs before incoming handler invocation, can be asynchronous, and rejects a delivery by throwing. TypeScript payload types do not validate broker data. Outgoing payloads are serialized but not schema-validated.

Publishing uses a separate confirm channel. `publish()` resolves after broker confirmation, not after consumer processing. It does not guarantee that a queue matched the routing key. Mandatory publishing and returned-message handling are not implemented; `mandatory` is intentionally unavailable. Default publications are persistent, and asserted exchanges and queues are durable unless overridden.

Each decorated consumer owns a channel, one queue, and one exchange binding. Queue names must be unique within an adapter. Use separate queues for subscribers that must each receive an event. Multiple application instances consuming the same queue compete for deliveries.

Consumer options include `queueOptions`, `bindingArguments`, `consumeOptions`, and `prefetch`. The latter defaults to 1. Configure dead-letter routing using RabbitMQ policies or queue arguments.

- Successful handler completion acknowledges the delivery.
- JSON parsing, validation, or handler failure rejects it without requeue by default. **Without dead-letter routing, RabbitMQ discards that delivery.**
- `requeueOnError: true` requeues failed deliveries and can cause an immediate retry loop. There is no managed retry delay or attempt limit.
- `RabbitContext.message` contains the native delivery and headers. `RabbitContext.channel` exposes its native channel for inspection. Do not manually acknowledge deliveries; the adapter owns acknowledgement.
- Topology setup reruns after reconnect. Delivery acknowledgement uses the original channel, never a replacement channel. Disconnection before acknowledgement can cause redelivery; handlers must tolerate duplicates.
- `onError` on the adapter observes connection, channel, and delivery errors. It defaults to `console.error` and cannot override acknowledgement policy.

`publishTimeoutMs` defaults to 10 seconds and bounds each publication, including outgoing exchange setup. A per-call `timeout` overrides it. Publications buffer in memory while disconnected and reject on their timeout; this is not durable storage or an outbox. There is no startup timeout. Publishing is available after the registry starts during `afterInit`, not during `onInit`.

Application initialization does not guarantee consumers are subscribed. If you publish before queues and bindings exist, RabbitMQ may confirm an unroutable message. Provision required routing beforehand when message retention during initial startup matters.

Shutdown stops consumers, waits for active handlers and publications, then closes channels and connections. Active handlers may still publish while draining. There is no forced handler shutdown deadline; a handler that never settles prevents graceful shutdown.

RabbitMQ RPC, manual acknowledgement mode, multiple bindings per consumer, schema/AsyncAPI generation, and automatic trace-header propagation are not implemented. This package does not alter the example platform's existing RabbitMQ package.

## Additional adapters

`AdapterDefinition`, `MessagingAdapter`, `MessageBinding`, and `defineMessage` are exported for adapter implementations. An adapter supplies typed bindings for its consumer options, handler context, publish options, and publish result. `defineMessage<Payload>()(binding)` produces the base class used as a message contract.

An `AdapterDefinition<Connection, Transport, ClientKey>` supplies `transport`, a default `clientKey`, and a synchronous `create()` returning a `MessagingAdapter<Connection>`. The adapter exposes its native `connection`. Resource handles are created synchronously; connection establishment happens in the background.

The runtime adapter receives erased payload and option types and must validate its configuration. `subscribe()` synchronously validates and registers incoming handlers without awaiting connectivity, `stop()` stops deliveries and drains handlers while keeping publishing usable, and `close()` releases resources. The common registry handles module permissions, incoming validation, and lifecycle sequencing. Transport-specific delivery guarantees remain the adapter's responsibility.

## Development

```sh
npm install
npm test
npm run lint
npm run build
```

Tests include compile-time rejection checks and mocked adapter/lifecycle tests. To run the opt-in test against a real RabbitMQ broker:

```sh
RABBITMQ_URL=amqp://localhost:5672 npm run test:runtime -- test/rabbitmq-live.test.ts
```

The live test creates a uniquely named auto-delete exchange and exclusive queue.
