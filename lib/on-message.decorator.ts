import { createDecoratorStateUpdater } from "awilixify";
import type {
	ConsumerOptionsOf,
	ContextOf,
	MessageConstructor,
	PayloadOf,
} from "./message.js";

type MessageMetadata = { message: MessageConstructor; options: unknown };

const state = createDecoratorStateUpdater("Microservice messages", {
	method: (): MessageMetadata | undefined => undefined,
});

export const ON_MESSAGE_TOKEN = state.token;

export function onMessage<M extends MessageConstructor>(
	message: M,
	options: ConsumerOptionsOf<NoInfer<M>>,
) {
	return <TThis, TReturn>(
		_target: (
			this: TThis,
			payload: PayloadOf<M>,
			context: ContextOf<M>,
		) => TReturn,
		context: ClassMethodDecoratorContext<
			TThis,
			(this: TThis, payload: PayloadOf<M>, context: ContextOf<M>) => TReturn
		>,
	) => {
		state.update(context, {
			method: (previous) => {
				if (previous)
					throw new Error("Only one @onMessage is allowed per method");

				return { message, options };
			},
		});
	};
}
