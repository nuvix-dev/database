import { EventEmitter } from "node:events";
import { AsyncLocalStorage } from "node:async_hooks";
import { EventsEnum } from "./enums.js";
import { Logger } from "@utils/logger.js";
import { Doc } from "./doc.js";
import { Attribute, Collection, Index } from "@validators/schema.js";
import { IEntity } from "types.js";

/**
 * The silence set threaded through the emit chain.
 * - `null` silences EVERY listener for the current scope.
 * - An empty set silences nothing (the default for emissions outside any
 *   silent() scope).
 * - A non-empty set silences exactly the listener names it contains.
 *
 * Instances are always treated as immutable (frozen at creation), so a scope
 * can never mutate another scope's view.
 */
type SilencedListeners = ReadonlySet<string> | null;

/**
 * Shared empty set meaning "nothing is silenced". Frozen so it can never be
 * accidentally mutated into a global suppression.
 */
const NO_SILENCED_LISTENERS: ReadonlySet<string> = Object.freeze(
  new Set<string>(),
);

/**
 * Per-async-execution silence storage.
 *
 * Each silent() call installs its own immutable store for the duration of its
 * callback's async chain. Emissions resolve their scope at trigger time from
 * the CURRENT async execution, which guarantees:
 * - concurrent emissions outside the silenced chain are never swallowed;
 * - two overlapping silent() scopes each carry their own set and cannot
 *   silence or un-silence each other;
 * - no shared mutable state survives an await boundary (stores are frozen and
 *   never written after creation).
 */
const silenceStorage = new AsyncLocalStorage<SilencedListeners>();

/**
 * Pure predicate deciding whether a listener is suppressed under a silence set.
 */
function isListenerSilenced(
  silenced: SilencedListeners,
  listenerName: string,
): boolean {
  return silenced === null || silenced.has(listenerName);
}

/**
 * A type for the listener function that handles the wildcard event.
 * It receives the original event name as the first argument.
 */
type WildcardListener<EventsMap extends EmitterEventMap> = (
  eventName: keyof EventsMap,
  ...args: any[]
) => void;

/**
 * A generic type for your event map, extending the base event map.
 * This ensures the wildcard event is always an option.
 */
export type EmitterEventMap = Record<string | symbol, any[]> & {
  [EventsEnum.All]: WildcardListener<EmitterEventMap>;

  // Database events
  [EventsEnum.DatabaseList]: [string[]];
  [EventsEnum.DatabaseCreate]: [string];
  [EventsEnum.DatabaseDelete]: [string];

  // Collection events
  [EventsEnum.CollectionList]: [Doc<Collection>[]];
  [EventsEnum.CollectionCreate]: [Doc<Collection>];
  [EventsEnum.CollectionUpdate]: [Doc<Collection>];
  [EventsEnum.CollectionRead]: [Doc<Collection>];
  [EventsEnum.CollectionDelete]: [Doc<Collection>];

  // Attribute / Index events
  [EventsEnum.AttributeCreate]: [Doc<Collection>, Doc<Attribute>];
  [EventsEnum.AttributesCreate]: [Doc<Collection>, Doc<Attribute>[]];
  [EventsEnum.AttributeUpdate]: [Doc<Collection>, Doc<Index> | Doc<Attribute>];
  [EventsEnum.AttributeDelete]: [Doc<Collection>, Doc<Attribute>];

  [EventsEnum.IndexRename]: [Doc<Collection>, Doc<Index>, string /* oldName */];
  [EventsEnum.IndexCreate]: [Doc<Collection>, Doc<Index>];
  [EventsEnum.IndexDelete]: [Doc<Collection>, Doc<Index> | null];

  // Relationship events
  [EventsEnum.RelationshipCreate]: [
    Doc<Collection>,
    Doc<Attribute>,
    Doc<Collection>,
    Doc<Attribute>,
  ];
  [EventsEnum.RelationshipDelete]: [
    Doc<Collection>,
    Doc<Attribute>,
    Doc<Collection>,
    Doc<Attribute>,
  ];
  [EventsEnum.RelationshipUpdate]: [
    Doc<Collection>,
    Doc<Attribute>,
    Doc<Collection>,
    Doc<Attribute>,
  ];

  // Document events
  [EventsEnum.DocumentCreate]: [Doc<Partial<IEntity & Record<string, any>>>];
  [EventsEnum.DocumentsCreate]: [Doc<Partial<IEntity & Record<string, any>>>[]];

  [EventsEnum.DocumentRead]: [Doc<Partial<IEntity & Record<string, any>>>];
  [EventsEnum.DocumentFind]: [
    Doc<Partial<IEntity & Record<string, any>>> | undefined,
  ];
  [EventsEnum.DocumentsFind]: [Doc<Partial<IEntity & Record<string, any>>>[]];

  [EventsEnum.DocumentUpdate]: [Doc<Partial<IEntity & Record<string, any>>>];
  [EventsEnum.DocumentsUpdate]: [
    Doc<{ $collection: string; modified: number }>,
  ];
  [EventsEnum.DocumentsUpsert]: [
    Doc<{ $collection: string; created: number; updated: number }>,
  ];

  [EventsEnum.DocumentDelete]: [Doc<Partial<IEntity & Record<string, any>>>];
  [EventsEnum.DocumentsDelete]: [
    | Doc<Partial<IEntity & Record<string, any>>>[]
    | Doc<{ $collection: string; modified: number }>,
  ];

  [EventsEnum.DocumentPurge]: [Doc<Partial<IEntity & Record<string, any>>>];

  [EventsEnum.DocumentCount]: [number];
  [EventsEnum.DocumentSum]: [number];
  [EventsEnum.DocumentIncrease]: [
    Doc<Partial<IEntity & Record<string, any>>>,
    number,
  ];
  [EventsEnum.DocumentDecrease]: [
    Doc<Partial<IEntity & Record<string, any>>>,
    number,
  ];

  // Permissions events
  [EventsEnum.PermissionsCreate]: [Doc<Collection>, string[]];
  [EventsEnum.PermissionsRead]: [Doc<Collection>, string[]];
  [EventsEnum.PermissionsUpdate]: [Doc<Collection>, string[]];
  [EventsEnum.PermissionsDelete]: [Doc<Collection>, string[]];

  // Node standard error event
  error: [Error, string | number | symbol, string];
};

/**
 * The public interface for the Emitter class, defining the custom API.
 * @template EventsMap A map of event names to their argument types.
 */
export interface IEmitter<EventsMap extends EmitterEventMap> {
  on<K extends keyof EventsMap>(
    eventName: K,
    name: string,
    listener: (...args: EventsMap[K]) => void,
  ): this;
  off<K extends keyof EventsMap>(eventName: K, name: string): this;
  trigger<K extends keyof EventsMap>(eventName: K, ...args: EventsMap[K]): void;
  silent<T>(
    callback: () => Promise<T>,
    listeners?: string[] | null,
  ): Promise<T>;
}

/**
 * A utility type to filter out specific methods from the base EventEmitter,
 * allowing us to enforce our custom public API.
 */
type BaseEventEmitterFiltered = {
  [
    P in Exclude<
      keyof EventEmitter,
      | "on"
      | "addListener"
      | "removeListener"
      | "off"
      | "once"
      | "prependListener"
      | "prependOnceListener"
      | "removeAllListeners"
    >
  ]: EventEmitter[P];
};

/**
 * The Emitter class provides a custom event system with named listeners,
 * a wildcard event, and fire-and-forget asynchronous execution.
 * It extends EventEmitter to leverage its core functionality and event loop integration.
 *
 * @template EventsMap A map of event names to their argument types, ensuring type safety.
 */
export class Emitter<EventsMap extends EmitterEventMap = EmitterEventMap>
  extends (EventEmitter as unknown as {
    new (): BaseEventEmitterFiltered;
    prototype: BaseEventEmitterFiltered;
  })
  implements IEmitter<EventsMap>
{
  private _namedListeners = new Map<
    keyof EventsMap,
    Map<string, (...args: any[]) => void>
  >();

  constructor() {
    super();
    (this as unknown as EventEmitter).addListener("error", (err: Error) => {
      if (!this._namedListeners.get("error")?.size) {
        console.error(
          `[Emitter Internal] Unhandled listener error for event: ${err.message}`,
          err,
        );
      }
    });
  }

  /**
   * Registers a listener with a unique name for a specific event.
   * Throws an error if a listener with the same name already exists for the event.
   *
   * @param eventName The name of the event to listen for.
   * @param name A unique name for this listener.
   * @param listener The callback function to execute when the event is triggered.
   * @returns The Emitter instance for method chaining.
   */
  public on<K extends keyof EventsMap>(
    eventName: K,
    name: string,
    listener: (...args: EventsMap[K]) => void,
  ): this {
    if (!this._namedListeners.has(eventName)) {
      this._namedListeners.set(eventName, new Map());
    }
    const listenersForEvent = this._namedListeners.get(eventName)!;

    if (listenersForEvent.has(name)) {
      throw new Error(
        `Listener with name "${name}" already exists for event "${String(eventName)}".`,
      );
    }

    listenersForEvent.set(name, listener);
    return this;
  }

  /**
   * Removes a registered listener by its event name and unique name.
   * Throws an error if the listener does not exist.
   *
   * @param eventName The name of the event the listener was registered for.
   * @param name The unique name of the listener to remove.
   * @returns The Emitter instance for method chaining.
   */
  public off<K extends keyof EventsMap>(eventName: K, name: string): this {
    const listenersForEvent = this._namedListeners.get(eventName);
    if (!listenersForEvent || !listenersForEvent.has(name)) {
      throw new Error(
        `Listener with name "${name}" does not exist for event "${String(eventName)}".`,
      );
    }

    listenersForEvent.delete(name);

    if (listenersForEvent.size === 0) {
      this._namedListeners.delete(eventName);
    }

    return this;
  }

  /**
   * Triggers an event, executing all registered listeners.
   * This method is "fire and forget" and does not wait for asynchronous listeners to complete.
   * It handles errors gracefully by catching them and emitting a separate 'error' event.
   *
   * Listener suppression is resolved from the CURRENT async execution's
   * silence scope (see silent()). Emissions running outside any silent()
   * scope are never suppressed, and concurrent emissions in other async
   * chains are unaffected by this chain's scope.
   *
   * @param eventName The name of the event to trigger.
   * @param args The arguments to pass to the listener functions.
   */
  public trigger<K extends keyof EventsMap>(
    eventName: K,
    ...args: EventsMap[K]
  ): void {
    // Resolve this emission's silence set from its own async chain. There is
    // no shared instance state: absent store => nothing is silenced.
    const silenced: SilencedListeners =
      silenceStorage.getStore() ?? NO_SILENCED_LISTENERS;

    const allEventListeners = this._namedListeners.get(EventsEnum.All as K);
    if (allEventListeners) {
      for (const [listenerName, listenerFunc] of allEventListeners) {
        this._executeListener(
          listenerFunc,
          listenerName,
          EventsEnum.All,
          silenced,
          eventName,
          ...args,
        );
      }
    }

    const specificListeners = this._namedListeners.get(eventName);
    if (specificListeners) {
      for (const [listenerName, listenerFunc] of specificListeners) {
        this._executeListener(
          listenerFunc,
          listenerName,
          eventName,
          silenced,
          ...args,
        );
      }
    }
  }

  /**
   * Executes a callback with specified listeners silenced for the duration of
   * the callback's async execution ONLY.
   * If listeners is null, all listeners are silenced.
   * If listeners is an array, only those specific listeners are silenced.
   *
   * The silence set is computed locally and installed as an immutable
   * per-async-chain scope; it is threaded through the emit chain
   * (trigger -> _executeListener) instead of mutating shared instance state.
   * Overlapping silent() scopes each carry their own set and cannot affect
   * each other, and emissions outside this async chain are never suppressed.
   * Nesting can only widen suppression (an inner scope unions with its
   * enclosing scope and can never un-silence a listener).
   *
   * @param callback The async function to execute.
   * @param listeners Array of listener names to silence, or null to silence all.
   * @returns The result of the callback function.
   */
  public async silent<T>(
    callback: () => Promise<T>,
    listeners: string[] | null = null,
  ): Promise<T> {
    const enclosing = silenceStorage.getStore() ?? NO_SILENCED_LISTENERS;

    // Monotonic union with the enclosing scope: null (silence-all) wins, and
    // a nested named scope can only add names, never remove them.
    const silenced: SilencedListeners =
      listeners === null || enclosing === null
        ? null
        : Object.freeze(new Set([...enclosing, ...listeners]));

    return silenceStorage.run(silenced, () => callback());
  }

  /**
   * Copies every named listener registered on this emitter onto another
   * emitter instance. Used when constructing transaction scopes so that
   * listeners attached to the parent database keep receiving events
   * triggered inside the transaction, while each instance still owns its
   * own listener registry (no shared mutable state).
   *
   * @param target The emitter that receives copies of this emitter's listeners.
   */
  public copyListenersTo(target: Emitter<EventsMap>): void {
    for (const [eventName, listeners] of this._namedListeners) {
      for (const [name, listener] of listeners) {
        target.on(
          eventName,
          name,
          listener as (...args: EventsMap[keyof EventsMap]) => void,
        );
      }
    }
  }

  /**
   * A private helper method that safely executes a listener function.
   * It handles synchronous errors and attaches a '.catch' handler to Promises
   * returned by asynchronous listeners, without blocking execution.
   *
   * Suppression is decided by the silence set threaded in from trigger():
   * `null` silences every listener, otherwise set membership decides. The
   * check happens here so the entire emit chain carries its scope explicitly
   * instead of reading shared instance state.
   *
   * @param listener The listener function to execute.
   * @param listenerName The unique name of the listener (for error reporting).
   * @param originalEventName The original event name it was registered for.
   * @param silenced The silence set for this emission's scope.
   * @param args The arguments to pass to the listener.
   */
  private _executeListener<K extends keyof EventsMap>(
    listener: (...args: any[]) => void | Promise<void>,
    listenerName: string,
    originalEventName: K | typeof EventsEnum.All,
    silenced: SilencedListeners,
    ...args: any[]
  ): void {
    if (isListenerSilenced(silenced, listenerName)) {
      return;
    }

    try {
      const result = listener(...args);

      if (result && typeof result.then === "function") {
        result.catch((err: Error) => {
          this._handleListenerError(err, originalEventName, listenerName);
        });
      }
    } catch (err: any) {
      this._handleListenerError(err, originalEventName, listenerName);
    }
  }

  /**
   * A centralized handler for errors that occur within a listener function.
   * It logs the error and emits a dedicated 'error' event to all listeners.
   *
   * @param error The error that was caught.
   * @param eventName The name of the event that was being processed.
   * @param listenerName The name of the listener that failed.
   */
  private _handleListenerError(
    error: Error,
    eventName: keyof EventsMap | typeof EventsEnum.All,
    listenerName: string,
  ): void {
    Logger.error(
      `[Emitter Error] Listener "${listenerName}" for event "${String(eventName)}" failed:`,
      error,
    );
    this.emit("error", ...([error, String(eventName), listenerName] as any));
  }
}
