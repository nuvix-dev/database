import { DatabaseException } from "@errors/base.js";
import { Permission } from "@utils/permission.js";
import { IEntity, IEntityInput } from "types.js";
import chalk from "chalk";

type IsReferenceObject<T> = T extends { $id: string }
  ? true
  : T extends { $collection: string }
    ? true
    : false;

type TransformField<T> =
  IsReferenceObject<T> extends true
    ? Doc<T extends Partial<IEntity> ? T : Partial<IEntity>>
    : T extends Array<infer U>
      ? Array<TransformField<U>>
      : T extends object
        ? TransformEntity<T>
        : T;

type TransformEntity<T> = {
  [K in keyof T]: TransformField<T[K]>;
};

type Simplify<T> = { [K in keyof T]: T[K] };

type FilterInput<T> = Partial<Omit<T, "$permissions">> & IEntityInput;

function isEntityLike(value: unknown): value is Record<string, unknown> {
  return (
    typeof value === "object" &&
    value !== null &&
    ("$id" in value || "$collection" in value)
  );
}

export class Doc<
  T extends Record<string, any> & Partial<IEntity> = Partial<IEntity>,
> {
  #_data: Record<string, any> = {};

  /**
   * Creates a new Doc instance.
   */
  constructor(data: T extends IEntity ? FilterInput<T> : never);
  constructor(
    data?: (T | TransformEntity<T>) | (IEntityInput & Record<string, any>),
  );
  constructor(
    data?: (T | TransformEntity<T>) | (IEntityInput & Record<string, any>),
  ) {
    this.#_data = {};
    if (data) {
      if (data.$id && typeof data.$id !== "string") {
        throw new DocException("$id must be a string");
      }

      if (data.$permissions && !Array.isArray(data.$permissions)) {
        throw new DocException("$permissions must be an array");
      }

      for (const [key, value] of Object.entries(data)) {
        if (Array.isArray(value)) {
          this.#_data[key] = value.map((item) =>
            isEntityLike(item)
              ? item instanceof Doc
                ? item
                : new Doc(item as any)
              : item,
          );
        } else if (isEntityLike(value)) {
          this.#_data[key] =
            value instanceof Doc ? value : new Doc(value as any);
        } else {
          this.#_data[key] = value ?? null;
        }
      }
    }
  }

  /**
   * Creates a new Doc instance from the provided data.
   */
  static from<D extends Partial<IEntity>>(data: D & IEntityInput): Doc<D> {
    return new Doc(data);
  }

  /**
   * Gets the value of a field. If the field does not exist, returns the provided default value or null.
   */
  public get<K extends keyof T>(
    name: K,
  ): Exclude<TransformEntity<T>[K], undefined>;
  public get<K extends keyof T, D extends T[K]>(
    name: K,
    _default?: D,
  ): Exclude<TransformEntity<T>[K], undefined>;
  public get<K extends keyof T, D = null>(
    name: K,
    _default?: D,
  ): Exclude<TransformEntity<T>[K], undefined> | D;
  public get<K extends string, D = null>(name: K, _default?: D): D;
  public get<K extends keyof T, D = null>(
    name: K,
    _default?: D | null,
  ): Exclude<TransformEntity<T>[K], undefined> | D {
    const value = this.#_data[name as string];
    if (arguments.length === 1) {
      _default = null;
    }
    return value === undefined ? (_default as D) : value;
  }

  /**
   * Returns a shallow copy of all fields in the document.
   */
  public getAll(): TransformEntity<T> {
    return { ...this.#_data } as TransformEntity<T>;
  }

  /**
   * Sets the value of a field, transforming it into a Doc if it's an entity-like object.
   * If the value is an array, each entity-like item in the array is also transformed into a Doc.
   * If the value is null or undefined, it is set to null.
   */
  public set<K extends keyof T>(name: K, value: TransformField<T[K]>): this;
  public set<K extends string, V extends unknown>(
    name: K,
    value: V,
  ): Doc<Simplify<T & Record<K, TransformField<V>>>>;
  public set<K extends string, V extends unknown>(name: K, value: V): any {
    if (Array.isArray(value)) {
      this.#_data[name] = value.map((item) =>
        isEntityLike(item)
          ? item instanceof Doc
            ? item
            : new Doc(item as any)
          : item,
      );
    } else if (isEntityLike(value)) {
      this.#_data[name] = value instanceof Doc ? value : new Doc(value as any);
    } else {
      this.#_data[name] = value ?? null;
    }
    return this;
  }

  /**
   * Updates the value of a field if the value is provided (not undefined).
   * If the value is undefined, no change is made.
   */
  public update<K extends keyof T>(
    name: K,
    value?: TransformField<T[K]>,
  ): this {
    if (typeof value !== "undefined") {
      return this.set(name, value);
    }
    return this;
  }

  /**
   * Updates multiple fields from the provided data object.
   * Only fields with defined values are updated; undefined values are skipped.
   */
  public updateAll(data: Partial<FilterInput<T>>): this {
    for (const [key, value] of Object.entries(data)) {
      if (typeof value !== "undefined") {
        this.set(key, value);
      }
    }
    return this;
  }

  /**
   * Sets multiple fields from the provided data object.
   * All fields in the data object are set, including those with null values.
   */
  public setAll(data: FilterInput<T>): this;
  public setAll<D extends FilterInput<T> & Record<string, any>>(
    data: D,
  ): Doc<Simplify<T & D>>;
  public setAll(data: FilterInput<T>): this {
    for (const [key, value] of Object.entries(data)) {
      if (Array.isArray(value)) {
        this.#_data[key] = value.map((item) =>
          isEntityLike(item)
            ? item instanceof Doc
              ? item
              : new Doc(item as any)
            : item,
        );
      } else if (isEntityLike(value)) {
        this.#_data[key] =
          (value as any) instanceof Doc ? value : new Doc(value as any);
      } else {
        this.#_data[key] = value ?? null;
      }
    }
    return this;
  }

  /**
   * Appends a value to an array field. If the field is not an array, an exception is thrown.
   * If the value is an entity-like object, it is transformed into a Doc before appending.
   */
  public append<K extends string & keyof T>(
    name: K,
    value: TransformField<T[K]> extends Array<unknown>
      ? TransformField<T[K]>[number]
      : TransformField<T[K][number]>,
  ): this {
    if (!Array.isArray(this.#_data[name])) {
      throw new DocException(
        `Cannot append to ${String(name)}, it is not an array`,
      );
    }
    if (isEntityLike(value)) {
      this.#_data[name].push(
        value instanceof Doc ? value : new Doc(value as any),
      );
    } else {
      this.#_data[name].push(value);
    }
    return this;
  }

  /**
   * Prepends a value to an array field. If the field is not an array, an exception is thrown.
   * If the value is an entity-like object, it is transformed into a Doc before prepending.
   */
  public prepend<K extends string & keyof T>(
    name: K,
    value: TransformField<T[K]> extends Array<any>
      ? TransformField<T[K]>[number]
      : TransformField<T[K]>,
  ): this {
    if (!Array.isArray(this.#_data[name])) {
      throw new DocException(
        `Cannot prepend to ${String(name)}, it is not an array`,
      );
    }
    if (isEntityLike(value)) {
      this.#_data[name].unshift(
        value instanceof Doc ? value : new Doc(value as any),
      );
    } else {
      this.#_data[name].unshift(value);
    }
    return this;
  }

  /**
   * Deletes a field from the document if it exists.
   */
  public delete<K extends string & keyof T>(name: K): this {
    if (name in this.#_data) {
      delete this.#_data[name];
    }
    return this;
  }

  /**
   * Gets the document ID.
   */
  public getId(): string {
    return this.get("$id") as string;
  }

  /**
   * Gets the value of $sequence.
   */
  public getSequence(): number {
    return this.get("$sequence") as number;
  }

  /**
   * Gets the tenant ID, or null if not set.
   */
  public getTenant(): number | null {
    const tenant = this.get("$tenant", null);
    if (tenant === null || typeof tenant === "number") {
      return tenant;
    } else {
      throw new DocException("$tenant must be a number or null");
    }
  }

  /**
   * Gets the collection name.
   */
  public getCollection(): string {
    const collection = this.get("$collection");
    return collection as string;
  }

  /**
   * Gets the creation date, or null if not set. If the value is a string, it is converted to a Date object.
   */
  public createdAt(): Date | null {
    const value = this.get("$createdAt", null);
    if (typeof value === "string") {
      return new Date(value);
    }
    return value as Date | null;
  }

  /**
   * Gets the last updated date, or null if not set. If the value is a string, it is converted to a Date object.
   */
  public updatedAt(): Date | null {
    const value = this.get("$updatedAt", null);
    if (typeof value === "string") {
      return new Date(value);
    }
    return value as Date | null;
  }

  /**
   * Gets the list of permissions, ensuring uniqueness and converting Permission objects to strings.
   */
  public getPermissions(): string[] {
    const permissions: (string | Permission)[] = this.get(
      "$permissions",
      [],
    ) as (string | Permission)[];

    return Array.from(
      new Set(
        permissions
          .map((p) => (p instanceof Permission ? p.toString() : p))
          .filter(Boolean),
      ),
    );
  }

  /**
   * Gets the list of read permissions.
   */
  public getRead(): string[] {
    return this.getPermissionsByType("read");
  }

  /**
   * Gets the list of create permissions.
   */
  public getCreate(): string[] {
    return this.getPermissionsByType("create");
  }

  /**
   * Gets the list of update permissions.
   */
  public getUpdate(): string[] {
    return this.getPermissionsByType("update");
  }

  /**
   * Gets the list of delete permissions.
   */
  public getDelete(): string[] {
    return this.getPermissionsByType("delete");
  }

  /**
   * Gets the list of write permissions (create, update, delete).
   */
  public getWrite() {
    return Array.from(
      new Set([...this.getCreate(), ...this.getUpdate(), ...this.getDelete()]),
    );
  }

  /**
   * Gets permissions of a specific type (e.g., "read", "create", "update", "delete").
   */
  public getPermissionsByType(type: string): string[] {
    return this.getPermissions()
      .filter((permission) => permission.startsWith(type))
      .map((permission) =>
        permission
          .replace(`${type}(`, "")
          .replace(")", "")
          .replace(/"/g, "")
          .trim(),
      );
  }

  /**
   * Checks if a field exists in the document.
   */
  public has(name: keyof T): boolean;
  public has(name: string): boolean;
  public has(name: string): boolean {
    return Object.hasOwn(this.#_data, name);
  }

  /**
   * Returns an array of all field names in the document.
   */
  public keys(): (keyof T)[] {
    return Object.keys(this.#_data);
  }

  /**
   * Finds the first item in a field (which can be a single value or an array) that matches the provided predicate.
   * If the field is an array, it searches through the array items.
   * If the field is a single value, it checks that value against the predicate.
   * Returns the matching item or null if no match is found.
   */
  public findWhere<K extends string & keyof T>(
    key: K,
    predicate: (item: T[K] extends Array<any> ? T[K][number] : T[K]) => boolean,
  ): T[K] extends Array<any> ? T[K][number] : T[K] | null;
  public findWhere<V = unknown>(
    key: string,
    predicate: (item: V) => boolean,
  ): V | null;
  public findWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
  ): V | null {
    // Recursively search for a value matching the predicate at the given key in this entity and all nested entities/arrays
    const value = this.get(key);
    if (Array.isArray(value)) {
      for (const item of value as unknown[]) {
        if (item !== undefined && predicate(item as V)) {
          return item as V;
        }
      }
    } else if (value !== undefined && predicate(value as V)) {
      return value as V;
    }
    return null;
  }

  /**
   * Replaces items in a field (which can be a single value or an array) that match the provided predicate with the given replacement value or the result of the replacement function.
   * If the field is an array, it iterates through the array and replaces matching items.
   * If the field is a single value and it matches the predicate, it replaces that value.
   */
  public replaceWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
    replacement: V | ((item: V) => V),
  ): void;
  public replaceWhere<V = unknown>(
    key: string,
    predicate: (item: V) => boolean,
    replacement: V | ((item: V) => V),
  ): void;
  public replaceWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
    replacement: V | ((item: V) => V),
  ): void {
    const value = this.get(key);
    if (Array.isArray(value)) {
      for (let i = 0; i < value.length; i++) {
        if (predicate(value[i] as V)) {
          if (typeof replacement === "function") {
            value[i] = (replacement as (item: V) => V)(value[i] as V);
          } else {
            value[i] = replacement;
          }
        }
      }
    } else if (value !== undefined && predicate(value as V)) {
      if (typeof replacement === "function") {
        this.set(key, (replacement as (item: V) => V)(value as V));
      } else {
        this.set(key, replacement);
      }
    }
  }

  /**
   * Deletes items in a field (which can be a single value or an array) that match the provided predicate.
   * If the field is an array, it filters out matching items.
   * If the field is a single value and it matches the predicate, it deletes that field.
   */
  public deleteWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
  ): void;
  public deleteWhere<V = unknown>(
    key: string,
    predicate: (item: V) => boolean,
  ): void;
  public deleteWhere<V = unknown>(
    key: string & keyof T,
    predicate: (item: V) => boolean,
  ): void {
    const value = this.get(key);
    if (Array.isArray(value)) {
      this.set(
        key,
        value.filter((item: V) => !predicate(item)),
      );
    } else if (value !== undefined && predicate(value as V)) {
      this.delete(key);
    }
  }

  /**
   * Checks if the document has no fields.
   */
  public empty(): boolean {
    return this.keys().length === 0;
  }

  /**
   * Converts the document to a plain object, optionally filtering fields by allow and disallow lists.
   * If allow list is provided, only fields in that list are included.
   * If disallow list is provided, fields in that list are excluded.
   * Nested Doc instances are also converted to plain objects recursively.
   */
  public toObject(): T;
  public toObject(allow: (keyof T)[], disallow?: (keyof T)[]): T;
  public toObject(allow: any[] = [], disallow: any[] = []): T {
    const output: Record<string, unknown> = {};
    const keys = this.keys();
    for (const key of keys) {
      const value = this.#_data[key as string];
      if (allow.length && !allow.includes(key)) continue;
      if (disallow.includes(key)) continue;

      if (value instanceof Doc) {
        output[key as string] = value.toObject(allow, disallow);
      } else if (Array.isArray(value)) {
        output[key as string] = value.map((item) =>
          item instanceof Doc ? item.toObject(allow, disallow) : item,
        );
      } else {
        output[key as string] = value;
      }
    }
    if (!disallow.includes("$permissions")) {
      output["$permissions"] = this.getPermissions();
    }
    return output as T;
  }

  /**
   * Converts the document to JSON by calling toObject.
   */
  toJSON() {
    return this.toObject();
  }

  /**
   * Creates a deep clone of the document, including nested Doc instances and arrays.
   */
  clone() {
    const cloned = new Doc<T>();
    const keys = this.keys();
    for (const key of keys) {
      const value = this.#_data[key as string];
      if (value instanceof Doc) {
        (cloned as any).#_data[key as string] = value.clone();
      } else if (Array.isArray(value)) {
        (cloned as any).#_data[key as string] = value.map((item) =>
          item instanceof Doc ? item.clone() : item,
        );
      } else {
        (cloned as any).#_data[key as string] = value;
      }
    }
    return cloned;
  }

  /**
   * Custom inspection method for Node.js console and util.inspect, providing a colored and formatted string representation of the document.
   */
  [Symbol.for("nodejs.util.inspect.custom")]() {
    const formatValue = (value: any, depth: number = 0): string => {
      if (value instanceof Doc) {
        return chalk.cyan(`Doc(${formatValue(value.#_data, depth + 1)})`);
      } else if (Array.isArray(value)) {
        return chalk.green(
          `[${value.map((item) => formatValue(item, depth + 1)).join(", ")}]`,
        );
      } else if (typeof value === "function") {
        return chalk.gray("[Function]");
      } else if (typeof value === "undefined") {
        return chalk.gray("undefined");
      } else if (typeof value === "object" && value !== null) {
        // Only treat as object literal if prototype is Object
        if (Object.getPrototypeOf(value) === Object.prototype) {
          if (Object.keys(value).length === 0) {
            return chalk.gray("{}");
          }
          const indent = "  ".repeat(depth + 1);
          const entries = Object.entries(value)
            .map(
              ([key, val]) =>
                `${indent}${chalk.yellow(key)}: ${formatValue(val, depth + 1)}`,
            )
            .join(",\n");
          return `{
${entries}
${"  ".repeat(depth)}}`;
        } else {
          // It's a class instance, check for toString or toJSON
          if (typeof value.toJSON === "function") {
            return chalk.gray(`[toJSON: ${JSON.stringify(value.toJSON())}]`);
          } else if (
            typeof value.toString === "function" &&
            value.toString !== Object.prototype.toString
          ) {
            return chalk.gray(`[toString: ${value.toString()}]`);
          } else {
            return chalk.gray(
              `[instance of ${(value as any).constructor?.name || "Unknown"}]`,
            );
          }
        }
      } else if (typeof value === "string") {
        return chalk.magenta(`"${value}"`);
      } else if (typeof value === "number") {
        return chalk.blue(value.toString());
      } else if (typeof value === "boolean") {
        return chalk.red(value.toString());
      } else if (value === null) {
        return chalk.gray("null");
      } else {
        return String(value);
      }
    };

    return `Doc ${formatValue(this.#_data)}`;
  }
}

export class DocException extends DatabaseException {}
