import { QueryException } from "@errors/index.js";
import { Logger } from "@utils/logger.js";
import { QueryByType } from "./types.js";
import { Doc } from "./doc.js";
import { CursorEnum, OrderEnum } from "./enums.js";

/**
 * Defines the types of operations a query can perform.
 */
export enum QueryType {
  Equal = "equal",
  NotEqual = "notEqual",
  LessThan = "lessThan",
  LessThanEqual = "lessThanEqual",
  GreaterThan = "greaterThan",
  GreaterThanEqual = "greaterThanEqual",
  Contains = "contains",
  NotContains = "notContains",
  Search = "search",
  NotSearch = "notSearch",
  IsNull = "isNull",
  IsNotNull = "isNotNull",
  Between = "between",
  NotBetween = "notBetween",
  StartsWith = "startsWith",
  NotStartsWith = "notStartsWith",
  NotEndsWith = "notEndsWith",
  EndsWith = "endsWith",
  Select = "select",
  OrderDesc = "orderDesc",
  OrderAsc = "orderAsc",
  Limit = "limit",
  Offset = "offset",
  CursorAfter = "cursorAfter",
  CursorBefore = "cursorBefore",
  And = "and",
  Or = "or",
  Not = "not",
  /**
   * Populate is used to fetch related documents based on the query.
   */
  Populate = "populate",
}

export type ScalarValue = string | number | boolean | null; // Null included for IsNull/IsNotNull implicitly
export type QueryValue = ScalarValue | Query; // Query itself can be a value for logical operators

export type QueryValues = ScalarValue[] | Query[];

interface RawQueryObject {
  method: string;
  attribute?: string;
  values?: any;
}

/**
 * Represents a single query operation with a method, attribute, and values.
 * Can represent filtering, ordering, limiting, or logical combinations of queries.
 */
export class Query {
  public static readonly TYPES: QueryType[] = Object.values(QueryType);
  public static readonly DEFAULT_ALIAS = "main";

  private static readonly TYPES_SET: ReadonlySet<string> = new Set(
    Object.values(QueryType),
  );

  protected static readonly LOGICAL_TYPES = [
    QueryType.And,
    QueryType.Or,
  ] as const;

  protected method: QueryType;
  protected attribute: string;
  protected values: QueryValues;
  protected _onArray: boolean;

  /**
   * Constructs a new Query object.
   *
   * @param method - The type of query operation.
   * @param attribute - The attribute/field the query applies to. Defaults to an empty string.
   * @param values - The values associated with the query. Defaults to an empty array.
   * @throws {QueryException} If values are provided for `IsNull` or `IsNotNull` methods.
   */
  constructor(
    method: QueryType,
    attribute: string = "",
    values: QueryValues = [],
  ) {
    if (
      attribute === "" &&
      (method === QueryType.OrderAsc || method === QueryType.OrderDesc)
    ) {
      attribute = "$sequence";
    }

    if (
      (method === QueryType.IsNull || method === QueryType.IsNotNull) &&
      values.length > 0
    ) {
      throw new QueryException(
        `Query method "${method}" should not have values.`,
      );
    }

    this.method = method;
    this.attribute = attribute;
    this.values = values;
    this._onArray = false;
  }

  /**
   * Creates a deep clone of the current Query object.
   * For logical queries (AND/OR), it recursively clones nested Query objects.
   *
   * @returns {Query} A new Query instance that is a deep copy.
   */
  public clone(): Query {
    let clonedValues: QueryValues;

    if (this.isNested()) {
      clonedValues = (this.values as Query[]).map((value) => value.clone());
    } else {
      clonedValues = [...(this.values as ScalarValue[])];
    }

    const clonedQuery = new Query(this.method, this.attribute, clonedValues);
    clonedQuery.setOnArray(this.onArray());
    return clonedQuery;
  }

  /**
   * Gets the query method.
   * @returns {QueryType} The method of the query.
   */
  public getMethod(): QueryType {
    return this.method;
  }

  /**
   * Gets the attribute associated with the query.
   * @returns {string} The attribute name.
   */
  public getAttribute(): string {
    return this.attribute;
  }

  /**
   * Gets all values associated with the query.
   * Returns an array of scalar values or an array of nested Query objects.
   * @returns {QueryValues} An array of values.
   */
  public getValues(): QueryValues {
    return this.values;
  }

  /**
   * Gets the first value associated with the query.
   *
   * @param defaultValue - A default value to return if no values are present. Defaults to `null`.
   * @returns {ScalarValue | Query | null} The first value, or the default value.
   */
  public getValue(
    defaultValue: ScalarValue | Query | null = null,
  ): ScalarValue | Query | null {
    return (this.values as Array<ScalarValue | Query>)[0] ?? defaultValue;
  }

  /**
   * Sets the query method.
   * @param method - The new method.
   * @returns {this} The current Query instance for chaining.
   */
  public setMethod(method: QueryType): this {
    this.method = method;
    return this;
  }

  /**
   * Sets the attribute for the query.
   * @param attribute - The new attribute name.
   * @returns {this} The current Query instance for chaining.
   */
  public setAttribute(attribute: string): this {
    this.attribute = attribute;
    return this;
  }

  /**
   * Sets the values for the query.
   * @param values - The new array of values.
   * @returns {this} The current Query instance for chaining.
   */
  public setValues(values: QueryValues): this {
    this.values = values;
    return this;
  }

  /**
   * Sets a single value for the query, replacing any existing values.
   * @param value - The single value to set.
   * @returns {this} The current Query instance for chaining.
   */
  public setValue(value: QueryValue): this {
    this.values = [value] as QueryValues;
    return this;
  }

  /**
   * Checks if a given string is a valid QueryType method.
   *
   * @param value - The string to check.
   * @returns {boolean} True if the value is a valid QueryType, false otherwise.
   */
  public static isMethod(value: string): value is QueryType {
    return Query.TYPES_SET.has(value);
  }

  /**
   * Parses a JSON string representation of a query into a Query object.
   *
   * @param queryJsonString - The JSON string representing the query.
   * @returns {Query} The parsed Query object.
   * @throws {QueryException} If the JSON is invalid or the query structure is malformed.
   */
  public static parse(queryJsonString: string | object): Query {
    try {
      const parsedQuery =
        typeof queryJsonString === "string"
          ? JSON.parse(queryJsonString)
          : queryJsonString;
      return Query.parseQuery(parsedQuery);
    } catch (e: unknown) {
      const errorMessage = e instanceof Error ? e.message : String(e);
      throw new QueryException(`Invalid query JSON string: ${errorMessage}`);
    }
  }

  /**
   * Parses a raw JavaScript object representation of a query into a Query object.
   * This is the core parsing logic that validates the structure and types.
   *
   * @param rawQueryObject - The raw object representing the query.
   * @returns {Query} The parsed Query object.
   * @throws {QueryException} If the query object structure is malformed or invalid.
   */
  public static parseQuery(rawQueryObject: RawQueryObject): Query {
    const {
      method: rawMethod,
      attribute: rawAttribute,
      values: rawValues,
    } = rawQueryObject;

    if (typeof rawMethod !== "string" || !Query.isMethod(rawMethod)) {
      throw new QueryException(
        `Invalid query method: "${rawMethod}". Must be a valid QueryType string.`,
      );
    }
    const method: QueryType = rawMethod;

    let attribute: string = rawAttribute ?? "";
    if (typeof attribute !== "string") {
      throw new QueryException(
        `Invalid query attribute: "${rawAttribute}". Must be a string or undefined.`,
      );
    }

    let values: QueryValues = [];
    if (rawValues !== undefined && rawValues !== null) {
      if (!Array.isArray(rawValues)) {
        throw new QueryException(
          `Invalid query values for method "${method}". Must be an array.`,
        );
      }

      if (method === QueryType.And || method === QueryType.Or) {
        values = rawValues.map((val, index) => {
          if (typeof val !== "object" || val === null) {
            throw new QueryException(
              `Invalid nested query at index ${index} for method "${method}". Expected an object.`,
            );
          }
          return Query.parseQuery(val as RawQueryObject);
        });
      } else if (method === QueryType.Populate) {
        if (attribute === "") {
          throw new QueryException(
            `Invalid query attribute: "${rawAttribute}". Must be a non-empty string.`,
          );
        }
        values = rawValues?.map((val, index) => {
          if (typeof val !== "object" || val === null) {
            throw new QueryException(
              `Invalid populate query at index ${index} for attribute "${attribute}". Expected an object.`,
            );
          }
          return Query.parseQuery(val as RawQueryObject);
        });
      } else {
        values = rawValues.map((val, index) => {
          if (
            typeof val !== "string" &&
            typeof val !== "number" &&
            typeof val !== "boolean" &&
            val !== null
          ) {
            Logger.warn(
              `Unexpected value type for method "${method}" at index ${index}: ${typeof val}. Expected scalar.`,
            );
          }
          return val as ScalarValue;
        });

        if (
          (method === QueryType.IsNull || method === QueryType.IsNotNull) &&
          values.length > 0
        ) {
          throw new QueryException(
            `Query method "${method}" should not have values.`,
          );
        }
      }
    }

    return new Query(method, attribute, values);
  }

  /**
   * Parses an array of JSON query strings into an array of Query objects.
   *
   * @param queriesJsonStrings - An array of JSON strings, each representing a query.
   * @returns {Query[]} An array of parsed Query objects.
   * @throws {QueryException} If any query string is invalid.
   */
  public static parseQueries(queriesJsonStrings: string[]): Query[] {
    return queriesJsonStrings.map((queryJson) => Query.parse(queryJson));
  }

  /**
   * Finds a cursor query (CursorAfter or CursorBefore) in an array of queries.
   *
   * @param queries - An array of Query objects.
   * @returns {Query | undefined} The first found cursor query, or undefined if none is found.
   */
  public static findCursor(queries: Query[]): Query | undefined {
    return queries.find(
      (query) =>
        query.getMethod() === QueryType.CursorAfter ||
        query.getMethod() === QueryType.CursorBefore,
    );
  }

  /**
   * Converts the Query object to a plain object representation.
   * Nested logical queries are also converted to their object representation.
   *
   * @returns {{ method: QueryType; attribute?: string; values: QueryValues; }} The object representation.
   */
  public toObject(): { method: QueryType; attribute?: string; values?: any } {
    const object: { method: QueryType; attribute?: string; values?: any } = {
      method: this.method,
    };

    if (this.attribute) {
      object.attribute = this.attribute;
    }

    if (this.isNested()) {
      object.values = (this.values as Query[]).map((value) => value.toObject());
    } else if (
      this.values.length > 0 ||
      (this.method !== QueryType.IsNull && this.method !== QueryType.IsNotNull)
    ) {
      object.values = this.values;
    }

    return object;
  }

  /**
   * Converts the Query object to its JSON string representation.
   *
   * @returns {string} The JSON string.
   * @throws {QueryException} If the object cannot be serialized to JSON.
   */
  public toString(): string {
    try {
      return JSON.stringify(this.toObject());
    } catch (e: unknown) {
      const errorMessage = e instanceof Error ? e.message : String(e);
      throw new QueryException(
        `Failed to serialize query to JSON: ${errorMessage}`,
      );
    }
  }

  /**
   * Creates an 'equal' query.
   * @param attribute - The attribute name.
   * @param values - Values to match.
   * @returns {Query}
   */
  public static equal(attribute: string, values: ScalarValue[]): Query {
    return new Query(QueryType.Equal, attribute, values);
  }

  /**
   * Creates a 'notEqual' query.
   * @param attribute - The attribute name.
   * @param value - Value not to match.
   * @returns {Query}
   */
  public static notEqual(attribute: string, value: ScalarValue): Query {
    return new Query(QueryType.NotEqual, attribute, [value]);
  }

  /**
   * Creates a 'lessThan' query.
   * @param attribute - The attribute name.
   * @param value - Value to compare against.
   * @returns {Query}
   */
  public static lessThan(attribute: string, value: number | string): Query {
    return new Query(QueryType.LessThan, attribute, [value]);
  }

  /**
   * Creates a 'lessThanEqual' query.
   * @param attribute - The attribute name.
   * @param value - Value to compare against.
   * @returns {Query}
   */
  public static lessThanEqual(
    attribute: string,
    value: number | string,
  ): Query {
    return new Query(QueryType.LessThanEqual, attribute, [value]);
  }

  /**
   * Creates a 'greaterThan' query.
   * @param attribute - The attribute name.
   * @param value - Value to compare against.
   * @returns {Query}
   */
  public static greaterThan(attribute: string, value: number | string): Query {
    return new Query(QueryType.GreaterThan, attribute, [value]);
  }

  /**
   * Creates a 'greaterThanEqual' query.
   * @param attribute - The attribute name.
   * @param value - Value to compare against.
   * @returns {Query}
   */
  public static greaterThanEqual(
    attribute: string,
    value: number | string,
  ): Query {
    return new Query(QueryType.GreaterThanEqual, attribute, [value]);
  }

  /**
   * Creates a 'contains' query.
   * @param attribute - The attribute name.
   * @param values - Values to check for containment.
   * @returns {Query}
   */
  public static contains(attribute: string, values: ScalarValue[]): Query {
    return new Query(QueryType.Contains, attribute, values);
  }

  /**
   * Creates a 'between' query.
   * @param attribute - The attribute name.
   * @param start - Start value (inclusive).
   * @param end - End value (inclusive).
   * @returns {Query}
   */
  public static between(
    attribute: string,
    start: ScalarValue,
    end: ScalarValue,
  ): Query {
    return new Query(QueryType.Between, attribute, [start, end]);
  }

  /**
   * Creates a 'search' query.
   * @param attribute - The attribute name.
   * @param value - Search string.
   * @returns {Query}
   */
  public static search(attribute: string, value: string): Query {
    return new Query(QueryType.Search, attribute, [value]);
  }

  /**
   * Creates a 'select' query for specific attributes.
   * @param attributes - Array of attribute names to select.
   * @returns {Query}
   */
  public static select(attributes: string[]): Query {
    return new Query(QueryType.Select, "", attributes); // Attributes are passed as values for 'select'
  }

  /**
   * Creates an 'orderDesc' query.
   * @param attribute - The attribute name to order by. Defaults to '$sequence'.
   * @returns {Query}
   */
  public static orderDesc(attribute: string = "$sequence"): Query {
    return new Query(QueryType.OrderDesc, attribute);
  }

  /**
   * Creates an 'orderAsc' query.
   * @param attribute - The attribute name to order by. Defaults to '$sequence'.
   * @returns {Query}
   */
  public static orderAsc(attribute: string = "$sequence"): Query {
    return new Query(QueryType.OrderAsc, attribute);
  }

  /**
   * Creates a 'limit' query.
   * @param value - The maximum number of results to return.
   * @returns {Query}
   */
  public static limit(value: number): Query {
    return new Query(QueryType.Limit, "", [value]);
  }

  /**
   * Creates an 'offset' query.
   * @param value - The number of results to skip.
   * @returns {Query}
   */
  public static offset(value: number): Query {
    return new Query(QueryType.Offset, "", [value]);
  }

  /**
   * Creates a 'cursorAfter' query for pagination.
   * @param value - The cursor value (typically a document ID).
   * @returns {Query}
   */
  public static cursorAfter(value: string | Doc<any>): Query {
    return new Query(QueryType.CursorAfter, "", [value as any]);
  }

  /**
   * Creates a 'cursorBefore' query for pagination.
   * @param value - The cursor value (typically a document ID).
   * @returns {Query}
   */
  public static cursorBefore(value: string | Doc<any>): Query {
    return new Query(QueryType.CursorBefore, "", [value as any]);
  }

  /**
   * Creates an 'isNull' query.
   * @param attribute - The attribute name to check for null.
   * @returns {Query}
   */
  public static isNull(attribute: string): Query {
    return new Query(QueryType.IsNull, attribute); // No values for IsNull
  }

  /**
   * Creates an 'isNotNull' query.
   * @param attribute - The attribute name to check for not null.
   * @returns {Query}
   */
  public static isNotNull(attribute: string): Query {
    return new Query(QueryType.IsNotNull, attribute); // No values for IsNotNull
  }

  /**
   * Creates a 'startsWith' query.
   * @param attribute - The attribute name.
   * @param value - The string prefix.
   * @returns {Query}
   */
  public static startsWith(attribute: string, value: string): Query {
    return new Query(QueryType.StartsWith, attribute, [value]);
  }

  /**
   * Creates an 'endsWith' query.
   * @param attribute - The attribute name.
   * @param value - The string suffix.
   * @returns {Query}
   */
  public static endsWith(attribute: string, value: string): Query {
    return new Query(QueryType.EndsWith, attribute, [value]);
  }

  /**
   * Creates an 'or' logical query.
   * @param queries - An array of nested Query objects to combine with OR.
   * @returns {Query}
   */
  public static or(queries: Query[]): Query {
    return new Query(QueryType.Or, "", queries);
  }

  /**
   * Creates an 'and' logical query.
   * @param queries - An array of nested Query objects to combine with AND.
   * @returns {Query}
   */
  public static and(queries: Query[]): Query {
    return new Query(QueryType.And, "", queries);
  }

  /**
   * Creates a 'populate' query to fetch related documents.
   * @param queries - An array of nested Query objects to specify the population criteria.
   * @returns {Query}
   */
  public static populate(attribute: string, queries?: Query[]): Query {
    return new Query(QueryType.Populate, attribute, queries);
  }

  /**
   * Filters an array of queries by their method types.
   *
   * @param queries - The array of Query objects to filter.
   * @param types - An array of QueryType values to filter by.
   * @returns {Query[]} A new array containing only the queries matching the specified types.
   */
  public static getByType(queries: Query[], types: QueryType[]): Query[] {
    if (types.some((type) => !Query.isMethod(type))) {
      throw new QueryException(
        "Invalid QueryType provided in types array for getByType.",
      );
    }
    const typesSet = new Set(types);
    return queries
      .filter((query) => typesSet.has(query.getMethod()))
      .map((query) => query.clone());
  }

  /**
   * Groups an array of queries into different categories for easier processing.
   *
   * @param queries - An array of Query objects to group.
   */
  public static groupByType(queries: Query[]): QueryByType {
    const filters: Query[] = [];
    const selections: Query[] = [];
    const populateQueries: Map<string, Query[]> = new Map();
    let limit: number | null = null;
    let offset: number | null = null;
    let cursor: Doc<any> | null = null;
    let cursorDirection: CursorEnum | null = null;
    const _orders: Record<string, OrderEnum> = {};

    for (const query of queries) {
      const method = query.getMethod();
      const attribute = query.getAttribute();
      const values = query.getValues();

      switch (method) {
        case QueryType.OrderAsc:
        case QueryType.OrderDesc:
          const order =
            method === QueryType.OrderAsc ? OrderEnum.Asc : OrderEnum.Desc;
          _orders[attribute] = order;
          break;
        case QueryType.Limit:
          if (limit === null && typeof values[0] === "number") {
            limit = values[0];
          }
          break;
        case QueryType.Offset:
          if (offset === null && typeof values[0] === "number") {
            offset = values[0];
          }
          break;
        case QueryType.CursorAfter:
        case QueryType.CursorBefore:
          cursor = (values[0] ?? null) as Doc<any> | null;
          cursorDirection =
            method === QueryType.CursorAfter
              ? CursorEnum.After
              : CursorEnum.Before;
          break;
        case QueryType.Select:
          selections.push(query.clone());
          break;
        case QueryType.And:
        case QueryType.Or:
          filters.push(query.clone());
          break;
        case QueryType.IsNull:
        case QueryType.IsNotNull:
          filters.push(query.clone());
          break;
        case QueryType.Populate:
          // TODO: May be we should throw error
          if (query.isNested()) {
            Logger.debug("Populate in nested query:", query.toString());
          }
          if (attribute) {
            const populateQuery = query.clone();
            populateQuery.setAttribute(attribute);
            populateQueries.set(attribute, values as Query[]);
          } else {
            Logger.warn(
              `Populate query without attribute: ${query.toString()}`,
            );
          }
          break;
        default:
          filters.push(query.clone());
          break;
      }
    }

    return {
      filters,
      selections,
      limit,
      offset,
      orders: _orders,
      cursor,
      cursorDirection,
      populateQueries,
    };
  }

  /**
   * Checks if the query method is a logical operator (AND or OR).
   * @returns {boolean} True if the query is a logical operator, false otherwise.
   */
  public isNested(): boolean {
    return this.method === QueryType.And || this.method === QueryType.Or;
  }

  /**
   * Checks if the query is intended to operate on an array attribute.
   * @returns {boolean}
   */
  public onArray(): boolean {
    return this._onArray;
  }

  /**
   * Sets whether the query is intended to operate on an array attribute.
   * @param bool - True if on array, false otherwise.
   */
  public setOnArray(bool: boolean): void {
    this._onArray = bool;
  }
}
