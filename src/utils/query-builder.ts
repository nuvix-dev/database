import { Doc } from "@core/doc.js";
import { CursorEnum } from "@core/enums.js";
import { Query, ScalarValue } from "@core/query.js";
import type { Entities } from "@nuvix/db";

type Attrs<T extends string> = T extends keyof Entities
  ? string & keyof Entities[T]
  : string;

/**
 * A fluent builder class for constructing an array of Query objects.
 * This class provides a chainable API for creating complex queries.
 */
export class QueryBuilder<T extends string = string> {
  private queries: Query[] = [];

  constructor() {}

  /**
   * Creates a new QueryBuilder instance from an existing array of Query objects.
   * The incoming queries are cloned to ensure the builder is mutable without side effects.
   *
   * @param queries - An array of Query objects to start the builder with.
   * @returns {QueryBuilder} A new QueryBuilder instance.
   */
  static from(queries: Query[]): QueryBuilder {
    const builder = new QueryBuilder();
    builder.queries = queries.map((query) => query.clone());
    return builder;
  }

  /**
   * Adds an equality condition.
   * @param attribute - The attribute name.
   * @param values - A variadic list of values to match.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  equal(attribute: Attrs<T>, ...values: ScalarValue[]): this {
    this.queries.push(Query.equal(attribute, values));
    return this;
  }

  /**
   * Adds a not equal condition.
   * @param attribute - The attribute name.
   * @param value - The value to exclude.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  notEqual(attribute: Attrs<T>, value: ScalarValue): this {
    this.queries.push(Query.notEqual(attribute, value));
    return this;
  }

  /**
   * Adds a less than condition.
   * @param attribute - The attribute name.
   * @param value - The upper bound value.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  lessThan(attribute: Attrs<T>, value: number | string): this {
    this.queries.push(Query.lessThan(attribute, value));
    return this;
  }

  /**
   * Adds a less than or equal condition.
   * @param attribute - The attribute name.
   * @param value - The upper bound value.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  lessThanEqual(attribute: Attrs<T>, value: number | string): this {
    this.queries.push(Query.lessThanEqual(attribute, value));
    return this;
  }

  /**
   * Adds a greater than condition.
   * @param attribute - The attribute name.
   * @param value - The lower bound value.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  greaterThan(attribute: Attrs<T>, value: number | string): this {
    this.queries.push(Query.greaterThan(attribute, value));
    return this;
  }

  /**
   * Adds a greater than or equal condition.
   * @param attribute - The attribute name.
   * @param value - The lower bound value.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  greaterThanEqual(attribute: Attrs<T>, value: number | string): this {
    this.queries.push(Query.greaterThanEqual(attribute, value));
    return this;
  }

  /**
   * Adds a contains condition (checks if an array attribute contains values).
   * @param attribute - The attribute name.
   * @param values - A variadic list of values to check for containment.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  contains(attribute: Attrs<T>, ...values: ScalarValue[]): this {
    this.queries.push(Query.contains(attribute, values));
    return this;
  }

  /**
   * Adds a between condition.
   * @param attribute - The attribute name.
   * @param start - The start value (inclusive).
   * @param end - The end value (inclusive).
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  between(attribute: Attrs<T>, start: ScalarValue, end: ScalarValue): this {
    this.queries.push(Query.between(attribute, start, end));
    return this;
  }

  /**
   * Adds a full-text search condition.
   * @param attribute - The attribute name.
   * @param value - The search term.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  search(attribute: Attrs<T>, value: string): this {
    this.queries.push(Query.search(attribute, value));
    return this;
  }

  /**
   * Selects specific attributes to return.
   * @param attributes - A variadic list of attributes to select.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  select(...attributes: Attrs<T>[]): this {
    this.queries.push(Query.select(attributes));
    return this;
  }

  /**
   * Adds a descending order sort condition.
   * @param attribute - The attribute to sort by. Defaults to '$sequence'.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  orderDesc(attribute: Attrs<T> = "" as Attrs<T>): this {
    this.queries.push(Query.orderDesc(attribute));
    return this;
  }

  /**
   * Adds an ascending order sort condition.
   * @param attribute - The attribute to sort by. Defaults to '$sequence'.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  orderAsc(attribute: Attrs<T> = "" as Attrs<T>): this {
    this.queries.push(Query.orderAsc(attribute));
    return this;
  }

  /**
   * Sets a result limit.
   * @param value - The maximum number of results.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  limit(value: number): this {
    this.queries.push(Query.limit(value));
    return this;
  }

  /**
   * Sets a result offset.
   * @param value - The number of results to skip.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  offset(value: number): this {
    this.queries.push(Query.offset(value));
    return this;
  }

  /**
   * Sets a cursor position after a given document or ID.
   * @param value - The cursor document or ID.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  cursorAfter(value: Doc<any> | string): this {
    this.queries.push(Query.cursorAfter(value));
    return this;
  }

  /**
   * Sets a cursor position before a given document or ID.
   * @param value - The cursor document or ID.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  cursorBefore(value: Doc<any> | string): this {
    this.queries.push(Query.cursorBefore(value));
    return this;
  }

  /**
   * Adds a condition to check if an attribute is null.
   * @param attribute - The attribute name.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  isNull(attribute: Attrs<T>): this {
    this.queries.push(Query.isNull(attribute));
    return this;
  }

  /**
   * Adds a condition to check if an attribute is not null.
   * @param attribute - The attribute name.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  isNotNull(attribute: Attrs<T>): this {
    this.queries.push(Query.isNotNull(attribute));
    return this;
  }

  /**
   * Adds a condition to check if an attribute value starts with a string.
   * @param attribute - The attribute name.
   * @param value - The string prefix.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  startsWith(attribute: Attrs<T>, value: string): this {
    this.queries.push(Query.startsWith(attribute, value));
    return this;
  }

  /**
   * Adds a condition to check if an attribute value ends with a string.
   * @param attribute - The attribute name.
   * @param value - The string suffix.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  endsWith(attribute: Attrs<T>, value: string): this {
    this.queries.push(Query.endsWith(attribute, value));
    return this;
  }

  /**
   * Adds an OR group of conditions.
   * @param builderFn - A function that receives a new QueryBuilder instance to define nested conditions.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  or(builderFn: (builder: QueryBuilder) => void): this {
    const nestedBuilder = new QueryBuilder();
    builderFn(nestedBuilder);
    this.queries.push(Query.or(nestedBuilder.build()));
    return this;
  }

  /**
   * Adds an AND group of conditions.
   * @param builderFn - A function that receives a new QueryBuilder instance to define nested conditions.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  and(builderFn: (builder: QueryBuilder) => void): this {
    const nestedBuilder = new QueryBuilder();
    builderFn(nestedBuilder);
    this.queries.push(Query.and(nestedBuilder.build()));
    return this;
  }

  /**
   * Adds a nested populate query.
   * @param attribute - The attribute to populate.
   * @param builderFn - A function that receives a new QueryBuilder instance to define the nested query.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  populate(
    attribute: Attrs<T>,
    builderFn?: (builder: QueryBuilder) => void,
  ): this {
    const nestedBuilder = new QueryBuilder();
    builderFn?.(nestedBuilder);
    this.queries.push(Query.populate(attribute, nestedBuilder.build()));
    return this;
  }

  /**
   * Finalizes the build process and returns the constructed array of queries.
   *
   * @returns {Query[]} A new array containing the built Query objects.
   */
  build(): Query[] {
    return [...this.queries];
  }

  /**
   * Clears the current builder state, resetting it for a new query.
   *
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  clear(): this {
    this.queries = [];
    return this;
  }

  /**
   * Adds pagination parameters in a single call.
   * @param limit - The number of results per page.
   * @param offset - The page offset.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  paginate(limit: number, offset: number): this {
    return this.limit(limit).offset(offset);
  }

  /**
   * Adds ID-based cursor pagination.
   * @param cursorId - The cursor document ID.
   * @param direction - The pagination direction ('after' or 'before'). Defaults to 'after'.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  cursorPaginate(
    cursorId: Doc<any> | string,
    direction: CursorEnum = CursorEnum.After,
  ): this {
    return direction === CursorEnum.After
      ? this.cursorAfter(cursorId)
      : this.cursorBefore(cursorId);
  }

  /**
   * Adds a full-text search across multiple fields with an OR condition.
   * @param fields - The fields to search.
   * @param term - The search term.
   * @returns {this} The current QueryBuilder instance for chaining.
   */
  multiSearch(fields: Attrs<T>[], term: string): this {
    const nestedQueries: Query[] = fields.map((field) =>
      Query.search(field, term),
    );
    this.queries.push(Query.or(nestedQueries));
    return this;
  }
}
