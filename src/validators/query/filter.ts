import { Query, QueryType, ScalarValue } from "../../core/query.js";
import { Base, MethodType } from "./base.js";
import { Datetime as DatetimeValidator } from "../datetime.js";
import { Attribute } from "@validators/schema.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, RelationEnum, RelationSideEnum } from "@core/enums.js";
import { Integer } from "@validators/Integer.js";
import { FloatValidator } from "@validators/float-validator.js";
import { Boolean } from "@validators/boolean.js";
import { Text } from "@validators/text.js";

/**
 * Validates a Query object to ensure it is a valid filter query
 * based on a provided schema of attributes.
 */
export class Filter extends Base {
  protected message: string = "Invalid filter query";
  protected schema: Record<string, Attribute> = {};
  protected maxValuesCount: number;
  protected minAllowedDate: Date;
  protected maxAllowedDate: Date;

  /**
   * @param attributes - An array of Document objects representing the attribute schema.
   * @param maxValuesCount - The maximum number of values allowed in a single query (e.g., for `equal` or `in`).
   * @param minAllowedDate - The minimum date value allowed for datetime attributes.
   * @param maxAllowedDate - The maximum date value allowed for datetime attributes.
   */
  constructor(
    attributes: Doc<Attribute>[] = [],
    maxValuesCount: number = 100,
    minAllowedDate: Date = new Date("0000-01-01"),
    maxAllowedDate: Date = new Date("9999-12-31"),
  ) {
    super();
    if (maxValuesCount < 1) {
      throw new Error("maxValuesCount must be a positive integer.");
    }
    this.maxValuesCount = maxValuesCount;
    this.minAllowedDate = minAllowedDate;
    this.maxAllowedDate = maxAllowedDate;

    for (const attribute of attributes) {
      const attributeKey = attribute.get("$id") as string;
      if (attributeKey) {
        this.schema[attributeKey] = attribute.toObject();
      }
    }
  }

  /**
   * Check if a Query object is valid according to the filter rules.
   * This is the main public validation method.
   *
   * @param value - The Query object to validate.
   * @returns {boolean} True if the query is a valid filter, false otherwise.
   */
  public $valid(value: unknown): boolean {
    this.message = "Invalid filter query"; // Reset message at the beginning

    if (!(value instanceof Query)) {
      this.message = "Value must be a Query object.";
      return false;
    }

    const method = value.getMethod();
    const attribute = value.getAttribute();

    switch (method) {
      case QueryType.Equal:
      case QueryType.NotContains:
      case QueryType.Contains: {
        const values = value.getValues() as ScalarValue[];
        if (this.isEmpty(values)) {
          this.message = `${method} queries require at least one value.`;
          return false;
        }
        return this.validateAttributeAndValues(
          attribute,
          values,
          method,
          value,
        );
      }

      case QueryType.NotEqual:
      case QueryType.LessThan:
      case QueryType.LessThanEqual:
      case QueryType.GreaterThan:
      case QueryType.GreaterThanEqual:
      case QueryType.Search:
      case QueryType.NotSearch:
      case QueryType.NotStartsWith:
      case QueryType.NotEndsWith:
      case QueryType.StartsWith:
      case QueryType.EndsWith: {
        const values = value.getValues() as ScalarValue[];
        if (values.length !== 1) {
          this.message = `${method} queries require exactly one value.`;
          return false;
        }
        return this.validateAttributeAndValues(
          attribute,
          values,
          method,
          value,
        );
      }

      case QueryType.NotBetween:
      case QueryType.Between: {
        const values = value.getValues() as ScalarValue[];
        if (values.length !== 2) {
          this.message = `${method} queries require exactly two values.`;
          return false;
        }
        return this.validateAttributeAndValues(
          attribute,
          values,
          method,
          value,
        );
      }

      case QueryType.IsNull:
      case QueryType.IsNotNull:
        // These queries don't have values to validate, just the attribute.
        return this.validateAttributeAndValues(attribute, [], method, value);

      case QueryType.Or:
      case QueryType.And: {
        const values = value.getValues() as Query[];
        if (!Array.isArray(values) || values.length < 2) {
          this.message = `${method} queries require an array of at least two nested queries.`;
          return false;
        }

        const nestedQueries = Query.groupByType(values).filters;
        if (values.length !== nestedQueries.length) {
          this.message = `${method} queries can only contain filter queries.`;
          return false;
        }
        return true;
      }

      default:
        this.message = `Unknown or unsupported filter method: "${method}".`;
        return false;
    }
  }

  /**
   * Returns the method type handled by this validator.
   * @returns {MethodType.Filter} The string literal 'filter'.
   */
  public getMethodType(): MethodType {
    return MethodType.Filter;
  }

  /**
   * Checks if the attribute exists in the schema and is a valid target for filtering.
   * @param attribute - The name of the attribute.
   * @returns {boolean} True if the attribute is valid, false otherwise.
   */
  protected validateAttributeSchema(attribute: string): boolean {
    if (attribute.includes("->>") && !this.schema[attribute]) {
      attribute = attribute.split("->>")[0]!;
    } else if (attribute.includes("->") && !this.schema[attribute]) {
      attribute = attribute.split("->")[0]!;
    }
    const attributeSchema = this.schema[attribute!];
    if (!attributeSchema) {
      this.message = `Attribute not found in schema: "${attribute}".`;
      return false;
    }

    if (
      Array.isArray(attributeSchema.filters) &&
      attributeSchema.filters.includes("encrypt")
    ) {
      this.message = `Cannot query encrypted attribute: ${attribute}`;
      return false;
    }

    // TODO: recheck
    if (attribute.includes(".") && !this.schema[attribute]) {
      this.message = "Cannot query on nested attributes.";
      return false;
    }

    if (attributeSchema.type === AttributeEnum.Relationship) {
      const { relationType, twoWay, side } = attributeSchema.options ?? {};

      if (
        (relationType === RelationEnum.OneToOne &&
          !twoWay &&
          side === RelationSideEnum.Child) ||
        (relationType === RelationEnum.OneToMany &&
          side === RelationSideEnum.Parent) ||
        (relationType === RelationEnum.ManyToOne &&
          side === RelationSideEnum.Child) ||
        relationType === RelationEnum.ManyToMany
      ) {
        this.message = "Cannot query on virtual relationship attribute.";
        return false;
      }
    }
    if (attributeSchema.type === AttributeEnum.Virtual) {
      this.message = "Cannot query on virtual attribute: " + attribute;
      return false;
    }
    return true;
  }

  /**
   * Validates both the attribute's existence and the types/count of its values.
   * @param attribute - The attribute name from the query.
   * @param values - The array of values from the query.
   * @param method - The query method type.
   * @returns {boolean} True if both attribute and values are valid, false otherwise.
   */
  protected validateAttributeAndValues(
    attribute: string,
    values: ScalarValue[],
    method: QueryType,
    query: Query,
  ): boolean {
    if (!this.validateAttributeSchema(attribute)) {
      return false;
    }

    if (attribute.includes("->>") && !this.schema[attribute]) {
      attribute = attribute.split("->>")[0]!;
    } else if (attribute.includes("->") && !this.schema[attribute]) {
      attribute = attribute.split("->")[0]!;
    }

    const attributeSchema = this.schema[attribute]!;

    if (values.length > this.maxValuesCount) {
      this.message = `Query on attribute "${attribute}" has more than ${this.maxValuesCount} values.`;
      return false;
    }

    if (!this.validateMethodVsAttributeType(attributeSchema, method, query)) {
      return false;
    }

    return this.validateValuesAgainstSchema(attribute, attributeSchema, values);
  }

  /**
   * Checks if the query method is valid for the attribute's type (e.g., `contains` on an array/string).
   * @param attributeSchema - The schema object for the attribute.
   * @param method - The query method.
   * @returns {boolean} True if the method is allowed for the attribute, false otherwise.
   */
  protected validateMethodVsAttributeType(
    attributeSchema: any,
    method: QueryType,
    query: Query,
  ): boolean {
    const isArray = attributeSchema.array ?? false;

    if (isArray) {
      query.setOnArray(true); // I Know this is not the best place, but it is a temporary solution. we will refactor this later.
    }

    if (
      !isArray &&
      [QueryType.NotContains, QueryType.Contains].includes(method) &&
      attributeSchema.type !== AttributeEnum.String &&
      attributeSchema.type !== AttributeEnum.Json
    ) {
      this.message = `Cannot use "${method}" on attribute "${attributeSchema.key}" because it is not an array or string.`;
      return false;
    }

    if (
      isArray &&
      ![
        QueryType.Contains,
        QueryType.NotContains,
        QueryType.IsNull,
        QueryType.IsNotNull,
      ].includes(method)
    ) {
      this.message = `Cannot use "${method}" on attribute "${attributeSchema.key}" because it is an array.`;
      return false;
    }
    return true;
  }

  /**
   * Validates an array of values against the attribute's type in the schema.
   * @param attribute - The attribute key.
   * @param attributeSchema - The schema object for the attribute.
   * @param values - An array of values to validate.
   * @returns {boolean} True if all values are valid, false otherwise.
   */
  protected validateValuesAgainstSchema(
    attribute: string,
    attributeSchema: Attribute,
    values: ScalarValue[],
  ): boolean {
    const attributeType = attributeSchema.type;
    let validator:
      | DatetimeValidator
      | Integer
      | FloatValidator
      | Boolean
      | Text
      | null = null;

    switch (attributeType) {
      case AttributeEnum.String:
      case AttributeEnum.Relationship:
        validator = new Text(attributeSchema.size ?? 0, 0);
        break;
      case AttributeEnum.Integer:
        validator = new Integer();
        break;
      case AttributeEnum.Float:
        validator = new FloatValidator();
        break;
      case AttributeEnum.Boolean:
        validator = new Boolean();
        break;
      case AttributeEnum.Timestamptz:
        validator = new DatetimeValidator(
          this.minAllowedDate,
          this.maxAllowedDate,
        );
        break;
      case AttributeEnum.Json:
        break; // JSON can be any valid JSON value, no specific validator needed.
      default:
        this.message = `Unknown data type for attribute "${attribute}".`;
        return false;
    }

    for (const value of values) {
      if (validator && !validator?.$valid(value)) {
        this.message = `Value "${value}" is invalid for attribute "${attribute}" of type "${attributeType}".`;
        return false;
      }
    }
    return true;
  }

  /**
   * Helper to check if an array of values is empty.
   * @param values - The array of values.
   * @returns {boolean} True if the array is empty or contains only an empty array.
   */
  protected isEmpty(values: ScalarValue[]): boolean {
    return (
      values.length === 0 ||
      (Array.isArray(values[0]) && values[0].length === 0)
    );
  }
}
