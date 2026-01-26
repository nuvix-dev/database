import {
  AttributeEnum,
  IndexEnum,
  OnDelete,
  RelationEnum,
  RelationSideEnum,
} from "@core/enums.js";

export type AttributeOptions = {
  relationType: RelationEnum;
  side: RelationSideEnum;
  relatedCollection: string;
  twoWay?: boolean; // default false, optional
  twoWayKey?: string;
  onDelete: OnDelete;
};

export type Attribute = {
  $id: string;
  key: string;
  type: AttributeEnum;
  size?: number; // default 0, optional
  required?: boolean; // default false, optional
  array?: boolean; // default false, optional
  filters?: string[]; // default [], optional
  format?: string;
  formatOptions?: Record<string, any>;
  default?: any;
  options?: AttributeOptions | Record<string, any>;
  __type?: string; // internal use only
};

export type Index = {
  $id: string;
  key?: string;
  type: IndexEnum;
  attributes?: string[];
  orders?: (string | null)[];
};

export type Collection = {
  $id: string;
  $collection: string;
  $schema?: string;
  name: string;
  attributes: Attribute[];
  indexes?: Index[];
  documentSecurity?: boolean; // default false, optional
  enabled?: boolean; // default true, optional
};

export type { AttributeOptions as RelationOptions };
