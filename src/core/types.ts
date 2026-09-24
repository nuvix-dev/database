import { Attribute, Index } from "@validators/schema.js";
import type { Session } from "./session.js";
import type { Doc } from "./doc.js";
import { Permission } from "@utils/permission.js";
import { Query } from "./query.js";
import {
  OnDelete,
  RelationEnum,
  type CursorEnum,
  type OrderEnum,
} from "./enums.js";
import { IEntity } from "../types.js";

export type FilterValue =
  | string
  | number
  | boolean
  | Date
  | null
  | undefined
  | object
  | any[]
  | Record<string, any>
  | FilterValue[];

export type Filter<T = FilterValue, U = FilterValue, D = Doc> = {
  encode: (
    value: T,
    document: D,
    db?: Session,
    Database?: any,
  ) => U | Promise<U>;
  decode: (
    value: U,
    document: D,
    db?: Session,
    Database?: any,
  ) => T | Promise<T>;
};

export type Filters = Record<string, Filter>;

export type CreateCollection = {
  id: string;
  attributes?: Doc<Attribute>[];
  indexes?: Doc<Index>[];
  permissions?: (Permission | string)[];
  documentSecurity?: boolean;
  enabled?: boolean;
};

export type UpdateCollection = {
  id: string;
  permissions: (Permission | string)[];
  documentSecurity: boolean;
  enabled?: boolean;
};

export type QueryByType = {
  filters: Query[];
  selections: Query[];
  limit: number | null;
  offset: number | null;
  orders: Record<string, OrderEnum>;
  cursor: Doc<IEntity> | null;
  cursorDirection: CursorEnum | null;
  populateQueries: Map<string, Query[]>;
};

export type CreateRelationshipAttribute = {
  collectionId: string;
  relatedCollectionId: string;
  type: RelationEnum;
  twoWay?: boolean;
  id?: string;
  twoWayKey?: string;
  onDelete?: OnDelete;
};
export type UpdateRelationshipAttribute = {
  collectionId: string;
  id: string;
  newKey?: string;
  newTwoWayKey?: string;
  twoWay?: boolean;
  onDelete?: OnDelete;
};

export type RelationshipUpdates = {
  set?: string[];
  connect?: string[];
  disconnect?: string[];
};

export interface RelationshipContext {
  visited: Set<string>;
  skipRelationships?: boolean;
}
