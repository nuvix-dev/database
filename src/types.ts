import { type Database } from "@core/database.js";
import { Permission } from "@utils/permission.js";
import { Attribute, Index } from "@validators/schema.js";

export interface MetadataEntity {
  $id: string;
  $collection: string;
  $schema?: string;
  name: string;
  attributes: Attribute[];
  indexes: Index[];
  documentSecurity: boolean;
  enabled: boolean;
}

export interface Entities {
  [Database.METADATA]: MetadataEntity;
}

export interface IEntity {
  $id: string;
  $createdAt: Date;
  $updatedAt: Date;
  $permissions: string[];
  $sequence: number;
  $collection: string;
  $tenant?: number | null; // Optional tenant ID for multi-tenant support
  $schema?: string; // Optional schema where the entity is stored
}

export type IEntityInput = {
  $id?: string;
  $createdAt?: Date | null;
  $updatedAt?: Date | null;
  $permissions?: (Permission | string)[];
  $sequence?: number;
  $collection?: string;
  $tenant?: number | null;
};
