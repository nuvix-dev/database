import { Attribute, Index } from "@validators/schema.js";
import { Doc } from "@core/doc.js";

export interface CreateCollectionOptions {
  name: string;
  attributes: Doc<Attribute>[];
  indexes?: Doc<Index>[];
}
