import type { Doc, Entities, IEntity, Session } from "@nuvix/db";

interface TestCollection extends IEntity {
  required_field: string;
  optional_field?: string;
}

declare module "@nuvix/db" {
  interface Entities {
    test: TestCollection;
  }
}

type Equal<Left, Right> =
  (<Value>() => Value extends Left ? 1 : 2) extends
  (<Value>() => Value extends Right ? 1 : 2)
    ? true
    : false;
type Expect<Value extends true> = Value;

declare const session: Session;

type AugmentedRegistry = Expect<Equal<Entities["test"], TestCollection>>;

const typedDocuments = session.find("test", (query) => {
  query.equal("required_field", "value");

  // @ts-expect-error Generated query builders reject unknown attributes.
  query.equal("missing_field", "value");

  return query;
});

type TypedFindResult = Expect<
  Equal<Awaited<typeof typedDocuments>, Doc<TestCollection>[]>
>;

const typedDocument = session.getDocument("test", "document-id");
type TypedGetResult = Expect<
  Equal<Awaited<typeof typedDocument>, Doc<TestCollection>>
>;

declare const testDocument: TestCollection;
const createdDocument = session.createDocument("test", testDocument);
type TypedCreateResult = Expect<
  Equal<Awaited<typeof createdDocument>, Doc<TestCollection>>
>;

// @ts-expect-error Known registries reject unknown literal collection IDs.
session.find("missing");

// @ts-expect-error Known registries reject unknown literal collection IDs.
session.getDocument("missing", "document-id");

// @ts-expect-error Known registries reject unknown literal collection IDs.
session.createDocument("missing", testDocument);

declare const arbitraryCollection: string;
const untypedDocuments = session.find(arbitraryCollection);
type UntypedFindResult = Expect<
  Equal<
    Awaited<typeof untypedDocuments>,
    Doc<Partial<IEntity> & Record<string, any>>[]
  >
>;

// Keep compile-time assertions live under noUnusedLocals-independent configs.
export type GeneratedTypesAssertions =
  | AugmentedRegistry
  | TypedCreateResult
  | TypedFindResult
  | TypedGetResult
  | UntypedFindResult;
