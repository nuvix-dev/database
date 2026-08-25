import { describe, test, expect, beforeEach, afterEach } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum, RelationEnum, OnDelete } from "@core/enums.js";
import { Attribute } from "@validators/schema.js";
import { Authorization } from "@utils/authorization.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";

/**
 * Regression tests for authorization consistency in internal
 * relationship maintenance (WS-F2).
 *
 * Contract under test:
 * - Public entry points (create/update/delete/find) enforce caller
 *   permissions normally.
 * - INTERNAL relationship maintenance (cascade, sever, FK updates,
 *   junction rows) runs under Authorization.skip so it is atomic and
 *   never partially applied for restricted callers.
 * - Aggregates (count/sum) agree for privileged callers.
 */
describe("Relationship authorization integrity", () => {
  let db: Database;
  const schema = new Date().getTime().toString();

  const restrictAs = (role: string) => {
    Authorization.cleanRoles();
    Authorization.setRole(role);
    Authorization.setDefaultStatus(true);
  };

  const release = () => {
    Authorization.cleanRoles();
    Authorization.setDefaultStatus(false);
  };

  beforeEach(async () => {
    release();
    db = createTestDb({ namespace: `rel_auth_${schema}` });
    db.setMeta({ schema });
    await db.create();
  });

  afterEach(async () => {
    release();
    await db.delete();
  });

  const strAttr = (key: string, size = 255): Doc<Attribute> =>
    new Doc<Attribute>({
      $id: key,
      key,
      type: AttributeEnum.String,
      size,
      required: false,
    });

  test("cascade deletes ALL children even when caller cannot access them", async () => {
    const ts = Date.now();
    const authorsId = `authors_${ts}`;
    const postsId = `posts_${ts}`;

    // Authors: alice may delete at collection level.
    await db.createCollection({
      id: authorsId,
      attributes: [strAttr("name")],
      permissions: [
        Permission.create(Role.any()),
        Permission.read(Role.any()),
        Permission.delete(Role.user("alice")),
      ],
    });

    // Posts: document security ON, zero collection permissions.
    // Alice can neither see nor modify posts directly.
    await db.createCollection({
      id: postsId,
      attributes: [strAttr("title")],
      permissions: [],
      documentSecurity: true,
    });

    await db.createRelationship({
      collectionId: authorsId,
      relatedCollectionId: postsId,
      type: RelationEnum.OneToMany,
      id: "posts",
      twoWayKey: "author",
      onDelete: OnDelete.Cascade,
    });

    const author = await db.createDocument(
      authorsId,
      new Doc({ name: "Alice" }),
    );
    const post1 = await Authorization.skip(() =>
      db.createDocument(
        postsId,
        new Doc({ title: "P1", author: author.getId() }),
      ),
    );
    const post2 = await Authorization.skip(() =>
      db.createDocument(
        postsId,
        new Doc({ title: "P2", author: author.getId() }),
      ),
    );

    // Act as restricted caller.
    restrictAs("user:alice");
    await db.deleteDocument(authorsId, author.getId());

    // Assert as admin: BOTH posts must be gone (no partial cascade).
    await Authorization.skip(async () => {
      const p1 = await db.getDocument(postsId, post1.getId());
      const p2 = await db.getDocument(postsId, post2.getId());
      expect(p1.empty()).toBe(true);
      expect(p2.empty()).toBe(true);
    });
  });

  test("ManyToMany cascade removes linked documents for restricted callers", async () => {
    const ts = Date.now();
    const postsId = `posts_${ts}`;
    const tagsId = `tags_${ts}`;

    await db.createCollection({
      id: postsId,
      attributes: [strAttr("title")],
      permissions: [
        Permission.create(Role.any()),
        Permission.read(Role.any()),
        Permission.delete(Role.user("alice")),
      ],
    });

    await db.createCollection({
      id: tagsId,
      attributes: [strAttr("name", 100)],
      permissions: [],
      documentSecurity: true,
    });

    await db.createRelationship({
      collectionId: postsId,
      relatedCollectionId: tagsId,
      type: RelationEnum.ManyToMany,
      id: "tags",
      twoWayKey: "posts",
      onDelete: OnDelete.Cascade,
    });

    const post = await db.createDocument(postsId, new Doc({ title: "P" }));
    const tag1 = await Authorization.skip(() =>
      db.createDocument(tagsId, new Doc({ name: "T1" })),
    );
    const tag2 = await Authorization.skip(() =>
      db.createDocument(tagsId, new Doc({ name: "T2" })),
    );
    await Authorization.skip(() =>
      db.updateDocument(
        postsId,
        post.getId(),
        new Doc({
          tags: { set: [tag1.getId(), tag2.getId()] },
        }),
      ),
    );

    restrictAs("user:alice");
    await db.deleteDocument(postsId, post.getId());

    await Authorization.skip(async () => {
      expect((await db.getDocument(tagsId, tag1.getId())).empty()).toBe(true);
      expect((await db.getDocument(tagsId, tag2.getId())).empty()).toBe(true);
    });
  });

  test("OneToOne re-link clears old partner FK despite no update rights on partner collection", async () => {
    const ts = Date.now();
    const membersId = `members_${ts}`;
    const seatsId = `seats_${ts}`;

    await db.createCollection({
      id: membersId,
      attributes: [strAttr("name")],
      permissions: [
        Permission.create(Role.user("alice")),
        Permission.read(Role.user("alice")),
        Permission.update(Role.user("alice")),
        Permission.delete(Role.user("alice")),
      ],
    });

    await db.createCollection({
      id: seatsId,
      attributes: [strAttr("label", 100)],
      permissions: [],
      documentSecurity: true,
    });

    await db.createRelationship({
      collectionId: membersId,
      relatedCollectionId: seatsId,
      type: RelationEnum.OneToOne,
      id: "seat",
      twoWayKey: "member",
      twoWay: true,
    });

    const m1 = await db.createDocument(membersId, new Doc({ name: "M1" }));
    const s1 = await Authorization.skip(() =>
      db.createDocument(seatsId, new Doc({ label: "S1", member: m1.getId() })),
    );
    // S2 is readable by alice (existence check enforces read),
    // but the seats COLLECTION grants her nothing else.
    const s2 = await Authorization.skip(() =>
      db.createDocument(
        seatsId,
        new Doc({
          label: "S2",
          $permissions: [Permission.read(Role.user("alice"))],
        }),
      ),
    );

    restrictAs("user:alice");
    await db.updateDocument(
      membersId,
      m1.getId(),
      new Doc({
        seat: s2.getId(),
      }),
    );

    // Assert as admin: old partner released, new partner linked.
    // Relationship values are only visible via populate().
    await Authorization.skip(async () => {
      const s1After = await db.getDocument(seatsId, s1.getId(), (qb) =>
        qb.populate("member"),
      );
      const s2After = await db.getDocument(seatsId, s2.getId(), (qb) =>
        qb.populate("member"),
      );
      expect(s1After.get("member")).toBeNull();
      const linked = s2After.get("member") as Doc;
      expect(linked?.getId()).toBe(m1.getId());
    });
  });

  test("count and sum agree for callers with collection-level read", async () => {
    const ts = Date.now();
    const metricsId = `metrics_${ts}`;

    await db.createCollection({
      id: metricsId,
      attributes: [
        new Doc<Attribute>({
          $id: "value",
          key: "value",
          type: AttributeEnum.Integer,
          size: 8,
          required: true,
        }),
      ],
      permissions: [Permission.read(Role.user("bob"))],
      documentSecurity: true,
    });

    // Two docs readable by bob, one hidden from him at DOCUMENT level.
    await db.createDocument(
      metricsId,
      new Doc({
        value: 10,
        $permissions: [Permission.read(Role.user("bob"))],
      }),
    );
    await db.createDocument(
      metricsId,
      new Doc({
        value: 20,
        $permissions: [Permission.read(Role.user("bob"))],
      }),
    );
    await db.createDocument(metricsId, new Doc({ value: 30 }));

    restrictAs("user:bob");

    // Collection-level read grant ⇒ full visibility, same as find().
    const total = await db.count(metricsId);
    expect(total).toBe(3);

    const summed = await db.sum(metricsId, "value");
    expect(summed).toBe(60);
  });

  test("quote escapes embedded double quotes", () => {
    const adapter = db.getAdapter();
    expect(adapter.quote("plain")).toBe('"plain"');
    expect(adapter.quote('has"quote')).toBe('"has""quote"');
    expect(adapter.quote('a"b"c')).toBe('"a""b""c"');
  });

  test("increaseDocumentAttribute honors independent min and max bounds", async () => {
    const ts = Date.now();
    const countersId = `counters_${ts}`;

    await db.createCollection({
      id: countersId,
      attributes: [
        new Doc<Attribute>({
          $id: "n",
          key: "n",
          type: AttributeEnum.Integer,
          size: 8,
          required: true,
        }),
      ],
    });

    const doc = await db.createDocument(countersId, new Doc({ n: 5 }));
    const adapter = db.getAdapter();
    const bump = (value: number, min?: number, max?: number) =>
      adapter.increaseDocumentAttribute({
        collection: countersId,
        id: doc.getId(),
        attribute: "n",
        updatedAt: new Date(),
        value,
        min,
        max,
      });
    // Direct adapter calls bypass DB-level cache invalidation.
    const readN = async () => {
      await db.purgeCachedDocument(countersId, doc.getId());
      const raw = (await db.getDocument(countersId, doc.getId())).get("n");
      return Number(raw);
    };

    // Bounds filter rows by their CURRENT value (no result clamping):
    // an update applies only when min <= n <= max holds pre-update.

    // n=5 within [0, ∞): applies, crossing below min (documented semantics).
    await bump(-10, 0);
    expect(await readN()).toBe(-5);

    // n=-5 outside [0, ∞): min alone blocks the update.
    // Regression for the `min !== null` fix — previously a min-only bound
    // was silently ignored (guard checked `max`) and this would apply.
    await bump(-5, 0);
    expect(await readN()).toBe(-5);

    // n=-5 within (∞, 50]: applies, crossing above max.
    await bump(100, undefined, 50);
    expect(await readN()).toBe(95);

    // n=95 outside (∞, 50]: max alone blocks the update.
    await bump(1, undefined, 50);
    expect(await readN()).toBe(95);

    // Absent max must NOT disable the min guard (regression for the
    // copy-pasted `max !== null` condition on the min clause).
    await bump(-200, 100);
    expect(await readN()).toBe(95);

    // In-range without bounds: applied.
    await bump(2);
    expect(await readN()).toBe(97);
  });
});
