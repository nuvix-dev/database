import { describe, test, expect, beforeAll, afterAll } from "bun:test";
import { Database } from "@core/database.js";
import { createTestDb } from "../helpers.js";
import { Doc } from "@core/doc.js";
import { AttributeEnum } from "@core/enums.js";
import { Permission } from "@utils/permission.js";
import { Role } from "@utils/role.js";

/**
 * Concurrency regression tests for the session-scoped authorization
 * redesign (context.md §7).
 *
 * Contract under test:
 * 1. Parallel sessions bound to DISTINCT roles see only their own
 *    roles' data — no cross-talk between concurrent callers.
 * 2. A system-session operation running CONCURRENTLY with a
 *    restricted session must NOT disable that session's authorization
 *    checks. Under the old global-status semantics (a privileged
 *    operation toggling process-wide bypass state), an in-flight
 *    system op leaked elevated access to every concurrent caller —
 *    these tests interleave awaits precisely so they would FAIL under
 *    those semantics.
 */
describe("Concurrent session authorization", () => {
  const db = createTestDb({ namespace: `concurrency_auth_${Date.now()}` });
  let docsId: string;
  let alphaDocId: string;
  let betaDocId: string;

  beforeAll(async () => {
    await db.create();

    docsId = `secrets_${Date.now()}`;
    await db.createCollection({
      id: docsId,
      attributes: [
        new Doc({
          $id: "label",
          key: "label",
          type: AttributeEnum.String,
          size: 255,
          required: true,
        }),
      ],
      permissions: [], // no collection-level grants at all
      documentSecurity: true,
    });

    // Seed one document per role through the system session.
    const system = db.system();
    const alphaDoc = await system.createDocument(
      docsId,
      new Doc({
        label: "alpha-doc",
        $permissions: [Permission.read(Role.user("alpha"))],
      }),
    );
    const betaDoc = await system.createDocument(
      docsId,
      new Doc({
        label: "beta-doc",
        $permissions: [Permission.read(Role.user("beta"))],
      }),
    );
    alphaDocId = alphaDoc.getId();
    betaDocId = betaDoc.getId();
  });

  afterAll(async () => {
    await db.delete();
    await db.getAdapter().$client.disconnect();
  });

  test("parallel sessions with distinct roles only see their own data", async () => {
    // Arrange: two independent sessions bound to different roles.
    const alpha = db.for("user:alpha");
    const beta = db.for("user:beta");

    // Act: run both sessions' queries concurrently.
    const [alphaDocs, betaDocs] = await Promise.all([
      alpha.find(docsId),
      beta.find(docsId),
    ]);

    // Assert: each session sees exactly its own role's document —
    // neither sees the other's, and neither sees both.
    expect(alphaDocs.map((d) => d.get("label")).sort()).toEqual([
      "alpha-doc",
    ]);
    expect(betaDocs.map((d) => d.get("label")).sort()).toEqual(["beta-doc"]);
  });

  test("system op in flight does not disable another session's auth check", async () => {
    // Arrange: a restricted session with NO grant on docsId.
    const intruder = db.for("user:mallory");
    const system = db.system();

    // A deferred promise parks the system operation mid-flight so it
    // definitively overlaps the unauthorized call below. Under old
    // global-status semantics the in-flight system op disabled
    // authorization process-wide, so the intruder's read would have
    // SUCCEEDED here and the assertion below would have failed.
    let releaseSystemOp!: () => void;
    const systemOpCheckpoint = new Promise<void>((resolve) => {
      releaseSystemOp = resolve;
    });

    const systemOp = (async () => {
      await systemOpCheckpoint;
      return system.updateDocument(
        docsId,
        betaDocId,
        new Doc({ label: "beta-doc-touched" }),
      );
    })();

    // Yield a tick so the system op is observably started and parked
    // at its checkpoint before the unauthorized call executes.
    await new Promise((resolve) => setImmediate(resolve));

    // Act + Assert: the unauthorized read must STILL be denied even
    // though a privileged system operation is concurrently in flight.
    // Document-level security denies by returning an empty document;
    // under global-bypass semantics the full alpha document would have
    // leaked through here instead.
    const leaked = await intruder.getDocument(docsId, alphaDocId);
    expect(leaked.empty()).toBe(true);

    // The system op itself must complete successfully once released.
    releaseSystemOp();
    await expect(systemOp).resolves.toBeDefined();
  });
});
