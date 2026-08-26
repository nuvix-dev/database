import { describe, expect, it } from "bun:test";
import {
  SYSTEM_CONTEXT,
  authorize,
  type AuthContext,
} from "../src/core/auth.js";
import { PermissionEnum } from "../src/core/enums.js";
import { Role, UserDimension } from "../src/utils/role.js";

const ctx = (roles: string[]): AuthContext => Object.freeze({ roles });

describe("authorize", () => {
  it("allows when a permission matches a context role", () => {
    const user = ctx(["user:123", "role:admin"]);

    expect(authorize(user, ["user:123"], PermissionEnum.Read)).toBe(true);
  });

  it("allows when any permission in the list matches a context role", () => {
    const user = ctx(["user:123"]);

    expect(
      authorize(user, ["team:abc", "user:123"], PermissionEnum.Update),
    ).toBe(true);
  });

  it("denies when no permission matches a context role", () => {
    const user = ctx(["user:456"]);

    expect(authorize(user, ["user:123"], PermissionEnum.Read)).toBe(false);
  });

  it("denies when the context has no roles", () => {
    const nobody = ctx([]);

    expect(authorize(nobody, ["any"], PermissionEnum.Read)).toBe(false);
  });

  it("denies on empty permissions", () => {
    const user = ctx(["user:123"]);

    expect(authorize(user, [], PermissionEnum.Read)).toBe(false);
  });

  it("denies on missing permissions", () => {
    const user = ctx(["user:123"]);

    expect(
      authorize(user, undefined as unknown as string[], PermissionEnum.Read),
    ).toBe(false);
  });

  it("SYSTEM_CONTEXT bypasses checks even with empty permissions", () => {
    // Bypass takes precedence over the empty-permissions rejection,
    // mirroring the old `!Authorization.getStatus()` early return.
    expect(authorize(SYSTEM_CONTEXT, [], PermissionEnum.Delete)).toBe(true);
    expect(
      authorize(SYSTEM_CONTEXT, ["user:123"], PermissionEnum.Create),
    ).toBe(true);
  });

  it("matches role strings produced by Role.toString()", () => {
    const user = ctx([
      Role.user("123").toString(), // "user:123"
      Role.users(UserDimension.VERIFIED).toString(), // "users/verified"
      Role.team("abc", "dev").toString(), // "team:abc/dev"
      Role.any().toString(), // "any"
    ]);

    expect(authorize(user, ["user:123"], PermissionEnum.Read)).toBe(true);
    expect(authorize(user, ["users/verified"], PermissionEnum.Read)).toBe(true);
    expect(authorize(user, ["team:abc/dev"], PermissionEnum.Write)).toBe(true);
    expect(authorize(user, ["any"], PermissionEnum.Read)).toBe(true);
    expect(authorize(user, ["guests"], PermissionEnum.Read)).toBe(false);
  });

  it("does not mutate its inputs (pure function)", () => {
    const roles = ["user:123"];
    const permissions = ["user:123"];

    authorize(ctx(roles), permissions, PermissionEnum.Read);

    expect(roles).toEqual(["user:123"]);
    expect(permissions).toEqual(["user:123"]);
  });

  it("exposes SYSTEM_CONTEXT as frozen and empty", () => {
    expect(Object.isFrozen(SYSTEM_CONTEXT)).toBe(true);
    expect(Object.isFrozen(SYSTEM_CONTEXT.roles)).toBe(true);
    expect(SYSTEM_CONTEXT.roles).toEqual([]);
    expect(SYSTEM_CONTEXT.system).toBe(true);
  });
});
