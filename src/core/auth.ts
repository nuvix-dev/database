import { PermissionEnum } from "./enums.js";

/**
 * Immutable authorization context carried explicitly through every
 * document-plane operation. Replaces the static/global state previously
 * held by the legacy static auth class (module-level role/status globals).
 */
export interface AuthContext {
  /**
   * Role strings this context acts as, e.g. `["user:123", "role:admin"]`.
   * Formats follow `Role.toString()`: `"roleName"`, `"roleName:identifier"`,
   * `"roleName/dimension"` or `"roleName:identifier/dimension"`.
   */
  readonly roles: readonly string[];
}

/**
 * Branded auth context for privileged internal operations.
 * Bypasses all authorization checks (replaces the legacy global skip).
 */
export interface SystemAuthContext extends AuthContext {
  readonly system: true;
}

/**
 * Shared privileged context. Frozen; authorizes every action regardless
 * of its (empty) roles list.
 */
export const SYSTEM_CONTEXT: SystemAuthContext = Object.freeze({
  roles: Object.freeze([]),
  system: true,
});

/**
 * Structural check so any `{ roles, system: true }` object qualifies as a
 * system context without requiring the branded type at runtime boundaries.
 */
const isSystemContext = (ctx: AuthContext): ctx is SystemAuthContext =>
  (ctx as Partial<SystemAuthContext>).system === true;

/**
 * Pure replacement for the legacy mutable validator instance pattern
 * (constructing a validator per action, then calling its `$valid` method).
 *
 * Semantics replicated from the legacy `$valid` implementation:
 * 1. A system context bypasses all checks (previously `!getStatus()`), and
 *    this bypass takes precedence over the empty-permissions rejection.
 * 2. Empty or missing permissions deny access.
 * 3. Otherwise, access is granted when any permission string is a member of
 *    the context's roles (`ctx.roles.includes(permission)`).
 *
 * The `$description` message side-channel is intentionally dropped: callers
 * needing a reason should treat `false` uniformly instead of reading mutable
 * module state.
 *
 * @param ctx - The immutable auth context performing the action.
 * @param permissions - Role strings granted the action (e.g. from
 * `Doc.getRead()`, which strips `read("...")` down to bare role strings).
 * @param action - The action being authorized (e.g. `PermissionEnum.Read`).
 * @returns `true` if the action is authorized, `false` otherwise.
 */
export function authorize(
  ctx: AuthContext,
  permissions: string[],
  action: PermissionEnum,
): boolean {
  if (isSystemContext(ctx)) {
    return true;
  }

  if (!permissions || permissions.length === 0) {
    return false;
  }

  const { roles } = ctx;
  for (const permission of permissions) {
    if (roles.includes(permission)) {
      return true;
    }
  }

  return false;
}
