import { PermissionEnum } from "@core/enums.js";
import { Validator } from "@validators/interface.js";
import { AsyncLocalStorage } from "async_hooks";
import { Role } from "./role.js";

type AuthorizationStore = Map<string, Record<string, boolean> | boolean>;

export const storage = new AsyncLocalStorage<AuthorizationStore>();

/**
 * Manages application authorization roles and status, supporting both
 * global static state and per-request contextual state using AsyncLocalStorage.
 */
export class Authorization implements Validator {
  private static globalRoles: Record<string, boolean> = { any: true };
  private static globalStatus: boolean = true;
  private static useAsyncLocalStorage: boolean = false;

  protected action: PermissionEnum;
  protected message: string = "Authorization Error";

  /**
   * Creates an instance of Authorization for a specific action.
   * @param action - The action string to check permission for (e.g., "create", "read").
   */
  constructor(action: PermissionEnum) {
    this.action = action;
  }

  /**
   * Enables per-request storage using `AsyncLocalStorage`.
   * When enabled, `setRole`, `getRoles`, `setStatus`, `getStatus`, etc.,
   * operate on a context-specific store if available.
   */
  public static enableAsyncLocalStorage(): void {
    this.useAsyncLocalStorage = true;
  }

  /**
   * Disables per-request storage, reverting all authorization state management
   * to global static properties.
   */
  public static disableAsyncLocalStorage(): void {
    this.useAsyncLocalStorage = false;
  }

  /**
   * Gets the authorization error message if a check fails.
   * @returns The error message string.
   */
  public get $description(): string {
    return this.message;
  }

  /**
   * Checks if the provided permissions allow the configured action.
   * @param permissions - An array of permission strings associated with the current context (e.g., user roles, specific scopes).
   * @returns `true` if authorization passes, `false` otherwise.
   */
  public $valid(permissions: string[]): boolean {
    // Bypass all authorization checks if the global/contextual status is disabled.
    if (!Authorization.getStatus()) {
      return true;
    }

    if (!permissions || permissions.length === 0) {
      this.message = `No permissions provided for action '${this.action}'.`;
      return false;
    }

    let lastPermission = "-";
    const authorizedRoles = Authorization.getRoles();

    for (const permission of permissions) {
      lastPermission = permission;
      if (authorizedRoles.includes(permission)) {
        return true;
      }
    }

    this.message = `Missing "${this.action}" permission for role "${lastPermission}". Only "${JSON.stringify(permissions)}" scopes are allowed and "${JSON.stringify(authorizedRoles)}" was given.`;
    return false;
  }

  /**
   * Gets the current `AuthorizationStore` for the active AsyncLocalStorage context.
   * If AsyncLocalStorage is not enabled or no store is active, returns `undefined`.
   * @returns The `AuthorizationStore` map or `undefined`.
   */
  private static getStore(): AuthorizationStore | undefined {
    return this.useAsyncLocalStorage ? storage.getStore() : undefined;
  }

  /**
   * Sets a role as authorized.
   * The role is set either in the current `AsyncLocalStorage` context or globally.
   * @param role - The role string to set (e.g., "admin", "guest").
   */
  public static setRole(role: string | Role): void {
    role = typeof role === "string" ? role : role.toString();
    const store = this.getStore();
    if (store) {
      const roles = (store.get("roles") as Record<string, boolean>) || {};
      roles[role] = true;
      store.set("roles", roles);
    } else {
      this.globalRoles[role] = true;
    }
  }

  /**
   * Unsets (removes) a role from being authorized.
   * The role is unset either in the current `AsyncLocalStorage` context or globally.
   * @param role - The role string to unset.
   */
  public static unsetRole(role: string): void {
    const store = this.getStore();
    if (store) {
      const roles = (store.get("roles") as Record<string, boolean>) || {};
      delete roles[role];
      store.set("roles", roles);
    } else {
      delete this.globalRoles[role];
    }
  }

  /**
   * Gets all currently authorized roles.
   * Roles are retrieved from the current `AsyncLocalStorage` context if active, otherwise from global state.
   * @returns An array of authorized role strings.
   */
  public static getRoles(): string[] {
    const store = this.getStore();
    // If store exists, get roles from it, otherwise from globalRoles.
    return Object.keys(
      (store?.get("roles") as Record<string, boolean>) || this.globalRoles,
    );
  }

  /**
   * Cleans (removes all) currently authorized roles.
   * Roles are cleared from the current `AsyncLocalStorage` context if active, otherwise from global state.
   */
  public static cleanRoles(): void {
    const store = this.getStore();
    if (store) {
      store.set("roles", {});
    } else {
      this.globalRoles = {};
    }
  }

  /**
   * Checks if a specific role is currently authorized.
   * @param role - The role string to check.
   * @returns `true` if the role is authorized, `false` otherwise.
   */
  public static isRole(role: string): boolean {
    return this.getRoles().includes(role);
  }

  /**
   * Sets the default authorization status (applied when no AsyncLocalStorage context is active).
   * Also sets the current status to this new default.
   * @param status - The new default status (`true` for enabled, `false` for disabled).
   */
  public static setDefaultStatus(status: boolean): void {
    this.globalStatus = status;
    this.setStatus(status);
  }

  /**
   * Sets the current authorization status.
   * The status is set either in the current `AsyncLocalStorage` context or globally.
   * @param status - The new status (`true` for enabled, `false` for disabled).
   */
  public static setStatus(status: boolean): void {
    const store = this.getStore();
    if (store) {
      store.set("status", status);
    } else {
      this.globalStatus = status;
    }
  }

  /**
   * Gets the current authorization status.
   * Status is retrieved from the current `AsyncLocalStorage` context if active, otherwise from global state.
   * @returns The current authorization status.
   */
  public static getStatus(): boolean {
    const store = this.getStore();
    return (store?.get("status") as boolean) ?? this.globalStatus;
  }

  /**
   * Temporarily disables authorization, executes a callback, and then restores the original status.
   * This is useful for operations that should bypass all authorization checks.
   *
   * @template T - The return type of the callback function.
   * @param callback - An asynchronous function to execute with authorization disabled.
   * @returns A Promise that resolves with the result of the callback.
   */
  public static async skip<T>(callback: () => Promise<T>): Promise<T> {
    const initialStatus = this.getStatus();
    this.disable();
    try {
      return await callback();
    } finally {
      this.setStatus(initialStatus);
    }
  }

  /**
   * Enables authorization.
   * This affects the current `AsyncLocalStorage` context or global state.
   */
  public static enable(): void {
    this.setStatus(true);
  }

  /**
   * Disables authorization.
   * This affects the current `AsyncLocalStorage` context or global state.
   */
  public static disable(): void {
    this.setStatus(false);
  }

  /**
   * Resets the current authorization status to the `globalStatus`.
   * This affects the current `AsyncLocalStorage` context or global state.
   */
  public static reset(): void {
    this.setStatus(this.globalStatus);
  }
}
