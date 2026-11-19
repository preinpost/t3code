import { type AuthEnvironmentScope, EnvironmentHttpApi } from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as FetchHttpClient from "effect/http/FetchHttpClient";
import { isHttpClientError } from "effect/http/HttpClientError";
import * as HttpApiClient from "effect/http-api/HttpApiClient";

import type * as EnvironmentAuth from "../auth/EnvironmentAuth.ts";
import type * as ServerConfig from "../config.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerOwnership from "../serverOwnership.ts";
import {
  clearPersistedServerRuntimeState,
  readLiveServerRuntimeState,
} from "../serverRuntimeState.ts";

export class RunningServerRequestError extends Schema.TaggedError<RunningServerRequestError>()(
  "RunningServerRequestError",
  { origin: Schema.String, cause: Schema.Defect() },
) {
  override get message(): string {
    const detail = this.cause instanceof Error ? ` (${this.cause.message})` : "";
    return `The T3 Code server at ${this.origin} is running but did not complete the request${detail}. Nothing was changed. Retry, or restart the server and retry.`;
  }
}

export type ServerClient = HttpApiClient.ForApi<typeof EnvironmentHttpApi>;
export interface ServerClientHeaders {
  readonly authorization: string;
}

const RUNNING_SERVER_TIMEOUT = Duration.seconds(30);
const CLI_SESSION_TTL = Duration.minutes(2);

const isUnanswered = (cause: unknown) =>
  Cause.isTimeoutError(cause) || (isHttpClientError(cause) && cause.response === undefined);

/** Calls the server as a short-lived client of its own, and revokes that session afterwards. */
export const withServerClient = <A, E, R>(input: {
  readonly environmentAuth: EnvironmentAuth.EnvironmentAuth["Service"];
  readonly origin: string;
  readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
  readonly label: string;
  readonly use: (client: ServerClient, headers: ServerClientHeaders) => Effect.Effect<A, E, R>;
}) =>
  Effect.acquireUseRelease(
    input.environmentAuth.issueSession({
      scopes: input.scopes,
      subject: "cli",
      label: input.label,
      // The release revokes it; this bounds it if the release never runs.
      ttl: CLI_SESSION_TTL,
    }),
    (issued) =>
      HttpApiClient.make(EnvironmentHttpApi, { baseUrl: input.origin }).pipe(
        Effect.flatMap((client) => input.use(client, { authorization: `Bearer ${issued.token}` })),
        Effect.provide(FetchHttpClient.layer),
      ),
    (issued) =>
      input.environmentAuth.revokeSession(issued.sessionId).pipe(Effect.ignore({ log: true })),
  );

/**
 * A command that changes state goes through the server when one runs, so the server applies
 * the change and its clients see it. With no server, the command writes locally while it holds
 * the ownership lock, so a server cannot start halfway through the write. `offline` runs before
 * the lock is taken and returns that write, so a failed setup leaves server state alone.
 */
export const changeState = Effect.fn("cli.changeState")(function* <
  A,
  LiveError,
  LiveR,
  OfflineError,
  OfflineR,
  SetupError,
  SetupR,
>(input: {
  readonly environmentAuth: EnvironmentAuth.EnvironmentAuth["Service"];
  readonly config: ServerConfig.ServerConfig["Service"];
  readonly scopes: ReadonlyArray<AuthEnvironmentScope>;
  readonly label: string;
  readonly live: (
    client: ServerClient,
    headers: ServerClientHeaders,
  ) => Effect.Effect<A, LiveError, LiveR>;
  readonly offline: Effect.Effect<Effect.Effect<A, OfflineError, OfflineR>, SetupError, SetupR>;
}) {
  const statePath = input.config.serverRuntimeStatePath;
  const runningServer = yield* readLiveServerRuntimeState(statePath);
  let liveFailure: Option.Option<{ readonly origin: string; readonly cause: unknown }> =
    Option.none();
  if (Option.isSome(runningServer)) {
    const origin = runningServer.value.origin;
    const viaServer = yield* withServerClient({
      environmentAuth: input.environmentAuth,
      origin,
      scopes: input.scopes,
      label: input.label,
      use: (client, headers) =>
        input.live(client, headers).pipe(Effect.timeout(RUNNING_SERVER_TIMEOUT)),
    }).pipe(Effect.result);
    if (viaServer._tag === "Success") return viaServer.success;
    // Any answer, even an error, means the server handled the request.
    if (!isUnanswered(viaServer.failure)) return yield* Effect.fail(viaServer.failure);
    liveFailure = Option.some({ origin, cause: viaServer.failure });
  }

  const write = yield* input.offline;
  return yield* Effect.gen(function* () {
    // A server that died since it wrote its record cannot answer; only the lock tells them apart.
    yield* ServerOwnership.acquireServerOwnership(statePath).pipe(
      Effect.provide(ProcessRunner.layer),
      Effect.catchTags({
        ServerAlreadyRunningError: (alreadyRunning) =>
          Effect.fail(
            Option.isSome(liveFailure)
              ? new RunningServerRequestError(liveFailure.value)
              : alreadyRunning,
          ),
      }),
    );
    yield* clearPersistedServerRuntimeState(statePath);
    return yield* write;
  }).pipe(Effect.scoped);
});
