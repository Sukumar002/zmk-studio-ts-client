import { Request, Response, RequestResponse, Notification } from './studio';

import { get_encoder, get_decoder } from './framing';
import { RpcTransport } from './transport';

import { Mutex } from 'async-mutex';
import { ErrorConditions } from './meta';
export { Request, RequestResponse, Response, Notification };

export interface RpcConnection {
  label: string;
  request_response_readable: ReadableStream<RequestResponse>;
  request_writable: WritableStream<Request>;
  notification_readable: ReadableStream<Notification>;
  current_request: number;
  /**
   * Abort the underlying transport. A timed-out or desynchronized RPC cannot
   * safely be followed by another request on the same byte stream.
   */
  abort?: (reason?: any) => void;
}

export interface CreateRpcConnectionOpts {
  signal?: AbortSignal;
}

export function create_rpc_connection(transport: RpcTransport, opts?: CreateRpcConnectionOpts): RpcConnection {
  let { writable: request_writable, readable: byte_readable } =
    new TransformStream<Request, Uint8Array>({
      transform(chunk, controller) {
        let bytes = Request.encode(chunk).finish();
        controller.enqueue(bytes);
      },
    });

  let reqPipelineClosed = byte_readable
    .pipeThrough(new TransformStream(get_encoder()), { signal: opts?.signal })
    .pipeTo(transport.writable, { signal: opts?.signal });

  reqPipelineClosed.catch((r) => {console.log("Closed error", r); return r}).then(async (reason: any) => {
    await byte_readable.cancel();
    transport.abortController.abort(reason);
  });

  let response_readable = transport.readable
    .pipeThrough(new TransformStream(get_decoder()), { signal: opts?.signal })
    .pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          controller.enqueue(Response.decode(chunk));
        },
      }),
      { signal: opts?.signal }
    );

  let [a, b] = response_readable.tee();

  let request_response_readable = a.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.requestResponse) {
          controller.enqueue(chunk.requestResponse);
        }
      },
    }),
    { signal: opts?.signal }
  );

  let notification_readable = b.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.notification) {
          controller.enqueue(chunk.notification);
        }
      },
    }),
    { signal: opts?.signal }
  );

  return {
    label: transport.label,
    request_response_readable,
    request_writable,
    notification_readable,
    current_request: 0,
    abort: (reason?: any) => transport.abortController.abort(reason),
  };
}

const rpcMutexes = new WeakMap<RpcConnection, Mutex>();

function getRpcMutex(conn: RpcConnection): Mutex {
  let mutex = rpcMutexes.get(conn);
  if (!mutex) {
    mutex = new Mutex();
    rpcMutexes.set(conn, mutex);
  }
  return mutex;
}

export const DEFAULT_RPC_TIMEOUT_MS = 15000;

export class RpcTimeoutError extends Error {
  readonly timeoutMs: number;

  constructor(timeoutMs: number) {
    super(`RPC response timed out after ${timeoutMs} ms`);
    this.timeoutMs = timeoutMs;
    Object.setPrototypeOf(this, RpcTimeoutError.prototype);
  }
}

export class RpcProtocolError extends Error {
  constructor(message: string) {
    super(message);
    Object.setPrototypeOf(this, RpcProtocolError.prototype);
  }
}

export class NoResponseError extends Error {
  constructor() {
    super("No RPC response received");
    Object.setPrototypeOf(this, NoResponseError.prototype);
  }
}

export class MetaError extends Error {
  readonly condition: ErrorConditions;

  constructor(condition: ErrorConditions) {
    super("Meta error: " + condition);
    this.condition = condition;
    Object.setPrototypeOf(this, MetaError.prototype);
  }
}

export async function call_rpc(
  conn: RpcConnection,
  req: Omit<Request, 'requestId'>,
  timeoutMs: number = DEFAULT_RPC_TIMEOUT_MS
): Promise<RequestResponse> {
  return await getRpcMutex(conn).runExclusive(async () => {
    let request: Request = { ...req, requestId: conn.current_request++ };
    let writer: WritableStreamDefaultWriter<Request> | undefined;
    let reader: ReadableStreamDefaultReader<RequestResponse> | undefined;

    const operation = (async () => {
      writer = conn.request_writable.getWriter();
      try {
        await writer.write(request);
      } finally {
        writer.releaseLock();
        writer = undefined;
      }

      reader = conn.request_response_readable.getReader();
      try {
        let { done, value } = await reader.read();

        if (done || !value) {
          throw new RpcProtocolError("RPC response stream closed without a response");
        }

        if (value.requestId != request.requestId) {
          throw new RpcProtocolError(
            `RPC response ID mismatch: expected ${request.requestId}, received ${value.requestId}`
          );
        }

        if (value.meta?.noResponse) {
          throw new NoResponseError();
        } else if (value.meta?.simpleError) {
          throw new MetaError(value.meta.simpleError);
        }

        return value;
      } finally {
        reader.releaseLock();
        reader = undefined;
      }
    })();

    let timeoutHandle: ReturnType<typeof setTimeout> | undefined;
    const timeoutPromise = new Promise<never>((_, reject) => {
      timeoutHandle = setTimeout(() => {
        const error = new RpcTimeoutError(timeoutMs);
        conn.abort?.(error);
        reject(error);
      }, timeoutMs);
    });

    try {
      return await Promise.race([operation, timeoutPromise]);
    } catch (error) {
      if (error instanceof RpcTimeoutError) {
        // Abort the transport first, then cancel any outstanding stream
        // operation so its reader/writer lock is released before the mutex.
        conn.abort?.(error);
        if (reader) {
          await reader.cancel(error).catch(() => undefined);
        }
        if (writer) {
          await writer.abort(error).catch(() => undefined);
        }
        await operation.catch(() => undefined);
      } else if (!(error instanceof NoResponseError) && !(error instanceof MetaError)) {
        // A failed read, malformed protobuf frame, or mismatched request ID
        // leaves the response stream unsafe to reuse. Force a clean reconnect.
        conn.abort?.(error);
      }
      throw error;
    } finally {
      if (timeoutHandle !== undefined) {
        clearTimeout(timeoutHandle);
      }
    }
  });
}
