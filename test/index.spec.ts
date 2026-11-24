jest.mock(
  '../src/studio',
  () => ({
    Request: { encode: jest.fn() },
    Response: { decode: jest.fn() },
  }),
  { virtual: true }
);

import {
  call_rpc,
  create_rpc_connection,
  RpcConnection,
  RpcDecodeError,
  RpcTimeoutError,
} from '../src/index';
import { Request, Response } from '../src/studio';

function makeConnection(
  readable: ReadableStream<any>,
  writable: WritableStream<any>,
  abort = jest.fn()
): RpcConnection {
  return {
    label: 'test',
    request_response_readable: readable as any,
    request_writable: writable as any,
    notification_readable: new ReadableStream<any>({
      start(controller) {
        controller.close();
      },
    }) as any,
    current_request: 0,
    abort,
  };
}

describe('call_rpc failure handling', () => {
  it('releases both stream locks after a successful request', async () => {
    const response = { requestId: 0 };
    const readable = new ReadableStream<any>({
      start(controller) {
        controller.enqueue(response);
        controller.close();
      },
    });
    const writable = new WritableStream<any>();
    const conn = makeConnection(readable, writable);

    await expect(call_rpc(conn, {} as any)).resolves.toBe(response);

    expect(readable.locked).toBe(false);
    expect(writable.locked).toBe(false);
    expect(conn.abort).not.toHaveBeenCalled();
  });

  it('releases the writer lock and aborts when writing fails', async () => {
    const failure = new Error('serial write failed');
    const readable = new ReadableStream<any>();
    const writable = new WritableStream<any>({
      write() {
        throw failure;
      },
    });
    const conn = makeConnection(readable, writable);

    await expect(call_rpc(conn, {} as any)).rejects.toBe(failure);

    expect(writable.locked).toBe(false);
    expect(readable.locked).toBe(false);
    expect(conn.abort).toHaveBeenCalledWith(failure);
  });

  it('releases the reader lock and aborts when the response stream fails', async () => {
    const failure = new Error('malformed protobuf response');
    const readable = new ReadableStream<any>({
      start(controller) {
        controller.error(failure);
      },
    });
    const writable = new WritableStream<any>();
    const conn = makeConnection(readable, writable);

    await expect(call_rpc(conn, {} as any)).rejects.toBe(failure);

    expect(readable.locked).toBe(false);
    expect(writable.locked).toBe(false);
    expect(conn.abort).toHaveBeenCalledWith(failure);
  });

  it('isolates a malformed frame and allows the next RPC to proceed', async () => {
    const decodeFailure = new Error('truncated protobuf response');
    const readable = new ReadableStream<any>({
      start(controller) {
        controller.enqueue({ __rpcDecodeError: decodeFailure });
        controller.enqueue({ requestId: 1 });
        controller.close();
      },
    });
    const writable = new WritableStream<any>();
    const conn = makeConnection(readable, writable);

    await expect(call_rpc(conn, {} as any)).rejects.toBeInstanceOf(
      RpcDecodeError
    );
    expect(conn.abort).not.toHaveBeenCalled();
    expect(readable.locked).toBe(false);
    expect(writable.locked).toBe(false);

    await expect(call_rpc(conn, {} as any)).resolves.toEqual({ requestId: 1 });
  });

  it('times out a missing response, aborts the connection, and releases locks', async () => {
    let responseController: ReadableStreamDefaultController<any> | undefined;
    const readable = new ReadableStream<any>({
      start(controller) {
        responseController = controller;
      },
    });
    const writable = new WritableStream<any>();
    const abort = jest.fn((reason?: any) => {
      responseController?.error(reason);
    });
    const conn = makeConnection(readable, writable, abort);

    await expect(call_rpc(conn, {} as any, 10)).rejects.toBeInstanceOf(
      RpcTimeoutError
    );

    expect(abort).toHaveBeenCalled();
    expect(readable.locked).toBe(false);
    expect(writable.locked).toBe(false);
  });

  it('continues decoding after a malformed protobuf frame', async () => {
    (Request.encode as jest.Mock).mockReturnValue({
      finish: () => Uint8Array.from([1]),
    });
    (Response.decode as jest.Mock)
      .mockImplementationOnce(() => {
        throw new Error('truncated protobuf response');
      })
      .mockReturnValueOnce({ requestResponse: { requestId: 1 } });

    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        controller.enqueue(
          Uint8Array.from([0xab, 1, 0xad, 0xab, 2, 0xad])
        );
        controller.close();
      },
    });
    const writable = new WritableStream<Uint8Array>();
    const transport = {
      label: 'test',
      abortController: new AbortController(),
      readable,
      writable,
    };
    const conn = create_rpc_connection(transport as any);

    await expect(call_rpc(conn, {} as any)).rejects.toBeInstanceOf(
      RpcDecodeError
    );
    await expect(call_rpc(conn, {} as any)).resolves.toEqual({ requestId: 1 });

    expect(transport.abortController.signal.aborted).toBe(false);
  });

  it('does not serialize independent connections behind one global mutex', async () => {
    let releaseFirstResponse: (() => void) | undefined;
    const firstReadable = new ReadableStream<any>({
      start(controller) {
        releaseFirstResponse = () => {
          controller.enqueue({ requestId: 0 });
          controller.close();
        };
      },
    });
    const firstWritable = new WritableStream<any>();
    const firstConn = makeConnection(firstReadable, firstWritable);

    const secondReadable = new ReadableStream<any>({
      start(controller) {
        controller.enqueue({ requestId: 0 });
        controller.close();
      },
    });
    const secondWritable = new WritableStream<any>();
    const secondConn = makeConnection(secondReadable, secondWritable);

    const firstRequest = call_rpc(firstConn, {} as any, 1000);
    const secondRequest = call_rpc(secondConn, {} as any, 1000);

    await expect(
      Promise.race([
        secondRequest,
        new Promise((_, reject) =>
          setTimeout(() => reject(new Error('second connection was blocked')), 100)
        ),
      ])
    ).resolves.toEqual({ requestId: 0 });

    releaseFirstResponse?.();
    await expect(firstRequest).resolves.toEqual({ requestId: 0 });
  });
});
