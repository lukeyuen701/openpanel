/**
 * The producer's fatal-error classification. Only these failures invalidate
 * the idempotent producer's PID/sequence state; everything else must be
 * retried against the SAME producer rather than silently rebuilding it.
 *
 * The errors are the client's own classes, shaped the way a failed produce
 * reports them: a broker code on a leaf, wrapped in the response and retry
 * aggregates.
 */

import { describe, expect, mock, test } from 'bun:test';
import {
  MultipleErrors,
  NetworkError,
  ProtocolError,
  TimeoutError,
} from '@platformatic/kafka';
import { isFatalProducerError, sendWithFatalRecovery } from './producer-errors';

const RETRIES = 3;

const asProduceFailure = (leaf: Error) =>
  new MultipleErrors('Producing messages failed.', [leaf]);

describe('isFatalProducerError', () => {
  test.each([45, 46, 47, 65])('protocol error %i is fatal', (code) => {
    expect(isFatalProducerError(new ProtocolError(code))).toBe(true);
  });

  test('a fatal code nested in the response aggregate is still fatal', () => {
    expect(isFatalProducerError(asProduceFailure(new ProtocolError(45)))).toBe(
      true
    );
  });

  test('retries-exceeded is fatal for this producer', () => {
    expect(
      isFatalProducerError(
        new MultipleErrors(`produce failed ${RETRIES} times.`, [
          new NetworkError('connection reset'),
        ])
      )
    ).toBe(true);
  });

  test.each([1, 7, 44])('protocol error %i is not fatal', (code) => {
    expect(
      isFatalProducerError(asProduceFailure(new ProtocolError(code)))
    ).toBe(false);
  });

  test.each([
    ['a plain error', new Error('connection reset')],
    ['a client timeout', new TimeoutError('request timed out')],
    ['a string', 'nope'],
    ['null', null],
  ])('%s is not fatal', (_label, err) => {
    expect(isFatalProducerError(err)).toBe(false);
  });
});

describe('sendWithFatalRecovery', () => {
  test('leaves a successful produce untouched', async () => {
    const onFatal = mock(() => undefined);
    await sendWithFatalRecovery(() => Promise.resolve(), onFatal);
    expect(onFatal).not.toHaveBeenCalled();
  });

  test('rethrows a non-fatal failure without recovering', async () => {
    const onFatal = mock(() => undefined);
    const err = new Error('transient');
    await expect(
      sendWithFatalRecovery(() => Promise.reject(err), onFatal)
    ).rejects.toBe(err);
    expect(onFatal).not.toHaveBeenCalled();
  });

  test('recovers and still rethrows a fatal failure', async () => {
    const onFatal = mock((_err: unknown) => undefined);
    const err = new ProtocolError(65);
    await expect(
      sendWithFatalRecovery(() => Promise.reject(err), onFatal)
    ).rejects.toBe(err);
    expect(onFatal).toHaveBeenCalledTimes(1);
  });
});
