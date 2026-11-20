import { notFound } from '@tanstack/react-router';
import { TRPCClientError } from '@trpc/client';

const HTTP_NOT_FOUND = 404;

// A loader error answers 500 on SSR; a share that is gone is a 404.
export function rethrowMissingShare(error: unknown): never {
  if (
    error instanceof TRPCClientError &&
    error.data?.httpStatus === HTTP_NOT_FOUND
  ) {
    throw notFound();
  }
  throw error;
}
