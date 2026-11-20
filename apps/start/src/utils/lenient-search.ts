import type { z } from 'zod';

// A throwing validateSearch answers 500 on SSR, so a malformed public link
// validates to blanks and renders its own "link not valid" state. The keys are
// blanked rather than omitted because the parent's raw params merge under them.
export function lenientSearch<Shape extends z.ZodRawShape>(
  schema: z.ZodObject<Shape>
) {
  type Search = Partial<z.output<z.ZodObject<Shape>>>;
  const blank = Object.fromEntries(
    Object.keys(schema.shape).map((key) => [key, undefined])
  ) as Search;
  return (search: Record<string, unknown>): Search => {
    const parsed = schema.safeParse(search);
    return parsed.success ? parsed.data : blank;
  };
}
