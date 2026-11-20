import type { PrismaConfig } from 'prisma/config';

// No runtime imports: the op-api image runs a standalone Prisma CLI from
// /opt/prisma, and this file must load without packages/db's devDependencies.
// Migrations take the direct URL, since a pooler cannot run them.
export default {
  schema: 'prisma/schema.prisma',
  migrations: { path: 'prisma/migrations' },
  datasource: {
    url: process.env.DATABASE_URL_DIRECT || process.env.DATABASE_URL,
  },
} satisfies PrismaConfig;
