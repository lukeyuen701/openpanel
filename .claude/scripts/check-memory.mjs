#!/usr/bin/env bun
// Checks the Claude memory layers of this checkout: the auto-memory index and
// the repo paths the instruction files cite. Exit 1 when anything fails.
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, relative } from 'node:path';

const MEMORY_INDEX_MAX_BYTES = 8192;
const MEMORY_INDEX_MAX_LINES = 150;
const MEMORY_INDEX_MAX_LINE_CHARS = 220;
const MEMORY_TYPES = new Set(['user', 'feedback', 'project', 'reference']);
const INSTRUCTION_FILE = /(^|\/)(AGENTS|CLAUDE)(\.local)?\.md$/;
const PATH_TOKEN = /`([\w.@-]+(?:\/[\w.@-]+)+\/?)`/g;
// Prose cites paths from the repo root, a workspace package or core's src/.
const WORKSPACE_PARENTS = ['apps', 'packages', 'packages/sdks', 'tooling'];

const repoRoot = Bun.spawnSync(['git', 'rev-parse', '--show-toplevel'])
  .stdout.toString()
  .trim();
const memoryDir =
  process.env.CLAUDE_MEMORY_DIR ??
  join(
    homedir(),
    '.claude/projects',
    repoRoot.replace(/[^a-zA-Z0-9]/g, '-'),
    'memory'
  );

const failures = [];
const warnings = [];

function frontmatter(text) {
  const block = text.match(/^---\n([\s\S]*?)\n---/)?.[1] ?? '';
  const fields = {};
  for (const line of block.split('\n')) {
    const match = line.match(/^\s*(\w+):\s*(.*)$/);
    if (match?.[1] && match[2] !== undefined) {
      fields[match[1]] = match[2].replace(/^"|"$/g, '');
    }
  }
  return fields;
}

function linkedFiles(text) {
  return [...text.matchAll(/\]\(([^)\s]+\.md)\)/g)].map(
    (match) => match[1] ?? ''
  );
}

function checkMemory() {
  const indexPath = join(memoryDir, 'MEMORY.md');
  if (!existsSync(indexPath)) {
    warnings.push(`memory: no index at ${indexPath}`);
    return { files: 0, indexBytes: 0 };
  }
  const index = readFileSync(indexPath, 'utf8');
  const lines = index.split('\n').filter(Boolean);
  if (index.length > MEMORY_INDEX_MAX_BYTES) {
    failures.push(
      `memory: MEMORY.md is ${index.length} B (max ${MEMORY_INDEX_MAX_BYTES})`
    );
  }
  if (lines.length > MEMORY_INDEX_MAX_LINES) {
    failures.push(
      `memory: MEMORY.md has ${lines.length} lines (max ${MEMORY_INDEX_MAX_LINES})`
    );
  }
  for (const line of lines) {
    if (line.length > MEMORY_INDEX_MAX_LINE_CHARS) {
      failures.push(
        `memory: index line over ${MEMORY_INDEX_MAX_LINE_CHARS} chars: ${line.slice(0, 60)}…`
      );
    }
  }

  const topics = readdirSync(memoryDir).filter(
    (name) => name.endsWith('.md') && name !== 'MEMORY.md'
  );
  const names = new Map();
  for (const topic of topics) {
    const fields = frontmatter(readFileSync(join(memoryDir, topic), 'utf8'));
    if (!(fields.name && fields.description)) {
      failures.push(`memory: ${topic} lacks frontmatter name/description`);
    }
    if (!MEMORY_TYPES.has(fields.type ?? '')) {
      failures.push(`memory: ${topic} has type "${fields.type ?? ''}"`);
    }
    if (fields.name) {
      names.set(fields.name, topic);
    }
  }

  const reached = new Set();
  const queue = linkedFiles(index);
  for (const target of queue) {
    if (!existsSync(join(memoryDir, target))) {
      failures.push(`memory: link to missing ${target}`);
      continue;
    }
    if (reached.has(target)) {
      continue;
    }
    reached.add(target);
    const body = readFileSync(join(memoryDir, target), 'utf8');
    queue.push(...linkedFiles(body));
    for (const [, name] of body.matchAll(/\[\[([^\]]+)\]\]/g)) {
      const linked = names.get(name ?? '');
      if (linked) {
        queue.push(linked);
      } else {
        warnings.push(
          `memory: ${target} links [[${name}]], which no memory has yet`
        );
      }
    }
  }
  for (const topic of topics) {
    if (!reached.has(topic)) {
      failures.push(`memory: ${topic} is not reachable from MEMORY.md`);
    }
  }
  return { files: topics.length, indexBytes: index.length };
}

function instructionFiles() {
  const tracked = Bun.spawnSync(
    ['git', 'ls-files', '--cached', '--others', '--exclude-standard'],
    { cwd: repoRoot }
  )
    .stdout.toString()
    .split('\n')
    .filter(
      (path) => INSTRUCTION_FILE.test(path) || path.startsWith('.claude/')
    )
    .filter((path) => path.endsWith('.md'));
  const local = existsSync(join(repoRoot, 'CLAUDE.local.md'))
    ? ['CLAUDE.local.md']
    : [];
  return [...tracked, ...local].map((path) => join(repoRoot, path));
}

function citationRoots() {
  const packages = WORKSPACE_PARENTS.flatMap((parent) => {
    const dir = join(repoRoot, parent);
    return existsSync(dir)
      ? readdirSync(dir)
          .map((name) => join(dir, name))
          .filter((path) => statSync(path).isDirectory())
      : [];
  });
  return [repoRoot, join(repoRoot, 'packages/core/src'), ...packages];
}

const roots = citationRoots();

function isGitIgnored(token) {
  return (
    Bun.spawnSync(['git', 'check-ignore', '-q', '--no-index', token], {
      cwd: repoRoot,
    }).exitCode === 0
  );
}

function resolves(token, file) {
  const bare = token.replace(/\/$/, '');
  return (
    [dirname(file), ...roots].some((root) => existsSync(join(root, bare))) ||
    isGitIgnored(bare)
  );
}

function checkCitedPaths(files) {
  for (const file of files) {
    const text = readFileSync(file, 'utf8');
    for (const [, token] of text.matchAll(PATH_TOKEN)) {
      const looksLikePath =
        token && !token.startsWith('@') && /\.[a-z]{1,5}\/?$|\/$/.test(token);
      if (looksLikePath && !resolves(token, file)) {
        failures.push(
          `paths: ${relative(repoRoot, file)} cites \`${token}\`, which does not exist`
        );
      }
    }
  }
}

function checkNestedAgents(files) {
  for (const file of files.filter((path) => path.endsWith('/AGENTS.md'))) {
    const claude = join(dirname(file), 'CLAUDE.md');
    const imports =
      existsSync(claude) && readFileSync(claude, 'utf8').includes('@AGENTS.md');
    if (!imports) {
      failures.push(
        `routing: ${relative(repoRoot, file)} has no sibling CLAUDE.md importing @AGENTS.md, so Claude Code never loads it`
      );
    }
  }
}

const files = instructionFiles();
const memory = checkMemory();
checkCitedPaths(files);
checkNestedAgents(files);

for (const line of warnings) {
  console.log(`warn  ${line}`);
}
for (const line of failures) {
  console.log(`FAIL  ${line}`);
}
const sizes = files
  .filter((file) => INSTRUCTION_FILE.test(file))
  .map(
    (file) =>
      `${relative(repoRoot, file)} ${readFileSync(file, 'utf8').split('\n').length}L`
  )
  .join(', ');
console.log(`instruction files: ${sizes}`);
console.log(
  `.claude files: ${files.filter((file) => file.includes('/.claude/')).length}; memory: ${memory.files} topics, index ${memory.indexBytes} B (${memoryDir})`
);
console.log(failures.length ? `${failures.length} failure(s)` : 'ok');
process.exit(failures.length ? 1 : 0);
