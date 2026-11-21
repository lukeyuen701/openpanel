package flush

// The flush lock scripts, verbatim from packages/core/src/buffers/base-buffer.ts,
// including the newline that opens and closes each template literal: the SHA
// covers it, so a TypeScript and a Go flush hash to the same script.
var (
	releaseLockScript = NewScript("\nif redis.call(\"get\", KEYS[1]) == ARGV[1] then\n  return redis.call(\"del\", KEYS[1])\nelse\n  return 0\nend\n")
	extendLockScript  = NewScript("\nif redis.call(\"get\", KEYS[1]) == ARGV[1] then\n  return redis.call(\"expire\", KEYS[1], ARGV[2])\nelse\n  return 0\nend\n")
)
