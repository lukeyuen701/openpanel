package bullmq

import (
	"context"
	"crypto/sha1" //nolint:gosec // BullMQ addresses scripts by SHA-1.
	"embed"
	"encoding/hex"
)

//go:embed scripts/*.lua
var scriptFiles embed.FS

// script is one vendored BullMQ Lua command, run with EVALSHA and loaded with
// EVAL when the server has not seen it, as ioredis's defineCommand does.
type script struct {
	text string
	sha  string
}

func loadScript(name string) script {
	text, err := scriptFiles.ReadFile("scripts/" + name + ".lua")
	if err != nil {
		panic("bullmq: vendored script missing: " + name)
	}
	sum := sha1.Sum(text) //nolint:gosec // see import
	return script{text: string(text), sha: hex.EncodeToString(sum[:])}
}

var (
	moveToActiveScript          = loadScript("moveToActive")
	moveToFinishedScript        = loadScript("moveToFinished")
	moveToDelayedScript         = loadScript("moveToDelayed")
	retryJobScript              = loadScript("retryJob")
	extendLocksScript           = loadScript("extendLocks")
	moveStalledJobsToWaitScript = loadScript("moveStalledJobsToWait")
)

func (s script) run(ctx context.Context, redis Redis, keys, args []string) (Reply, error) {
	reply, err := redis.EvalSHA(ctx, s.sha, keys, args)
	if isNoScript(err) {
		return redis.Eval(ctx, s.text, keys, args)
	}
	return reply, err
}
