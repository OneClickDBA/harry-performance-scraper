---
title: Configuring the Go Runtime
sidebar_position: 1
---

# Scraper Go Runtime

If you are running in an environment with limited memory, or you are running a large number of scraper processes, you may want to control the scraper's memory usage.

Under normal circumstances, the scraper process will retain OS memory that was used by the Go garbage collector but is no longer needed, in case it may be needed again in the future, unless the host OS is under memory pressure. The result of this behavior is that the resident set size will not decrease until the host OS memory is almost all used. Under most circumstances this will not cause issues, but the following options are available when memory must be constrained:

- You may set the `FREE_INTERVAL` environment variable to a Go [duration string](https://pkg.go.dev/maze.io/x/duration), for example `60s`, and run with `GODEBUG` containing `madvdontneed=1`, for example `GODEBUG=gctrace=1,madvdontneed=1`. The scraper will call [FreeOSMemory()](https://pkg.go.dev/runtime/debug#FreeOSMemory) at the specified interval. This asks the Go runtime to release memory that is no longer needed.
- You may set the `RESTART_INTERVAL` environment variable to a Go duration string, for example `10m`. The scraper will restart its own process at the specified interval by calling the OS `exec` syscall. The PID does not change, but the process image is replaced, which returns the resident set to its initial size.
- In addition to these, you may also set `GOMAXPROCS`, `GOGC`, and `GOMEMLIMIT` (see [documentation](https://pkg.go.dev/runtime#hdr-Environment_Variables)) to further limit the amount of resources that the Go runtime may use.
