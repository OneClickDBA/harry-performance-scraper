---
title: go-ora Driver
sidebar_position: 5
---

# Using the go-ora database driver

The scraper supports compiling with the [go-ora database driver](https://github.com/sijms/go-ora). By default, it compiles with the `godror` database driver, which uses CGO and Oracle Instant Client. The go-ora driver is useful when you need a pure-Go binary without Oracle Instant Client.

### Configuring go-ora

Because go-ora does not use Oracle Instant Client, provide connection string options in each database `url` property:

```yaml
databases:
  go_ora_db:
    username: myuser
    password: ******
    url: my_tnsname?wallet=/path/to/wallet&ssl=1
```

### Build with go-ora

To build using `go-ora` instead of `godror`, set `TAGS=goora CGO_ENABLED=0`:

```bash
make go-build TAGS=goora CGO_ENABLED=0
```
