# 时序存储

一个不依赖现成数据库的轻量时序存储。采集程序每秒写入一条带时间戳的浮点样本，画面通过推送立刻看到当前值，也可以按时间把一两小时的历史一次读出来。数据默认保留 14 天。

程序跑在局域网里：不从公网拉数据，也不把数据发到公网。画面默认只开在本机 [http://127.0.0.1:8741/](http://127.0.0.1:8741/)。传感器中枢用 TCP 把报文送到 `0.0.0.0:8742`，这个端口给局域网用，不要做公网端口映射。

如果 8741 或 8742 绑不上，程序会改用后面的空闲端口，并在窗口里写明实际地址。Windows 上即使 `netstat` 看不到占用进程，系统保留端口也会拒绝绑定，可以用 `netsh interface ipv4 show excludedportrange protocol=tcp` 查看。

## 运行

要求 Go 1.22 或更高版本。

```bash
go build -o tsdb ./cmd/tsdb
./tsdb serve -config tsdb.json
```

在 Windows 上：

```bat
go build -o tsdb.exe ./cmd/tsdb
tsdb.exe serve -config tsdb.json
```

也可以在其他系统上交叉编译：

```bash
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build -o tsdb.exe ./cmd/tsdb
```

`tsdb.json` 里的 `data_dir` 如果写成相对路径，是相对于配置文件所在目录，而不是进程的工作目录。Windows 服务的工作目录通常是 `System32`，这样数据不会写到那里。

打开根路径可以看到每个通道的当前数字。新样本到达后页面立即刷新，不轮询。超过 2 秒没有新样本时，数字标成停滞。

修改 `fields` 之后不能继续使用原来的数据目录。字段顺序就是磁盘上数值的顺序。示例里的识别码按井场报文习惯先放了 `0108`（井深）等几项，换成中枢实际使用的四位码再启动。

## 传感器 TCP

本程序是 TCP 服务端，在 `tcp_listen`（默认 `0.0.0.0:8742`）上等待连接。传感器中枢作为客户端连进来。程序不会主动去连中枢，也不从公网取数。报文是 UTF-8 文本。

支持两种行格式：

```text
&&
01081234.5
011080.25
!!
```

```text
&&01081234.5
&&011080.25
```

`&&` 开始一帧，`!!` 结束一帧。识别码是紧挨着的四位数字，后面是浮点。同一识别码再次出现时，上一帧也会收口。配置里没有的识别码会跳过。一帧里缺的测点记为 NaN。帧结束后用本机到达时间作为时间戳，并立刻推给画面。

若一帧没有 `!!`，默认再等 `frame_idle_ms`（200 毫秒）没有新行，就先写入，避免画面干等下一秒。

存储不再把一条样本限制在 1024 字节。原先字段上限 120 就是为了让 `8 + 字段数×8` 不超过 1024。现在一条样本最多 1024 个浮点。TCP 单行上限 4096 字节，连接上的未完整行缓冲最多 64KiB。按 40960 位（5120 字节）估算的一秒钟文本落在这个范围里。

## 写入和读取

写入一条 JSON。`values` 的顺序与配置文件一致，`null` 表示缺测。

```bash
curl -s -X POST http://127.0.0.1:8741/v1/write \
  -H 'Content-Type: application/json' \
  -d '{"ts":"2026-10-08T12:00:00Z","values":[21.5,80.1]}'
```

`ts` 可以是 RFC3339 字符串，也可以是整数。10 位及以下按秒，13 位按毫秒，16 位按微秒，更长按纳秒。

也可以直接 POST `application/octet-stream`，正文与磁盘记录相同：小端 `int64` 纳秒时间戳，随后每个字段一个小端 `float64`。一条样本最多 1024 个字段，不再按 1024 字节截断。

补写一段不倒退的历史：

```bash
curl -s -X POST http://127.0.0.1:8741/v1/write/batch \
  -H 'Content-Type: application/json' \
  -d '{"records":[{"ts":"2026-10-08T12:00:01Z","values":[21.6,80.0]}]}'
```

按时间读取，`from` 包含，`to` 不包含。`format=bin` 返回同样的小端记录流，响应头 `X-Count`、`X-Field-Count`、`X-Record-Size` 说明条数和宽度。

```bash
curl -s 'http://127.0.0.1:8741/v1/query?from=2026-10-08T12:00:00Z&to=2026-10-08T14:00:00Z'
curl -s 'http://127.0.0.1:8741/v1/latest?n=5'
```

实时推送是 `GET /v1/live`，内容类型 `text/event-stream`。连接后先收到 `snapshot`（当前最新一条，没有数据时 `ts` 为空），之后每条新样本是 `sample`。超过 2 秒没有新样本会收到 `heartbeat`。前端用 `EventSource` 即可。慢客户端不会堵住写入：队列里只留很短的一段，排满后丢掉中间点，只保留最新值。

时间戳必须不早于已经写入的最后一条。同一时刻可以有多条，按到达顺序保存。倒退的时间返回 409。

其他接口：`GET /v1/schema`、`GET /v1/health`。

## 数据怎么放

每个 UTC 小时一个文件，例如 `data/2026-10-08/15.seg`。

文件头 128 字节：魔数 `TSEG`、版本、字段数、记录宽度、schema 哈希、该小时起点（UTC 纳秒）。条数不写在文件头里，用 `(文件大小 - 128) / 记录宽度` 计算。重新打开时，尾部不足一条的残缺字节会被截掉，所以进程在写完一条之前崩溃，最多丢掉这一条。

一条记录先进入内存，实时订阅者立刻能看到，然后再追加到小时文件并刷盘。成功返回时，这条数据已经落盘。若在刷盘完成前断电，画面上刚出现的那一条可能不在磁盘上。

最近几个小时（默认 3 小时，由 `cache_hours` 决定）的原始字节留在内存里。查询当前这一两小时不读盘；更早的小时在等宽记录上二分时间戳，再顺序读出。默认保留 14 天，由 `retention_days` 修改。过期的小时文件会被删掉。

同一数据目录只能有一个进程。第二个进程会因为目录锁退出。

## Windows 服务

在管理员终端里：

```bat
tsdb.exe service install -config C:\tsdb\tsdb.json
tsdb.exe service start
tsdb.exe service stop
tsdb.exe service uninstall
```

服务名是 `Tsdb`，显示名是「时序存储」。安装时会把配置文件路径写成绝对路径。

## 测试

```bash
go test ./...
go test -bench=BenchmarkQueryTwoHours -benchtime=3x ./internal/store
```
