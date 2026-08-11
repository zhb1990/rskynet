# rskynet

用 Rust 复刻 [skynet](https://github.com/cloudwu/skynet) 的 Actor 内核。**不需要 Lua**——skynet 用协程解决的问题，Rust 的 `async`/`Future` 原样能解，还多了编译期类型检查。

```rust
// 这段代码读起来是同步的，实际上挂起的只是当前这个任务，服务照常处理其它消息
let reply = ctx.request(".pong", Payload::of(Ask::Ball(round))).await?;
```

## skynet 的灵魂是哪三件事

| skynet 的做法 | rskynet 的做法 |
| --- | --- |
| **服务即 Actor**：每个服务有独立地址、独立邮箱，彼此只靠消息往来，内部天然单线程 | 一样。服务实现 `Service` trait，状态放进 `SvcCell`，全程不需要锁 |
| **两级消息队列**：每服务一个邮箱，全局一个「有活干的服务」队列，N 个 worker 线程从中取活 | 一样。`Mailbox` + `GlobalQueue`，`in_global` 标志保证同一服务绝不被两个 worker 同时执行 |
| **session 配对**：`call` 分配一个 session 并挂起协程，回包带同一 session 回来时唤醒它 | 一样，只是把协程换成 `Future`：`session -> Waker`，`call` 就是一句 `.await` |

第三点是关键：**Lua 协程只是实现手段，不是设计本身**。把它换成 `Future` 之后，`call`、`sleep`、`fork` 的语义一字不差，而且不再需要嵌一个脚本虚拟机。

## 调度模型

rskynet 在 skynet 的基础上做了一处推广：**服务的「可运行」条件从「邮箱非空」变成「邮箱非空 或 有就绪任务」**。于是定时器回包、RPC 回包、外部线程唤醒三者走的是同一条路径，`in_global` 这一个标志就同时管住了消息与 Future 唤醒。

```mermaid
flowchart LR
    subgraph svc [ServiceContext 一个服务]
        MB[Mailbox 邮箱]
        RQ[就绪任务队列]
        TS[任务集 BoxFuture]
        SS["Session 表 session 到 Waker"]
    end
    Sender[其他服务 send/call] -->|投递消息| MB
    TimerThread[定时器线程 时间轮] -->|RESPONSE 消息| MB
    Waker[Future 被唤醒] -->|任务 id| RQ
    MB -->|in_global 为假时入队| GQ[全局队列]
    RQ -->|in_global 为假时入队| GQ
    GQ --> W1[worker 线程 1]
    GQ --> WN[worker 线程 N]
    W1 -->|取一件活干 就绪任务优先| svc
```

worker 的一轮调度（对照 C 版 `skynet_context_message_dispatch`）：

1. 从全局队列取一个服务，取不到就睡在条件变量上
2. 反复 `take_work()`：优先 poll 就绪任务，其次取一条消息
   - 消息是 `RESPONSE`/`ERROR` 且带 session → 直接唤醒等待中的任务
   - 其它消息 → 开一个新任务跑 `dispatch`，等价于 skynet 新建一个协程
3. 处理够 `队列长度 >> weight` 条消息就让渡，把自己排回队尾
4. 邮箱和就绪队列都空了 → 清掉 `in_global`，脱离全局队列，等下一次投递

「就绪任务优先于新消息」不是随便定的：它对应 skynet 里被 resume 的协程会一路跑到下一次 yield，之后才轮到下一条消息。

## 快速上手

```rust
use std::sync::Arc;
use rskynet::{BoxFuture, Config, Ctx, Message, Payload, Registry, Result, Service};

struct Echo;

impl Service for Echo {
    fn init(self: Arc<Self>, ctx: Ctx, _args: String) -> BoxFuture<'static, Result<()>> {
        Box::pin(async move {
            ctx.register_name("echo");
            Ok(())
        })
    }

    fn dispatch(self: Arc<Self>, ctx: Ctx, mut msg: Message) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            let payload = msg.take_payload();
            let _ = ctx.reply(&msg, payload);
        })
    }
}

fn main() -> Result<()> {
    let registry = Registry::new().with_builtins().with("echo", || Echo);
    let config = Config::default().with_bootstrap("bootstrap echo");
    rskynet::start(config, registry)
}
```

跑内置示例：

```bash
cargo run --example ping_pong
```

它会演示三件事：`call` 的同步写法、`spawn` 的服务内并发、`sleep` 走定时器。输出大致是：

```
[     0.00] [:00000004] 1000 个来回耗时 6.66ms，平均单程 3.3µs
[     0.31] [:00000004] 三个请求（30/10/20 厘秒）的完成顺序是 ["睡10厘秒", "睡20厘秒", "睡30厘秒"]，总耗时 303ms
```

第二行是重点：三个请求分别要睡 300/100/200 毫秒，总耗时 303ms 而不是 600ms，说明它们在对端是**并发**处理的；完成顺序也按实际睡眠时长排列，而不是发出顺序。

## API 对照表

| skynet (Lua) | rskynet |
| --- | --- |
| `skynet.send(addr, type, ...)` | `ctx.send(addr, mtype, payload)` / `ctx.post(addr, payload)` |
| `skynet.call(addr, type, ...)` | `ctx.call(addr, mtype, payload).await` / `ctx.request(addr, payload).await` |
| `skynet.ret(...)` | `ctx.reply(&msg, payload)` |
| `skynet.fork(f)` | `ctx.spawn(future)` |
| `skynet.sleep(ti)` | `ctx.sleep(ticks).await` / `ctx.sleep_ms(ms).await` |
| `skynet.newservice(name)` | `ctx.launch(kind, args).await` |
| `skynet.register(".name")` | `ctx.register_name("name")` |
| `skynet.localname(".name")` | `ctx.query_name("name")` |
| `skynet.exit()` / `skynet.kill(addr)` | `ctx.exit()` / `ctx.kill(addr)` |
| `skynet.abort()` | `ctx.abort()` |
| `skynet.now()` / `skynet.time()` | `ctx.now()` / `ctx.time()` |
| `skynet.error(...)` | `ctx.log(...)` / `rskynet::log!(ctx, "...")` |
| `skynet.stat("mqlen"/"message"/"cpu")` | `ctx.mailbox_len()` / `ctx.message_count()` / `ctx.cpu_cost()` |

寻址方式也照搬：`":0100000a"` 是十六进制 handle，`".name"` 是本地名字，直接传 `u32` 则是 handle。

## 源码对照

模块名刻意与 `skynet-src` 的文件名对齐，方便逐一比对：

| rskynet | skynet | 内容 |
| --- | --- | --- |
| `message.rs` | `skynet_mq.h` | `Message` / `MsgType`（数值与 `PTYPE_*` 一致）/ `Payload` |
| `mq.rs` | `skynet_mq.c` | 每服务邮箱、全局队列、`in_global` 标志、过载检测 |
| `handle.rs` | `skynet_handle.c` | handle 分配（harbor 占高 8 位）、槽位倍增、本地名字表 |
| `server.rs` | `skynet_server.c` | `ServiceContext`、`Node`、消息分发主循环、服务生命周期 |
| `timer.rs` | `skynet_timer.c` | 分层时间轮，256 格近期轮 + 4 层 64 格，精度 10ms |
| `module.rs` | `skynet_module.c` | 服务类型注册表（静态注册取代 `dlopen`） |
| `start.rs` | `skynet_start.c` | 配置、线程池、引导、退出 |
| `context.rs` | `lualib/skynet.lua` | 用户侧 API：`call` / `send` / `fork` / `sleep` |
| `session.rs` | `lualib/skynet.lua` | `session_id_coroutine` 的对应物 |
| `task.rs` | Lua 协程池 | 服务内 executor、`SvcCell` |
| `service/logger.rs` | `service_logger.c` | 日志服务 |
| `service/bootstrap.rs` | `bootstrap.lua` | 引导服务 |

## 为什么服务状态可以不加锁

调度器保证**同一个服务在任意时刻只会被一个 worker 线程执行**（由 `in_global` 标志维持），所以服务内部天生是单线程访问的，只是「哪个线程」会随调度变化。`SvcCell<T>` 就建立在这条不变量上：它本质是 `RefCell`，只额外声明了 `Sync`，好让 `Arc<MyService>` 满足 `Send`。

用它而不用 `Mutex` 是有意的：跨 `await` 持有 `Mutex` 会真的死锁，而 `SvcCell` 只会在借用冲突时 panic，能第一时间把 bug 暴露出来。

```rust
struct Counter { hits: SvcCell<u64> }

*self.hits.borrow_mut() += 1;      // 没有锁，没有原子操作
```

## 相对 C 版的几处有意改动

- **投递即唤醒**：C 版 `skynet_globalmq_push` 不唤醒 worker，靠定时器线程每 2.5ms 顺手唤醒，代价是所有 worker 都睡着时消息最坏要等一个 tick。这里改成有人睡就直接唤一个，延迟更低而开销可忽略。
- **节点不再是全局单例**：C 版用文件级静态变量，这里收进 `Arc<Node>`，同进程可以跑多个互不干扰的节点，单元测试因此能并行。
- **字符串命令表换成类型化方法**：`skynet_command("LAUNCH", ...)` 这类字符串接口改成 `Ctx` 上的方法，编译期就能查错。
- **消息负载可以是任意 Rust 对象**：同进程传递走 `Payload::Boxed`，零拷贝、不需要序列化；`Payload::Bytes` 留给日志和将来的网络层。

## 现状与边界

已实现：服务生命周期（launch / exit / kill / abort）、消息与自定义协议号、session RPC、服务内并发、本地名字表、分层时间轮、日志服务、引导服务、TOML 配置、worker 权重调度、过载检测、退出时给在途请求回错误。

尚未实现（下一版）：socket / gate / agent 网络层、harbor / cluster 跨节点、monitor 死循环检测、debug_console、消息序列化协议。

因为内核不碰 epoll/kqueue，目前是**跨平台**的，Windows 上可以直接 `cargo run`。网络层会以独立 crate（`crates/rskynet-net`）接入，socket 线程通过同一个「推消息进邮箱」的入口汇入，不影响内核。

## 性能

Release 构建，AMD64 Windows，4 个 worker 线程：

| 场景 | 数字 |
| --- | --- |
| 单服务消息吞吐 | 约 230 万条/秒 |
| `call` 一个来回（含两次调度与两次跨线程唤醒） | 约 3.3µs（debug 构建） |

压测跑法：

```bash
cargo test --release -- --ignored --nocapture
```

## 工程结构

```
Cargo.toml              workspace 根
config/dev.toml         节点配置示例
crates/rskynet/         内核 crate
  src/                  按 skynet-src 的文件名组织
  examples/ping_pong.rs 示例
  tests/kernel.rs       端到端测试
```

第一版只有一个成员 crate。用 workspace 布局是为了将来加 `rskynet-net`、`rskynet-cluster`、`rskynet-macros` 时只需往 `members` 里加一行，不必挪目录改 import。

业务代码不需要放进本 crate：`rskynet` 是 lib crate，对外提供 `Service` trait 与 `rskynet::start(config, registry)`，使用方在自己的 app crate 里写 `main` 并注册服务——对应 skynet 里「内核是宿主、服务是外挂模块」的形态。

## 测试

```bash
cargo test                                   # 单元测试 + 端到端测试
cargo test --release -- --ignored            # 压测
cargo run --example ping_pong                # 示例
```
