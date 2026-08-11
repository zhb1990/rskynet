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
| **两级消息队列**：每服务一个邮箱，全局一个「有活干的服务」队列，N 个 worker 线程从中取活 | 一样。`Mailbox` + 运行队列，`in_global` 标志保证同一服务绝不被两个 worker 同时执行。区别是运行队列不再是一条被抢的大锁队列，见下文 |
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
    MB -->|in_global 为假时入队| GQ[运行队列]
    RQ -->|in_global 为假时入队| GQ
    GQ --> W1[worker 线程 1]
    GQ --> WN[worker 线程 N]
    W1 -->|取一件活干 就绪任务优先| svc
```

worker 的一轮调度（对照 C 版 `skynet_context_message_dispatch`）：

1. 从运行队列取一个服务，取不到就睡在条件变量上
2. 反复 `take_work()`：优先 poll 就绪任务，其次取一条消息
   - 消息是 `RESPONSE`/`ERROR` 且带 session → 直接唤醒等待中的任务
   - 其它消息 → 开一个新任务跑 `dispatch`，等价于 skynet 新建一个协程
3. 处理够 `队列长度 >> weight` 条消息就让渡，把自己交回运行队列
4. 邮箱和就绪队列都空了 → 清掉 `in_global`，脱离运行队列，等下一次投递

「就绪任务优先于新消息」不是随便定的：它对应 skynet 里被 resume 的协程会一路跑到下一次 yield，之后才轮到下一条消息。

### 运行队列：每 worker 一条，闲了去偷别人的

C 版的全局队列是一把大锁护着的环形缓冲，worker 越多争得越凶。这里换成 [BWoS 块式工作窃取队列](https://www.usenix.org/conference/atc23/presentation/wang-jiawei)（移植自 [stdexec](https://github.com/NVIDIA/stdexec) 的 `bwos_lifo_queue.hpp`）：每个 worker 一条自己的队列，在上面 push/pop 不需要任何锁，闲下来才去别人队列头部窃取。

BWoS 的巧劲在于把环形缓冲切成若干**块**：owner 只碰当前块的写指针，窃贼只碰更早那些已经「交出去」的块，两者常态下一次 CAS 都不用打照面，只有跨块的瞬间才同步一次，摊薄到每 16 个元素一次。

```mermaid
flowchart LR
    W0[worker 0] -->|投递| L0[本地 BWoS 队列 0]
    W1[worker 1] -->|投递| L1[本地 BWoS 队列 1]
    L0 -->|尾部取 后进先出| W0
    L1 -->|尾部取 后进先出| W1
    L1 -->|头部偷 先进先出| W0
    L0 -->|头部偷 先进先出| W1
    Timer[定时器线程] --> INJ[injector 队列]
    Ext[外部线程唤醒 waker] --> INJ
    L0 -.->|写满溢出| INJ
    INJ --> W0
    INJ --> W1
```

BWoS 的 owner 侧操作只允许绑定线程调用，可投递方却是任意线程——定时器线程、被外部 channel 唤醒的 waker 都算。所以还留了一条加锁的 injector 队列兜底：非 worker 线程的投递、本地队列写满的溢出都落在这里，谁都能取。worker 每取 64 次活会回头看一眼 injector，免得里面的服务被本地队列饿死。

有两处取舍值得单独说：

- **本地队列是后进先出的**，这是 BWoS lifo 变体的定义，图的是刚投递的服务多半还热在缓存里。代价是让渡回去的服务下一轮很可能又被同一个 worker 取到，不再是 skynet 那种严格 FIFO 轮转；跨 worker 的公平由窃取（从队列头部取最老的）和 injector 兜底。
- **有 worker 在睡时，投递绕开本地队列直接走 injector**。BWoS 里 owner 正在写的那一块对窃贼是隐形的，活攒不满一块就塞本地，被唤醒的 worker 会空手而归。系统忙起来（没人睡）之后自然回到全速的本地路径。

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
[     0.01] [:00000004] 1000 个来回耗时 12.03ms，平均单程 6.0µs
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
| `mq.rs` | `skynet_mq.c` | 每服务邮箱、运行队列（每 worker 一条 + injector）、`in_global` 标志、过载检测 |
| `bwos.rs` | 无对应 | BWoS 块式工作窃取队列，移植自 stdexec 的 `bwos_lifo_queue.hpp` |
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

调度器保证**同一个服务在任意时刻只会被一个 worker 线程执行**（由 `in_global` 标志维持），所以服务内部天生是单线程访问的，只是「哪个线程」会随调度变化。换成工作窃取之后这条不变量照旧：一个服务同一时刻只躺在一条队列的一个槽位里，而 BWoS 保证每个槽位只会被取走一次，被偷走也只是换了个 worker 执行。`SvcCell<T>` 就建立在这条不变量上：它本质是 `RefCell`，只额外声明了 `Sync`，好让 `Arc<MyService>` 满足 `Send`。

用它而不用 `Mutex` 是有意的：跨 `await` 持有 `Mutex` 会真的死锁，而 `SvcCell` 只会在借用冲突时 panic，能第一时间把 bug 暴露出来。

```rust
struct Counter { hits: SvcCell<u64> }

*self.hits.borrow_mut() += 1;      // 没有锁，没有原子操作
```

## 相对 C 版的几处有意改动

- **全局队列换成每 worker 一条的窃取队列**：见上文「运行队列」。
- **投递即唤醒**：C 版 `skynet_globalmq_push` 不唤醒 worker，靠定时器线程每 2.5ms 顺手唤醒，代价是所有 worker 都睡着时消息最坏要等一个 tick。这里改成有人睡就直接唤一个，延迟更低而开销可忽略。队列分散之后「有没有新活」不再是一次判断能问出来的，改用一个投递序列号：worker 在**开始找活之前**取一份快照，睡前复查它没变过才真睡，与投递方「先自增序列号、再看有没有人睡」构成 Dekker 模式，不会丢唤醒。
- **节点不再是全局单例**：C 版用文件级静态变量，这里收进 `Arc<Node>`，同进程可以跑多个互不干扰的节点，单元测试因此能并行。
- **字符串命令表换成类型化方法**：`skynet_command("LAUNCH", ...)` 这类字符串接口改成 `Ctx` 上的方法，编译期就能查错。
- **消息负载可以是任意 Rust 对象**：同进程传递走 `Payload::Boxed`，零拷贝、不需要序列化；`Payload::Bytes` 留给日志和将来的网络层。

## 现状与边界

已实现：服务生命周期（launch / exit / kill / abort）、消息与自定义协议号、session RPC、服务内并发、本地名字表、分层时间轮、日志服务、引导服务、TOML 配置、worker 权重调度与工作窃取、过载检测、退出时给在途请求回错误。

尚未实现（下一版）：socket / gate / agent 网络层、harbor / cluster 跨节点、monitor 死循环检测、debug_console、消息序列化协议。

因为内核不碰 epoll/kqueue，目前是**跨平台**的，Windows 上可以直接 `cargo run`。网络层会以独立 crate（`crates/rskynet-net`）接入，socket 线程通过同一个「推消息进邮箱」的入口汇入，不影响内核。

## 性能

AMD64 Windows，22 逻辑核。「换队列前」是同一台机器上跑的大锁全局队列版本，两栏交替测量，取四轮的中位数：

| 场景 | 换队列前 | 换队列后 |
| --- | --- | --- |
| 多服务调度吞吐（4 worker，64 个服务同时可运行） | 约 276 万次/秒 | 约 293 万次/秒 |
| 单服务消息吞吐（2 worker） | 约 210 万条/秒 | 约 210 万条/秒 |
| `call` 一个来回（4 worker，debug 构建） | 约 6µs | 约 6µs |

只有第一行会碰到运行队列，所以也只有它体现得出这次改动：另外两个场景一个压的是单个邮箱的锁，一个压的是跨线程唤醒的延迟，跟运行队列没关系。

顺带一提，worker 数远多于同时可运行的服务数时（比如 16 个 worker 只有几十个轻量服务在转），两版都会退化到线程反复挂起唤醒的开销上，吞吐掉到四线程时的六分之一。这条路还没优化。

压测跑法：

```bash
cargo test --release -- --ignored --nocapture
```

## 工程结构

```
Cargo.toml              workspace 根
config/dev.toml         节点配置示例
crates/rskynet/         内核 crate
  src/                  按 skynet-src 的文件名组织（bwos.rs 例外，C 版没有对应物）
  examples/ping_pong.rs 示例
  tests/kernel.rs       端到端测试
```

第一版只有一个成员 crate。用 workspace 布局是为了将来加 `rskynet-net`、`rskynet-cluster`、`rskynet-macros` 时只需往 `members` 里加一行，不必挪目录改 import。

业务代码不需要放进本 crate：`rskynet` 是 lib crate，对外提供 `Service` trait 与 `rskynet::start(config, registry)`，使用方在自己的 app crate 里写 `main` 并注册服务——对应 skynet 里「内核是宿主、服务是外挂模块」的形态。

## 测试

```bash
cargo test                                   # 单元测试 + 端到端测试
cargo test --release -- --ignored            # 压测（单服务吞吐 + 多服务调度吞吐）
cargo run --example ping_pong                # 示例
```

两个压测跑在同一个进程里会互相干扰，要单独看某一个的读数就加上用例名：

```bash
cargo test --release --test kernel scheduling_throughput -- --ignored --nocapture
```
