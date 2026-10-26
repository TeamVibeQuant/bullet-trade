# 大 QMT 共享内存迁移

新版 helper 和 `--server-type big_qmt` 默认使用 `shm`。链路为：

```text
Windows 大 QMT（单文件 helper，ContextInfo / passorder / cancel）
       ⇅ ctypes → Windows 命名共享内存 + Mutex（同机、同登录会话）
Windows 外部 Python：bullet-trade server --server-type big_qmt
       ⇅ 现有 58620 qmt-remote 协议
本机或远端：run_bullettrade_live.bat，选项 2，--broker qmt-remote
```

## 为什么选 ctypes

`ctypes` 是调用 Windows API 的工具，共享内存是通信机制，两者并不互斥。这里直接调用 `CreateFileMappingW(INVALID_HANDLE_VALUE)` / `MapViewOfFile`，使用系统分页文件支持的命名内存；helper 不打开磁盘文件，不建立 socket，不依赖 Tornado、mmap 扩展或 Python 3.8 才加入的 `multiprocessing.shared_memory`。helper 和协议源码保留 Python 3.6 语法。

优先正常导入 ctypes；如果导入失败，只复用解释器中已经加载且具备必要属性的 `sys.modules['ctypes']`，健康信息会显示 `standard_import` 或 `loaded_modules`。不会从其他 Python 环境加载 DLL/pyd，也没有网络自动回退。若 ctypes 或 Windows 对象接口被禁用，启动明确失败。无法据此保证未来券商不会限制此通道。

依据：[Microsoft 命名共享内存](https://learn.microsoft.com/en-us/windows/win32/memory/creating-named-shared-memory)、[Windows 互斥量等待语义](https://learn.microsoft.com/en-us/windows/win32/api/synchapi/nf-synchapi-waitforsingleobject)、[Python shared_memory 版本要求](https://docs.python.org/3/library/multiprocessing.shared_memory.html)。

## 配置与启动

QMT 和外部 server 必须在同一台 Windows 电脑、同一个用户的登录会话运行，使用一致的权限级别。`Local\` 不跨 Windows 登录会话；不要把 server 放进另一台电脑、WSL、容器或 Session 0 的系统服务。live 策略仍可通过现有网络连接外部 server。

1. 在 QMT 编辑器中替换完整的 `helpers/big_qmt_gateway_strategy_sample.py`。IPC 源码已内嵌，不需要 QMT 读取额外 `.py` 或 `.env`。文件保留原来的 GBK 编码声明。
2. 核对顶部 `ACCOUNT_ID`、`ACCOUNT_TYPE`、`GATEWAY_PASSWORD`、`GATEWAY_SECRET`。保留 `GATEWAY_TRANSPORT = "shm"`。初次验证时可将 `ENABLE_TRADING`、`ENABLE_CANCEL_ORDER` 设为 `False`；修改后需重新运行 helper。
3. 外部 server 环境文件（当前项目为 `.env.live.hanyi_zheshang_server`）添加：

   ```dotenv
   BIG_QMT_GATEWAY_TRANSPORT=shm
   BIG_QMT_SHM_NAME=Local\BulletTrade_BigQMT_v1
   BIG_QMT_SHM_SIZE=16777216
   BIG_QMT_SHM_POLL_SECONDS=0.01
   BIG_QMT_GATEWAY_TIMEOUT_SECONDS=30
   BIG_QMT_SHM_JOURNAL=.bullet-trade/big_qmt_shm.sqlite3
   ```

   原来的 `BIG_QMT_GATEWAY_PASSWORD`、`BIG_QMT_GATEWAY_SECRET` 继续使用，必须与 helper 一致。`QMT_SERVER_TOKEN` 仍是 live → server 的独立凭证。`BIG_QMT_GATEWAY_URL` 在 shm 模式下忽略。

   名称、大小必须与 helper 的 `SHM_NAME`、`SHM_SIZE` 完全一致。每个独立 QMT helper/账户通道使用不同名称、独立 journal 路径；同一通道的 server 重启必须保留相同 journal。bat 会先切换到项目目录，避免相对 journal 路径漂移；其他启动方式建议用绝对路径。

4. 在 QMT 内运行 helper，看到 `shared memory ready ... ctypes=...`。与原来的前台 Tornado 模式一样，它持续占用 `init` 策略线程处理请求，不依赖交易时段的 bar 到达。QMT API 不移到外部线程执行。停止回调设置退出标记，或手动中断策略后，由该线程释放映射与互斥量。
5. 在项目根目录运行只读诊断：

   ```bat
   python third_party\bullet_trade\helpers\big_qmt_shm_probe.py --env-file .env.live.hanyi_zheshang_server
   ```

   应显示 `transport=shm`、`shm_alive=true`、`ready=true`、`gateway_build_id=20261026_shm_v1`。诊断不下单、不撤单，不打印密码或资金余额。
6. 运行 `run_bullettrade_server.bat`，再运行 `run_bullettrade_live.bat` 选择 2。策略仍使用 `--broker qmt-remote`，无需改策略文件或 remote 环境文件。

## Live 初始化与退出

- Live 先连接 broker 并读取账户初始快照，再执行 `initialize`、`after_code_changed`（需要时）和 `process_initialize`。首次账户同步失败会中止初始化。
- 初始化期间读取 `context.portfolio` 使用已取得的初始值，不触发额外账户查询或对临时子账户做现金对账。子账户恢复和初始化完成后，才保存运行态并启动自动保存、后台查询。
- 退出时等待正在执行的同步查询和自动保存结束，在 broker 连接仍可用、运行锁仍持有时完成最终保存，再断开连接、释放锁。已关闭运行态的迟到保存回调不会再次访问 broker。
- 保存前账户或持仓查询失败时保留上次子账户快照；持仓查询异常不会被解释成空仓。初始化未完成就中断时，也不会在退出流程中覆盖上次运行态。

验证顺序：启动 live 后确认账户资金、持仓及子账户恢复结果，再按 Ctrl+C 退出。连接可用时 `subportfolios.json` 的 `saved_at` 应更新到退出时间，退出日志不应再出现 `remote connection 尚未启动`。这些修复位于外部 Python 的 live/broker 层，无需重新复制 QMT helper。

## 请求与故障语义

- 单槽串行 RPC，16 MiB 默认容量。已有全部 JSON 路由保留，包括行情、历史、交易日、成分股、账户、持仓、委托、成交、下单、撤单。
- 固定小端消息头包含 magic、协议版本、映射容量、helper 会话、序号、状态、截止时间、活动时间、正文长度和 SHA-256。正文先写、头最后提交；跨进程读写都持有短期 state Mutex。校验失败立即停止，不把半包交给 QMT。
- owner Mutex 保证一个通道只有一个 helper；transaction Mutex 将不同线程/外部进程的请求串行化。state Mutex 不覆盖 QMT API 执行，客户端可区分“尚未领取”和“正在执行”。
- 请求过期且尚未领取：撤回或拒绝，明确未执行。QMT 已领取后超时、断开或重启：结果未知，绝不自动重发。新会话不会执行旧槽内请求。超大响应返回明确错误，历史数据应分批查询或同时调整两端容量。
- helper 重启后新请求自动重新打开映射；已发布请求跨重启只报未知，不重放。长时间执行的旧请求不会被后续请求覆盖。
- `qmt-remote` 为每次下单生成幂等键；网络断线后下单/撤单不再自动重发，查询继续自动重连重试。外部 server 本地排队时间也计入超时，排队过期不会再发送交易请求。
- 外部 SQLite journal 在发布下单/撤单之前同步写入意图，成功响应后写入结果。同 `idempotency_key`（无此字段则 `request_id`）和相同载荷返回已有结果；载荷冲突拒绝。未知交易意图会阻止同通道后续下单/撤单，查询仍可用。记录不自动过期或删除。
- 没有事务能把券商成交与本地回执原子提交；不能宣称跨崩溃“恰好执行一次”。此实现选择不重发，并保留请求参数及 `qmt_user_order_id` 供核对。新生成的幂等键代表新的订单，不能代替业务层去重。

## 文件 IO 与订单标签

shm 模式的 QMT helper 只输出控制台日志，不创建日志文件，不读写订单标签 JSON，不将日志传播给可能写文件的 Python 父 logger。错误格式化也不读取源码堆栈文件。QMT 自身行情接口在客户端内部是否仍可下载/读取其行情缓存，取决于券商版本，需要目标环境验证。

订单标签随已认证 RPC 回传，保存在外部 server journal；helper 新会话首次业务请求恢复标签。虚拟账户、remark、QMT userOrderId 的既有映射继续使用。客户端或 QMT 在委托后、标签回执前同时崩溃时，需按 journal 和券商委托核对；不能推断所有标签都已落盘。不要删除 journal 来解除交易保护。

## 未知结果的恢复

先停止 live 策略，等待或停止旧的 QMT 请求，确认它不再执行。通过 QMT 委托/成交界面及 `/orders`、`/trades` 查询，按请求参数和 `qmt_user_order_id` 核对是否已提交。检查 journal：

```bat
python third_party\bullet_trade\helpers\big_qmt_shm_probe.py --env-file .env.live.hanyi_zheshang_server --pending
```

完成核对后，由操作者解除对应意图的阻塞，附核对记录：

```bat
python third_party\bullet_trade\helpers\big_qmt_shm_probe.py --env-file .env.live.hanyi_zheshang_server --resolve REQUEST_ID --note "已核对券商委托及成交，记录实际委托号和结论"
```

此命令仅修改外部 journal，不下单、不撤单，也不重放原意图。它保留原幂等键为已人工核对状态。不要在旧请求仍可能执行时解除阻塞。恢复 live 前先同步实际持仓与委托。

## 验证与兼容

协议及故障测试：

```bat
set PYTHONPATH=third_party\bullet_trade
python -m pytest third_party\bullet_trade\tests\server\test_shared_memory_rpc.py -q
```

其中真实 Win32 跨进程映射/互斥量测试只在 Windows 运行，Linux 会跳过。Linux 模拟内存测试不代表目标券商 QMT 已验证。Windows 上先验证只读接口、空闲时段请求、helper/server 独立重启，再在模拟环境验证下单、撤单、未知结果核对及 GBK 单文件加载。

维护 IPC 源码时修改 `bullet_trade/server/shared_memory_rpc.py`，再运行 `python helpers/sync_big_qmt_ipc.py` 更新内嵌副本；`--check` 和测试会校验两端源码一致。

旧客户端如需 HTTP，必须显式设置 helper `GATEWAY_TRANSPORT="http"` 和 server `BIG_QMT_GATEWAY_TRANSPORT=http`，按[旧 HTTP 服务向导](big-qmt-server.md)运行。shm 失败不会降级到 HTTP。
