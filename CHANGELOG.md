# 更新日志

本文档记录所有重要的变更。格式基于 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.0.0/)，
版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

## [0.5.7] - 2025-12-26

### 新增
- **JoinQuant 远程实盘交易支持**：新增 `04.joinquant_remote_live_trade.ipynb` Notebook，支持通过 JoinQuant 平台进行远程实盘交易
  - 包含远程服务器连接配置（host、port、token）
  - 提供账户查询、持仓查询、限价单下单与撤单等功能
  - 附带完整的使用文档和辅助脚本引用

### 增强
- **订单处理功能优化**：
  - `QmtBrokerAdapter` 增强订单预处理，新增市场状态检查、价格限制检查和数量调整
  - `RemoteOrder` 类新增 `actual_amount` 和 `actual_price` 字段，记录实际成交信息
  - `RemoteBrokerClient` 订单方法优化，服务端统一处理 100 股取整、停牌检查和价格验证
  - 完善订单方法的文档说明，明确服务端处理逻辑和预期行为

- **TushareProvider 数据源增强**：
  - 新增 Tushare 与 JoinQuant 代码后缀映射字典
  - 实现 `_to_ts_code` 和 `_to_jq_code` 代码转换方法
  - 数据获取方法支持两种代码格式，提升兼容性
  - 优化分红和基金分配数据的解析与验证逻辑

- **持仓数据处理重构**：
  - `RemoteBrokerClient` 持仓数据解析优化，引入独立变量处理 amount、available 和 frozen 值
  - 增强可用/冻结数量的判断逻辑，支持多个可能的服务端响应字段
  - 简化 `RemotePosition` 实例创建逻辑，提升代码可读性和可维护性

### 测试
- 新增 Tushare 价格获取的单元测试，确保功能与数据完整性

---

## [0.5.6] - 2025-12-22

### 修复
- **远程交易助手价格获取问题**：修复 `bullet_trade_jq_remote_helper` 获取价格返回为空的问题（Fixes #4）
  - 添加调试信息，便于排查价格获取失败的原因

### 增强
- **MiniQMTProvider 错误处理优化**：
  - 为数据获取过程添加详细日志记录，捕获参数和错误信息
  - 改进 `_fetch_local_data` 方法，处理缺失 'time' 列等异常情况
  - 完善方法文档字符串，明确参数和返回类型说明

- **QmtDataAdapter 日志增强**：
  - 为 `get_history` 和 `get_snapshot` 方法添加详细日志记录
  - 记录详细的错误信息和请求参数，便于问题排查
  - 改进文档说明，提升代码可读性

---

## [0.5.5] - 2025-12-21

### 修复
- **TushareProvider 类拼写错误修复**：修复 `TushareProvider` 类中的拼写错误（Fixes #5）

### 文档
- **Tushare 配置文档增强**：
  - 增强 Tushare 自定义 URL 配置的文档说明
  - 更新 `docs/config.md` 和 `docs/data/DATA_PROVIDER_TUSHARE.md` 中的相关配置说明

---

## [0.5.4] - 2025-12-21

### 修复
- **碎股卖出问题修复**：修复碎股无法卖出的问题（Fixes #6）
  - 优化 `bullet_trade/core/engine.py` 中的订单处理逻辑，支持碎股（不足100股的股票）的正常卖出

### 致谢
- 感谢 [Sheng Li](https://github.com/mrlouisleel) 贡献的碎股卖出问题修复（PR #6）

---

## [0.5.3] - 2025-12-21

### 新增
- **Tushare 自定义 API URL 支持**：添加对自定义 Tushare API URL 的支持（Fixes #5）
  - 更新环境变量加载器，支持 `TUSHARE_API_URL` 配置
  - 更新示例配置文件 `env.backtest.example`，添加相关配置说明
  - 允许用户自定义 Tushare API 服务地址，提升灵活性

### 增强
- **回测引擎价格获取逻辑重构**：
  - 重构 `BacktestEngine` 中的价格获取逻辑，支持长格式数据框（包含 'code' 和 'close' 列）
  - 增强错误日志记录，当价格数据缺失或列不匹配时提供更详细的错误信息
  - 优化 `JQDataProvider` 的日志级别，将价格引擎回退检测从 info 调整为 debug

### 致谢
- 感谢 [Vanilla_Yukirin](https://github.com/Vanilla_Yukirin) 贡献的 Tushare 自定义 API URL 支持功能（PR #5）

---

## [0.5.2] - 2025-12-10

### 新增
- **交易成本管理功能**：
  - 新增 `set_commission` 函数，支持设置交易佣金
  - 新增 `set_universe` 函数，支持管理资产池
  - `OrderCost` 类新增 `commission_type` 属性，提供更灵活的费率管理

- **滑点类型扩展**：
  - 新增 `PriceRelatedSlippage`（价格相关滑点）类型
  - 新增 `StepRelatedSlippage`（阶梯相关滑点）类型
  - 保留现有的 `FixedSlippage`（固定滑点）类型

### 增强
- **交易引擎优化**：
  - 优化 `BacktestEngine` 和 `LiveEngine` 中的交易设置处理逻辑
  - 增强交易成本计算的准确性和灵活性
  - 改进滑点处理机制，支持多种滑点模型

- **文档和测试**：
  - 更新 API 文档，添加新功能的使用示例
  - 新增滑点行为测试（`tests/core/test_slippage.py`）
  - 增强订单成本测试（`tests/unit/test_order_costs.py`），确保功能正确性

---

## [未发布]

### 计划中
- 更多功能改进...

---

## 版本说明

- **新增**：新功能
- **增强**：现有功能的改进
- **修复**：Bug 修复
- **变更**：破坏性变更或重要行为变更
- **废弃**：即将移除的功能
- **移除**：已移除的功能
- **安全**：安全相关的修复

[0.5.7]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.6...v0.5.7
[0.5.6]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.4...v0.5.5
[0.5.4]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.3...v0.5.4
[0.5.3]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.2...v0.5.3
[0.5.2]: https://github.com/BulletTrade/bullet-trade/compare/v0.5.1...v0.5.2

