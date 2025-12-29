import importlib
import os
from typing import Iterable, Tuple

import pandas as pd
import pytest

from bullet_trade.data.providers.jqdata import JQDataProvider
from bullet_trade.data.providers.miniqmt import MiniQMTProvider
from bullet_trade.data.providers.tushare import TushareProvider
from bullet_trade.data import api as data_api
from bullet_trade.data.api import get_price, set_data_provider
from bullet_trade.utils.env_loader import load_env

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.requires_network,
    pytest.mark.requires_jqdata,
]

SECURITY = "000001.XSHE"
WINDOWS: Tuple[Tuple[str, str, str], ...] = (
    ("2025-05-20", "2025-06-30", "2025-06-12"),
    ("2025-09-20", "2025-10-31", "2025-10-15"),
)
PRICE_EPSILON = 1e-4
MINIQMT_AUTO_DOWNLOAD_ENV = "MINIQMT_AUTO_DOWNLOAD"


def _ensure_module(name: str, install_hint: str) -> None:
    try:
        importlib.import_module(name)
    except ImportError as exc:
        pytest.skip(f"{name} 未安装（{exc}），请执行 `{install_hint}` 后重试。")


def _normalize_bool_env(value: str) -> bool:
    return value.lower() in {"1", "true", "yes", "on"}


def _check_prerequisites() -> None:
    """加载依赖环境，检查必要条件，并输出关键信息便于调试。"""
    load_env()

    _ensure_module("jqdatasdk", "pip install jqdatasdk")
    _ensure_module("xtquant", "pip install xtquant 或 pip install bullet-trade[qmt]")

    if not os.getenv("JQDATA_USERNAME") or not os.getenv("JQDATA_PASSWORD"):
        pytest.skip(
            "缺少必要的环境变量：JQDATA_USERNAME/JQDATA_PASSWORD；"
            "请在 .env 中配置聚宽账号后重试。"
        )

    data_dir_raw = os.getenv("QMT_DATA_PATH")
    if data_dir_raw:
        data_dir = os.path.abspath(os.path.expanduser(data_dir_raw))
        print(f"[DEBUG] QMT_DATA_PATH 环境变量原始值：{data_dir_raw}，展开后路径：{data_dir}")
        if not os.path.isdir(data_dir):
            pytest.skip(
                f"QMT_DATA_PATH 指向的路径不存在：{data_dir_raw}（展开后 {data_dir}）。"
                " 请确认 miniQMT 数据目录已同步并可访问。"
            )
    else:
        print("[DEBUG] QMT_DATA_PATH 环境变量未设置，将使用 MiniQMT 默认数据目录。")

    auto_download = os.getenv(MINIQMT_AUTO_DOWNLOAD_ENV)
    if auto_download:
        normalized_auto_download = _normalize_bool_env(auto_download)
        print(
            "[DEBUG] MINIQMT_AUTO_DOWNLOAD 环境变量原始值："
            f"{auto_download}，解析后布尔值：{normalized_auto_download}"
        )
    else:
        normalized_auto_download = None
        print(
            "[DEBUG] MINIQMT_AUTO_DOWNLOAD 环境变量未设置，"
            "MiniQMTProvider 将回退到内部默认行为。"
        )

    if auto_download and not normalized_auto_download:
        pytest.skip(
            f"{MINIQMT_AUTO_DOWNLOAD_ENV} 当前为关闭状态（{auto_download}），"
            "请在 .env 中设置 MINIQMT_AUTO_DOWNLOAD=true 以自动补齐缺失行情。"
        )


def _check_jqdata_prerequisites() -> None:
    """只检查聚宽相关依赖与账号，用于不依赖 MiniQMT 的测试。"""
    load_env()

    _ensure_module("jqdatasdk", "pip install jqdatasdk")

    if not os.getenv("JQDATA_USERNAME") or not os.getenv("JQDATA_PASSWORD"):
        pytest.skip(
            "缺少必要的环境变量：JQDATA_USERNAME/JQDATA_PASSWORD；"
            "请在 .env 中配置聚宽账号后重试。"
        )


def _check_tushare_prerequisites() -> None:
    _ensure_module("tushare", "pip install tushare")
    if not os.getenv("TUSHARE_TOKEN"):
        pytest.skip("缺少必要的环境变量：TUSHARE_TOKEN，请在 .env 中配置后重试。")


def _authenticate_tushare() -> TushareProvider:
    provider = TushareProvider({"cache_dir": None})
    try:
        provider.auth()
    except Exception as exc:  # pragma: no cover - depends on external credentials
        pytest.skip(f"Tushare 认证失败：{exc}. 请检查 token 或网络。")
    return provider


def _extract_open_close(df: pd.DataFrame) -> Tuple[float, float]:
    if df.empty:
        return 0.0, 0.0
    row = df.iloc[0]
    return float(row["open"]), float(row["close"])


def _assert_pre_diff(df_none: pd.DataFrame, df_pre: pd.DataFrame, label: str) -> None:
    if df_none.empty or df_pre.empty:
        pytest.skip(f"{label} 数据为空，请检查权限或本地数据。")
    open_none, close_none = _extract_open_close(df_none)
    open_pre, close_pre = _extract_open_close(df_pre)
    if abs(open_none - open_pre) < 1e-9 and abs(close_none - close_pre) < 1e-9:
        raise AssertionError(f"{label} 前复权与未复权无差异，复权口径可能未生效。")


def _authenticate_providers() -> Tuple[JQDataProvider, MiniQMTProvider]:
    """实例化并认证双数据源，同时记录关键调试信息。"""
    jq = JQDataProvider({"cache_dir": None})
    try:
        jq.auth()
    except Exception as exc:  # pragma: no cover - depends on external credentials
        pytest.skip(f"JQData 认证失败：{exc}. 请检查账号或网络。")

    mini_config = {"mode": "backtest", "auto_download": True}
    print(f"[DEBUG] MiniQMTProvider 初始化配置：{mini_config}")
    mini = MiniQMTProvider(mini_config)
    try:
        mini.auth()
    except Exception as exc:  # pragma: no cover - depends on local xtquant setup
        pytest.skip(f"MiniQMT 初始化失败：{exc}. 请确认 xtquant 环境与数据目录。")
    print("[DEBUG] JQDataProvider 与 MiniQMTProvider 均已通过认证。")
    return jq, mini


def _extract_close(df: pd.DataFrame, security: str) -> pd.Series:
    if df.empty:
        return pd.Series(dtype="float64")
    data = df
    if isinstance(df.columns, pd.MultiIndex):
        top_levels = df.columns.get_level_values(0)
        if security in top_levels:
            data = df.xs(security, axis=1, level=0)
    if "close" not in data.columns:
        raise AssertionError("数据集中缺少 close 列，无法比较。")
    series = data["close"].astype(float)
    series.name = "close"
    return series.sort_index()


def _assert_price_parity(
    jq: JQDataProvider,
    mini: MiniQMTProvider,
    security: str,
    start: str,
    end: str,
    pre_factor_ref_date: str,
) -> None:
    """校验指定窗口的价格一致性，并输出数据长度等调试信息。"""
    jq_raw = jq.get_price(security, start_date=start, end_date=end, fq="none")
    mini_raw = mini.get_price(security, start_date=start, end_date=end, fq="none")
    # 诊断输出：原始未复权数据前10行与分红/拆分事件
    try:
        print("[DEBUG] JQData raw head(10):\n" + jq_raw.head(10).to_string())
    except Exception as _exc:
        print(f"[DEBUG] JQData raw head(10) 打印失败: {_exc}")
    try:
        print("[DEBUG] MiniQMT raw head(10):\n" + mini_raw.head(10).to_string())
    except Exception as _exc:
        print(f"[DEBUG] MiniQMT raw head(10) 打印失败: {_exc}")
    try:
        jq_div = jq.get_split_dividend(security, start_date=start, end_date=end)
    except Exception as _exc:
        jq_div = []
        print(f"[DEBUG] 读取 JQData 分红/拆分事件失败: {_exc}")
    try:
        mini_div = mini.get_split_dividend(security, start_date=start, end_date=end)
    except Exception as _exc:
        mini_div = []
        print(f"[DEBUG] 读取 MiniQMT 分红/拆分事件失败: {_exc}")
    try:
        if jq_div:
            print("[DEBUG] JQData 分红/拆分事件:\n" + pd.DataFrame(jq_div).to_string(index=False))
        else:
            print("[DEBUG] JQData 分红/拆分事件：[]")
        if mini_div:
            print("[DEBUG] MiniQMT 分红/拆分事件:\n" + pd.DataFrame(mini_div).to_string(index=False))
        else:
            print("[DEBUG] MiniQMT 分红/拆分事件：[]")
    except Exception as _exc:
        print(f"[DEBUG] 分红/拆分事件打印失败: {_exc}")
    print(
        f"[DEBUG] {security} 未复权数据行数：JQData={len(jq_raw)}，MiniQMT={len(mini_raw)}；"
        f"窗口：{start}~{end}"
    )

    jq_pre = jq.get_price(
        security,
        start_date=start,
        end_date=end,
        fq="pre",
        pre_factor_ref_date=pre_factor_ref_date,
    )
    mini_pre = mini.get_price(
        security,
        start_date=start,
        end_date=end,
        fq="pre",
        pre_factor_ref_date=pre_factor_ref_date,
    )
    print(
        f"[DEBUG] {security} 前复权数据行数：JQData={len(jq_pre)}，MiniQMT={len(mini_pre)}；"
        f"窗口：{start}~{end}，参考日：{pre_factor_ref_date}"
    )

    jq_raw_close = _extract_close(jq_raw, security)
    mini_raw_close = _extract_close(mini_raw, security)
    jq_pre_close = _extract_close(jq_pre, security)
    mini_pre_close = _extract_close(mini_pre, security)

    for label, jq_close, mini_close in (
        ("未复权", jq_raw_close, mini_raw_close),
        ("前复权", jq_pre_close, mini_pre_close),
    ):
        print(
            f"[DEBUG] {label} close 序列长度：JQData={len(jq_close)}，"
            f"MiniQMT={len(mini_close)}"
        )
        if jq_close.empty or mini_close.empty:
            pytest.skip(
                f"{label} 数据为空（区间 {start}~{end}）。请确认 miniQMT "
                "本地已同步行情，并且 JQData 账号可访问。"
            )
        aligned = jq_close.align(mini_close, join="inner")
        print(
            f"[DEBUG] {label} 对齐后的样本数：{len(aligned[0])}"
        )
        if aligned[0].empty:
            pytest.skip(
                f"{label} 数据未找到重叠日期（区间 {start}~{end}），"
                "请确认两个数据源均已同步该区间的行情。"
            )
        diff = (aligned[0] - aligned[1]).abs().max()
        try:
            max_idx = (aligned[0] - aligned[1]).abs().idxmax()
            jq_val = float(aligned[0].loc[max_idx])
            mini_val = float(aligned[1].loc[max_idx])
            print(
                f"[DEBUG] {label} 偏差详情: 日期={max_idx}, JQ={jq_val}, "
                f"MiniQMT={mini_val}, |差|={abs(jq_val-mini_val)}, 阈值={PRICE_EPSILON}"
            )
        except Exception:
            pass
        if diff >= PRICE_EPSILON:
            try:
                aligned_jq, aligned_mini = jq_close.align(mini_close, join="inner")
                _diff_series = (aligned_jq - aligned_mini).abs()
                _max_idx = _diff_series.idxmax()
                _jq_v = float(aligned_jq.loc[_max_idx])
                _mini_v = float(aligned_mini.loc[_max_idx])
                _max_diff = float(_diff_series.loc[_max_idx])
                raise AssertionError(
                    f"{label} 价格偏差超出阈值 {PRICE_EPSILON}: 实际 {_max_diff} 于 {_max_idx} "
                    f"(JQ={_jq_v}, MiniQMT={_mini_v}); 区间 {start}~{end}，参考日 {pre_factor_ref_date}"
                )
            except Exception:
                raise AssertionError(
                    f"{label} 价格偏差超出阈值 {PRICE_EPSILON}: 实际 {diff}；区间 {start}~{end}，参考日 {pre_factor_ref_date}"
                )
        assert diff < PRICE_EPSILON, (
            f"{label} 价格偏差超出阈值 {PRICE_EPSILON}: 实际 {diff}，"
            f"区间 {start}~{end}，参考日 {pre_factor_ref_date}"
        )


def _describe_windows(windows: Iterable[Tuple[str, str, str]]) -> str:
    parts = [
        f"[{start} ~ {end}]@{ref_date}"
        for start, end, ref_date in windows
    ]
    return ", ".join(parts)


def test_ping_an_bank_real_parity() -> None:
    """
    验证 miniQMT 与 JQData 在 2025 年平安银行两次派息窗口的价格一致性。

    注意：测试通过显式实例化 provider，不依赖 DEFAULT_DATA_PROVIDER。
    """
    _check_prerequisites()
    jq, mini = _authenticate_providers()

    for start, end, ref_date in WINDOWS:
        _assert_price_parity(jq, mini, SECURITY, start, end, ref_date)

    # 若运行到此处，代表两个窗口的价格均在容差内
    window_desc = _describe_windows(WINDOWS)
    print(f"平安银行 windows {window_desc} parity check passed.")


def test_tushare_vs_jqdata_single_day() -> None:
    """
    验证 Tushare 与 JQData 在 2025-07-01 的前复权/未复权差异与口径一致性。
    """
    _check_jqdata_prerequisites()
    _check_tushare_prerequisites()

    jq = JQDataProvider({"cache_dir": None})
    try:
        jq.auth()
    except Exception as exc:  # pragma: no cover - depends on external credentials
        pytest.skip(f"JQData 认证失败：{exc}. 请检查账号或网络。")
    ts = _authenticate_tushare()

    security = "000001.XSHE"
    date_str = "2025-07-01"

    jq_none = jq.get_price(security, start_date=date_str, end_date=date_str, fq=None)
    jq_pre = jq.get_price(security, start_date=date_str, end_date=date_str, fq="pre")
    ts_none = ts.get_price(security, start_date=date_str, end_date=date_str, fq=None)
    ts_pre = ts.get_price(security, start_date=date_str, end_date=date_str, fq="pre")

    _assert_pre_diff(jq_none, jq_pre, "JQData")
    _assert_pre_diff(ts_none, ts_pre, "Tushare")

    jq_open, jq_close = _extract_open_close(jq_pre)
    ts_open, ts_close = _extract_open_close(ts_pre)
    epsilon = 0.05
    assert abs(jq_open - ts_open) <= epsilon
    assert abs(jq_close - ts_close) <= epsilon


def test_multi_provider_single_day_fq_diff() -> None:
    """
    验证多个数据源在同一日期的前复权与未复权存在差异。
    """
    security = "000001.XSHE"
    date_str = "2025-07-01"
    providers = []

    if os.getenv("JQDATA_USERNAME") and os.getenv("JQDATA_PASSWORD"):
        providers.append("jqdata")
    if os.getenv("TUSHARE_TOKEN"):
        providers.append("tushare")
    if os.getenv("QMT_DATA_PATH"):
        providers.append("qmt")
    if os.getenv("QMT_SERVER_TOKEN"):
        providers.append("remote-qmt")

    if not providers:
        pytest.skip("未检测到可用的数据源配置。")

    original_provider = data_api._provider
    original_auth_attempted = data_api._auth_attempted
    original_cache = data_api._security_info_cache

    try:
        for name in providers:
            try:
                set_data_provider(name)
                df_none = get_price(security, start_date=date_str, end_date=date_str, fq=None)
                df_pre = get_price(security, start_date=date_str, end_date=date_str, fq="pre")
            except Exception as exc:
                pytest.skip(f"{name} 调用失败：{exc}")
            _assert_pre_diff(df_none, df_pre, name)
    finally:
        data_api._provider = original_provider
        data_api._auth_attempted = original_auth_attempted
        data_api._security_info_cache = original_cache
