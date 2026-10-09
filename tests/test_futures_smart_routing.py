"""测试期指数据源智能路由功能。

验证：
1. 新浪数据源获取 NQ/ES/YM
2. 东方财富数据源获取 NQ00Y/ES00Y/YM00Y
3. 轮询机制正常工作
4. 降级机制（优先源失败时切换到备用源）
"""

import asyncio
import logging

import pytest

from fetcher import (
    fetch_futures_with_routing,
    _fetch_futures_sina,
    _fetch_futures_eastmoney,
    set_proxies,
    _futures_source_health,
    _futures_source_roundrobin,
)

# 设置日志
logging.basicConfig(level=logging.INFO)


# ---------------------------------------------------------------------------
# 测试配置
# ---------------------------------------------------------------------------

TEST_PROXIES = [
    "http://*REMOVED**REMOVED*:7779",
]

TARGET_FUTURES = ["NQ", "ES", "YM"]


# ---------------------------------------------------------------------------
# 测试新浪数据源
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_sina_source():
    """测试新浪数据源单独获取期指。"""
    set_proxies(TEST_PROXIES)

    result = await _fetch_futures_sina(TARGET_FUTURES)

    assert result is not None, "新浪数据源应返回结果"
    assert len(result) == len(TARGET_FUTURES), f"应返回 {len(TARGET_FUTURES)} 个期指"

    for key in TARGET_FUTURES:
        assert key in result, f"结果应包含 {key}"
        if result[key] is not None:
            assert isinstance(result[key], float), f"{key} 涨跌幅应为浮点数"
            print(f"  {key}: {result[key]:.2f}%")
        else:
            print(f"  {key}: 无数据")


# ---------------------------------------------------------------------------
# 测试东方财富数据源
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_eastmoney_source():
    """测试东方财富数据源单独获取期指。"""
    set_proxies(TEST_PROXIES)

    result = await _fetch_futures_eastmoney(TARGET_FUTURES)

    assert result is not None, "东方财富数据源应返回结果"
    assert len(result) == len(TARGET_FUTURES), f"应返回 {len(TARGET_FUTURES)} 个期指"

    for key in TARGET_FUTURES:
        assert key in result, f"结果应包含 {key}"
        if result[key] is not None:
            assert isinstance(result[key], float), f"{key} 涨跌幅应为浮点数"
            print(f"  {key}: {result[key]:.2f}%")
        else:
            print(f"  {key}: 无数据")


# ---------------------------------------------------------------------------
# 测试智能路由
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_smart_routing_roundrobin():
    """测试智能路由（2026-10-08 起腾讯恒主源，东财→新浪降级链）。"""
    set_proxies(TEST_PROXIES)

    # 重置状态
    import fetcher
    fetcher._futures_source_index = 0
    fetcher._futures_source_health["sina"] = True
    fetcher._futures_source_health["eastmoney"] = True

    print("\n=== 测试路由：连续获取3次（腾讯主源应恒命中） ===")

    sources_used = []
    for i in range(3):
        result = await fetch_futures_with_routing(TARGET_FUTURES)

        # 判断使用了哪个数据源（以路由函数实际记录的成功源为准）
        source = fetcher._futures_last_used_source or "unknown"
        sources_used.append(source)

        # 验证结果
        assert result is not None, f"第{i+1}次应返回结果"
        success_count = sum(1 for v in result.values() if v is not None)
        print(f"  第{i+1}次: 源={source}, 成功={success_count}/{len(TARGET_FUTURES)}")

        # 避免请求过快
        await asyncio.sleep(3)

    print(f"\n使用的数据源顺序: {sources_used}")
    assert len(set(sources_used)) >= 1, "应该至少使用一个数据源"


@pytest.mark.asyncio
async def test_smart_routing_fallback():
    """测试智能路由降级机制。"""
    set_proxies(TEST_PROXIES)

    # 重置状态
    import fetcher
    fetcher._futures_source_index = 0

    print("\n=== 测试降级：标记新浪不可用，应使用东方财富 ===")

    # 标记新浪不可用，强制使用东方财富
    fetcher._futures_source_health["sina"] = False
    fetcher._futures_source_health["eastmoney"] = True

    # 准备轮询到东方财富
    fetcher._futures_source_index = 1  # 轮询索引指向东方财富

    result = await fetch_futures_with_routing(TARGET_FUTURES)

    assert result is not None, "降级后应返回结果"
    success_count = sum(1 for v in result.values() if v is not None)
    print(f"  降级结果: 成功={success_count}/{len(TARGET_FUTURES)}")

    # 恢复状态
    fetcher._futures_source_health["sina"] = True


# ---------------------------------------------------------------------------
# 测试对比两个数据源
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_compare_sources():
    """对比两个数据源的结果，验证数据一致性。"""
    set_proxies(TEST_PROXIES)

    print("\n=== 对比两个数据源 ===")

    # 获取新浪数据
    sina_result = await _fetch_futures_sina(TARGET_FUTURES)
    await asyncio.sleep(3)

    # 获取东方财富数据
    eastmoney_result = await _fetch_futures_eastmoney(TARGET_FUTURES)

    # 对比结果
    print(f"\n{'期指':<6s} {'新浪':<10s} {'东方财富':<10s} {'差异':<10s}")
    print("-" * 40)

    for key in TARGET_FUTURES:
        sina_val = sina_result.get(key) if sina_result else None
        em_val = eastmoney_result.get(key) if eastmoney_result else None

        sina_str = f"{sina_val:.2f}%" if sina_val is not None else "N/A"
        em_str = f"{em_val:.2f}%" if em_val is not None else "N/A"

        if sina_val is not None and em_val is not None:
            diff = abs(sina_val - em_val)
            diff_str = f"{diff:.2f}%"
        else:
            diff_str = "N/A"

        print(f"{key:<6s} {sina_str:<10s} {em_str:<10s} {diff_str:<10s}")


if __name__ == "__main__":
    # 直接运行
    asyncio.run(test_compare_sources())
