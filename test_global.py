"""测试全球指数获取（严格5秒间隔）"""
import asyncio
import sys
sys.path.insert(0, '.')
import fetcher

async def test():
    # 先等5秒
    await asyncio.sleep(5)
    
    # 测试N225
    print("测试 N225...")
    result1 = await fetcher.fetch_global_indices(["N225"])
    print(f"N225 结果: {result1}")
    
    # 等5秒
    print("等待5秒...")
    await asyncio.sleep(5)
    
    # 测试KS11
    print("测试 KS11...")
    result2 = await fetcher.fetch_global_indices(["KS11"])
    print(f"KS11 结果: {result2}")
    
    # 等5秒
    print("等待5秒...")
    await asyncio.sleep(5)
    
    # 测试批量
    print("测试批量 [N225, KS11]...")
    result3 = await fetcher.fetch_global_indices(["N225", "KS11"])
    print(f"批量结果: {result3}")

if __name__ == "__main__":
    asyncio.run(test())
