"""
VMA系统测试包
============

提供完整的VMA系统测试框架，包括：
- 单元测试 (unit/)
- 集成测试 (integration/)
- 性能测试 (performance/)
- 测试数据 (fixtures/)
"""

import sys
from pathlib import Path

# 添加VMA系统路径到Python路径
vma_root = Path(__file__).parent.parent
sys.path.insert(0, str(vma_root))

# 测试配置
TEST_CONFIG = {
    'sample_etf_codes': ['159001', '159201', '159215'],
    'test_thresholds': ['3000万门槛', '5000万门槛'],
    'test_data_size': 100,  # 测试数据记录数
    'timeout_seconds': 30,  # 测试超时时间
}

# 测试工具函数
def get_test_etf_codes(count: int = 3) -> list:
    """获取测试用的ETF代码列表"""
    return TEST_CONFIG['sample_etf_codes'][:count]

def get_test_thresholds() -> list:
    """获取测试用的门槛列表"""
    return TEST_CONFIG['test_thresholds']