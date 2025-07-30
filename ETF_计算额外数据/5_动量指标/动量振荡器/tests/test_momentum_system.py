#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
📊 动量振荡器系统测试套件
=========================

简化版测试模块，与OBV指标保持一致的结构

作者: Claude Code Assistant  
版本: 1.0.0
创建时间: 2025-07-30
"""

import unittest
import pandas as pd
import numpy as np
import sys
import os

# 添加父目录到路径以导入主模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mom_calculator.engines.momentum_engine import MomentumEngine
from mom_calculator.infrastructure.config import MomentumConfig


class TestMomentumSystem(unittest.TestCase):
    """动量振荡器系统基础测试类"""
    
    def setUp(self):
        """测试前置设置"""
        self.engine = MomentumEngine()
        
        # 创建简单测试数据
        self.test_data = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=50, freq='D'),
            'open': np.random.uniform(10, 20, 50),
            'high': np.random.uniform(15, 25, 50),
            'low': np.random.uniform(8, 15, 50),
            'close': np.random.uniform(10, 20, 50),
            'volume': np.random.randint(1000000, 50000000, 50),
            'amount': np.random.randint(10000, 500000, 50)
        })
    
    def test_calculation_pipeline(self):
        """测试完整计算流程"""
        result = self.engine.calculate_momentum_indicators(self.test_data, "TEST001")
        
        # 检查计算成功
        self.assertTrue(result['success'])
        
        # 获取计算结果
        result_df = result['data']
        
        # 检查结果不为空
        self.assertFalse(result_df.empty)
        
        # 检查关键字段存在
        expected_fields = ['code', 'date', 'momentum_10', 'roc_5', 'pmo', 'williams_r']
        for field in expected_fields:
            self.assertTrue(field in result_df.columns, f"字段 {field} 不存在")
        
        # 检查ETF代码
        self.assertEqual(result_df['code'].iloc[0], "TEST001")
    
    def test_momentum_config(self):
        """测试配置参数"""
        self.assertEqual(len(MomentumConfig.OUTPUT_FIELDS), 14)  # 纯客观字段数量
        self.assertIn('momentum_10', MomentumConfig.OUTPUT_FIELDS)
        self.assertIn('williams_r', MomentumConfig.OUTPUT_FIELDS)


if __name__ == '__main__':
    print("📊 动量振荡器系统简化测试")
    print("=" * 30)
    unittest.main(verbosity=2)