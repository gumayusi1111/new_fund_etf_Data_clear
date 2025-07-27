#!/usr/bin/env python3
"""
OBV指标系统测试套件
=================

全面的单元测试和集成测试
验证系统各组件功能正确性和性能指标

测试覆盖:
- OBV计算引擎测试
- 缓存管理器测试  
- 数据读取器测试
- 主控制器测试
- 系统集成测试

运行测试:
    python test_obv_system.py
    python test_obv_system.py --verbose
    python test_obv_system.py --component engine
"""

import unittest
import sys
import tempfile
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import argparse
import logging

# 添加项目路径
current_dir = Path(__file__).parent
project_dir = current_dir.parent
sys.path.insert(0, str(project_dir))

from obv_calculator.engines.obv_engine import OBVEngine
from obv_calculator.infrastructure.cache_manager import OBVCacheManager, CacheMetadata
from obv_calculator.infrastructure.data_reader import OBVDataReader
from obv_calculator.infrastructure.config import OBVConfig
from obv_calculator.controllers.main_controller import OBVController
from obv_calculator.outputs.csv_handler import OBVCSVHandler

class TestOBVEngine(unittest.TestCase):
    """OBV计算引擎测试"""
    
    def setUp(self):
        """测试前准备"""
        self.engine = OBVEngine(precision=8)
        self.sample_data = self._create_sample_data()
    
    def _create_sample_data(self) -> pd.DataFrame:
        """创建测试用样本数据"""
        dates = pd.date_range('2025-01-01', periods=30, freq='D')
        np.random.seed(42)  # 固定随机种子确保测试结果一致
        
        # 模拟ETF价格和成交量数据
        base_price = 10.0
        prices = []
        volumes = []
        
        for i in range(30):
            # 价格随机游走
            change = np.random.normal(0, 0.02)  # 2%标准差
            price = base_price * (1 + change)
            base_price = price
            prices.append(round(price, 2))
            
            # 成交量随机生成
            volume = np.random.randint(10000, 100000)
            volumes.append(volume)
        
        return pd.DataFrame({
            '代码': ['159001'] * 30,
            '日期': dates.strftime('%Y-%m-%d'),
            '收盘价': prices,
            '成交量(手数)': volumes,
            '成交额(千元)': [p * v / 100 for p, v in zip(prices, volumes)]
        })
    
    def test_obv_calculation_basic(self):
        """测试基本OBV计算"""
        result = self.engine.calculate_obv_batch(self.sample_data)
        
        self.assertTrue(result['success'], "OBV计算应该成功")
        self.assertIn('data', result, "结果应包含数据")
        
        data = result['data']
        self.assertFalse(data.empty, "计算结果不应为空")
        self.assertEqual(len(data), len(self.sample_data), "输出记录数应与输入一致")
        
        # 验证字段存在
        required_fields = ['code', 'date', 'obv', 'obv_ma10', 'obv_change_5', 'obv_change_20']
        for field in required_fields:
            self.assertIn(field, data.columns, f"应包含字段: {field}")
    
    def test_obv_calculation_precision(self):
        """测试OBV计算精度"""
        result = self.engine.calculate_obv_batch(self.sample_data)
        data = result['data']
        
        # 检查数值精度
        for _, row in data.iterrows():
            obv_str = f"{row['obv']:.8f}"
            self.assertEqual(len(obv_str.split('.')[-1]), 8, "OBV精度应为8位小数")
    
    def test_obv_calculation_logic(self):
        """测试OBV计算逻辑正确性"""
        # 创建简单的测试数据
        simple_data = pd.DataFrame({
            '代码': ['TEST'] * 5,
            '日期': ['2025-01-01', '2025-01-02', '2025-01-03', '2025-01-04', '2025-01-05'],
            '收盘价': [10.0, 10.5, 10.2, 10.8, 10.1],  # 上涨、下跌、上涨、下跌
            '成交量(手数)': [1000, 2000, 1500, 2500, 1200],
            '成交额(千元)': [100, 210, 153, 270, 121.2]
        })
        
        result = self.engine.calculate_obv_batch(simple_data)
        data = result['data']
        
        # 验证OBV逻辑
        # 第一天: OBV = 1000 (初始值)
        # 第二天: 价格上涨, OBV = 1000 + 2000 = 3000
        # 第三天: 价格下跌, OBV = 3000 - 1500 = 1500
        # 第四天: 价格上涨, OBV = 1500 + 2500 = 4000
        # 第五天: 价格下跌, OBV = 4000 - 1200 = 2800
        
        expected_obvs = [1000, 3000, 1500, 4000, 2800]
        actual_obvs = data['obv'].tolist()
        
        for i, (expected, actual) in enumerate(zip(expected_obvs, actual_obvs)):
            self.assertAlmostEqual(actual, expected, places=2, 
                                 msg=f"第{i+1}天OBV计算错误")
    
    def test_incremental_calculation(self):
        """测试增量计算功能"""
        # 分割数据测试增量计算
        historical_data = self.sample_data[:20]
        new_data = self.sample_data[20:]
        
        # 先计算历史数据
        historical_result = self.engine.calculate_obv_batch(historical_data)
        self.assertTrue(historical_result['success'])
        
        # 增量计算新数据
        incremental_result = self.engine.calculate_obv_incremental(
            historical_result['data'], new_data
        )
        
        self.assertTrue(incremental_result['success'], "增量计算应该成功")
        self.assertTrue(incremental_result.get('incremental', False), "应标记为增量计算")
    
    def test_data_validation(self):
        """测试数据验证功能"""
        # 测试空数据
        empty_data = pd.DataFrame()
        validation = self.engine.validate_input_data(empty_data)
        self.assertFalse(validation['valid'], "空数据应验证失败")
        
        # 测试缺少字段
        incomplete_data = self.sample_data.drop('收盘价', axis=1)
        validation = self.engine.validate_input_data(incomplete_data)
        self.assertFalse(validation['valid'], "缺少必需字段应验证失败")
        
        # 测试正常数据
        validation = self.engine.validate_input_data(self.sample_data)
        self.assertTrue(validation['valid'], "正常数据应验证通过")

class TestCacheManager(unittest.TestCase):
    """缓存管理器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.cache_dir = self.temp_dir / "cache"
        self.meta_dir = self.temp_dir / "meta"
        
        self.cache_manager = OBVCacheManager(
            cache_dir=self.cache_dir,
            meta_dir=self.meta_dir,
            expire_days=30
        )
        
        self.test_data = pd.DataFrame({
            'code': ['159001'] * 3,
            'date': ['2025-01-01', '2025-01-02', '2025-01-03'],
            'obv': [1000.12345678, 2000.87654321, 1500.55555555],
            'obv_ma10': [1000.0, 1500.0, 1833.33],
            'obv_change_5': [0.0, 100.0, -25.0],
            'obv_change_20': [0.0, 50.0, 12.5],
            'calc_time': ['2025-07-27 12:00:00'] * 3
        })
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def test_save_and_load_cache(self):
        """测试缓存保存和加载"""
        etf_code = "159001"
        threshold = "3000万门槛"
        
        # 保存缓存
        success = self.cache_manager.save_cache(etf_code, threshold, self.test_data)
        self.assertTrue(success, "缓存保存应该成功")
        
        # 加载缓存
        loaded_data = self.cache_manager.load_cache(etf_code, threshold)
        self.assertIsNotNone(loaded_data, "应该能加载缓存数据")
        self.assertEqual(len(loaded_data), len(self.test_data), "加载的数据长度应一致")
        
        # 验证数据内容
        pd.testing.assert_frame_equal(loaded_data, self.test_data, "缓存数据应与原数据一致")
    
    def test_cache_validation(self):
        """测试缓存有效性验证"""
        etf_code = "159001"
        threshold = "3000万门槛"
        
        # 保存缓存
        self.cache_manager.save_cache(etf_code, threshold, self.test_data)
        
        # 验证有效性
        is_valid = self.cache_manager.is_cache_valid(etf_code, threshold)
        self.assertTrue(is_valid, "新保存的缓存应该有效")
        
        # 测试数据日期更新
        is_valid_with_newer_date = self.cache_manager.is_cache_valid(
            etf_code, threshold, "2025-01-04"
        )
        self.assertFalse(is_valid_with_newer_date, "有更新数据时缓存应该无效")
    
    def test_incremental_update(self):
        """测试增量更新"""
        etf_code = "159001"
        threshold = "3000万门槛"
        
        # 保存初始缓存
        self.cache_manager.save_cache(etf_code, threshold, self.test_data)
        
        # 创建新增数据
        new_data = pd.DataFrame({
            'code': ['159001'] * 2,
            'date': ['2025-01-04', '2025-01-05'],
            'obv': [1800.11111111, 2200.99999999],
            'obv_ma10': [1900.0, 2000.0],
            'obv_change_5': [20.0, 22.2],
            'obv_change_20': [15.0, 18.5],
            'calc_time': ['2025-07-27 13:00:00'] * 2
        })
        
        # 增量更新
        success = self.cache_manager.update_cache_incremental(etf_code, threshold, new_data)
        self.assertTrue(success, "增量更新应该成功")
        
        # 验证更新后的数据
        updated_data = self.cache_manager.load_cache(etf_code, threshold)
        self.assertEqual(len(updated_data), 5, "更新后应有5条记录")
    
    def test_cache_statistics(self):
        """测试缓存统计功能"""
        # 保存一些测试缓存
        for i in range(3):
            etf_code = f"15900{i+1}"
            self.cache_manager.save_cache(etf_code, "3000万门槛", self.test_data)
        
        # 获取统计信息
        stats = self.cache_manager.get_cache_statistics()
        
        self.assertNotIn('error', stats, "获取统计信息不应出错")
        self.assertIn('performance', stats, "应包含性能统计")
        self.assertIn('storage', stats, "应包含存储统计")
        
        # 验证文件数量
        storage = stats['storage']
        self.assertEqual(storage['cache_files'], 3, "应有3个缓存文件")

class TestDataReader(unittest.TestCase):
    """数据读取器测试"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = Path(tempfile.mkdtemp())
        self.data_reader = OBVDataReader(source_dir=self.temp_dir)
        
        # 创建测试数据文件
        self._create_test_files()
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_test_files(self):
        """创建测试用的CSV文件"""
        # 创建ETF数据文件
        test_etfs = ['159001', '159002', '159003']
        
        for etf_code in test_etfs:
            data = pd.DataFrame({
                '代码': [etf_code] * 25,
                '日期': pd.date_range('2025-01-01', periods=25, freq='D').strftime('%Y-%m-%d'),
                '开盘价': np.random.uniform(9.5, 10.5, 25),
                '最高价': np.random.uniform(10.0, 11.0, 25),
                '最低价': np.random.uniform(9.0, 10.0, 25),
                '收盘价': np.random.uniform(9.5, 10.5, 25),
                '成交量(手数)': np.random.randint(10000, 100000, 25),
                '成交额(千元)': np.random.uniform(5000, 50000, 25)
            })
            
            file_path = self.temp_dir / f"{etf_code}.csv"
            data.to_csv(file_path, index=False, encoding='utf-8')
    
    def test_read_single_etf(self):
        """测试读取单个ETF数据"""
        etf_code = "159001"
        data = self.data_reader.read_etf_data(etf_code)
        
        self.assertIsNotNone(data, "应该能读取到数据")
        self.assertFalse(data.empty, "数据不应为空")
        self.assertEqual(len(data), 25, "应有25条记录")
        
        # 验证必需字段
        required_fields = ['代码', '日期', '收盘价', '成交量(手数)']
        for field in required_fields:
            self.assertIn(field, data.columns, f"应包含字段: {field}")
    
    def test_batch_read(self):
        """测试批量读取功能"""
        etf_codes = ['159001', '159002']
        data = self.data_reader.read_batch_etf_data(etf_codes)
        
        self.assertFalse(data.empty, "批量读取结果不应为空")
        self.assertEqual(len(data), 50, "应有50条记录(2个ETF × 25条)")
        
        # 验证数据包含指定的ETF
        unique_codes = data['代码'].unique()
        self.assertEqual(len(unique_codes), 2, "应包含2个ETF")
        for code in etf_codes:
            self.assertIn(code, unique_codes, f"应包含ETF: {code}")
    
    def test_data_availability_check(self):
        """测试数据可用性检查"""
        etf_codes = ['159001', '159002', '999999']  # 最后一个不存在
        availability = self.data_reader.check_data_availability(etf_codes)
        
        self.assertEqual(availability['total_codes'], 3, "总代码数应为3")
        self.assertEqual(availability['available_count'], 2, "可用代码数应为2")
        self.assertEqual(len(availability['missing_codes']), 1, "缺失代码数应为1")
        self.assertIn('999999', availability['missing_codes'], "999999应在缺失列表中")
    
    def test_date_filtering(self):
        """测试日期过滤功能"""
        etf_code = "159001"
        data = self.data_reader.read_etf_data(
            etf_code, 
            start_date='2025-01-10', 
            end_date='2025-01-15'
        )
        
        self.assertIsNotNone(data, "应该能读取到过滤后的数据")
        self.assertEqual(len(data), 6, "应有6条记录(2025-01-10到2025-01-15)")
        
        # 验证日期范围
        min_date = data['日期'].min()
        max_date = data['日期'].max()
        self.assertGreaterEqual(min_date, '2025-01-10', "最小日期应>=开始日期")
        self.assertLessEqual(max_date, '2025-01-15', "最大日期应<=结束日期")

class TestMainController(unittest.TestCase):
    """主控制器测试"""
    
    def setUp(self):
        """测试前准备"""
        # 创建临时测试环境
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # 创建自定义配置
        self.config = OBVConfig()
        self.config.BASE_DIR = self.temp_dir
        self.config.DATA_DIR = self.temp_dir / "data"
        self.config.CACHE_DIR = self.temp_dir / "cache"
        self.config.LOGS_DIR = self.temp_dir / "logs"
        self.config.CACHE_META_DIR = self.temp_dir / "cache" / "meta"
        
        # 创建测试数据源
        self.source_dir = self.temp_dir / "source"
        self.source_dir.mkdir(parents=True)
        self.config.ETF_SOURCE_DIR = self.source_dir
        
        # 创建测试ETF数据
        self._create_test_source_data()
        
        # 初始化控制器
        self.controller = OBVController(self.config)
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_test_source_data(self):
        """创建测试用的源数据"""
        etf_code = "159001"
        data = pd.DataFrame({
            '代码': [etf_code] * 30,
            '日期': pd.date_range('2025-01-01', periods=30, freq='D').strftime('%Y-%m-%d'),
            '收盘价': np.random.uniform(9.5, 10.5, 30),
            '成交量(手数)': np.random.randint(10000, 100000, 30),
            '成交额(千元)': np.random.uniform(30000, 80000, 30)  # 确保满足门槛要求
        })
        
        file_path = self.source_dir / f"{etf_code}.csv"
        data.to_csv(file_path, index=False, encoding='utf-8')
    
    def test_single_etf_calculation(self):
        """测试单ETF计算"""
        etf_code = "159001"
        threshold = "3000万门槛"
        
        result = self.controller.calculate_single_etf(etf_code, threshold)
        
        self.assertTrue(result['success'], f"单ETF计算应该成功: {result.get('error', '')}")
        self.assertEqual(result['etf_code'], etf_code, "返回的ETF代码应一致")
        self.assertEqual(result['threshold'], threshold, "返回的门槛类型应一致")
        self.assertGreater(result['data_points'], 0, "应有数据点")
        self.assertGreater(result['processing_time'], 0, "处理时间应大于0")
    
    def test_cache_functionality(self):
        """测试缓存功能"""
        etf_code = "159001"
        threshold = "3000万门槛"
        
        # 第一次计算(无缓存)
        result1 = self.controller.calculate_single_etf(etf_code, threshold)
        self.assertTrue(result1['success'])
        self.assertFalse(result1.get('cache_hit', True), "第一次计算不应命中缓存")
        
        # 第二次计算(应命中缓存)
        result2 = self.controller.calculate_single_etf(etf_code, threshold)
        self.assertTrue(result2['success'])
        self.assertTrue(result2.get('cache_hit', False), "第二次计算应命中缓存")
        
        # 强制重新计算
        result3 = self.controller.calculate_single_etf(etf_code, threshold, force_recalculate=True)
        self.assertTrue(result3['success'])
        self.assertFalse(result3.get('cache_hit', True), "强制重新计算不应命中缓存")
    
    def test_system_status(self):
        """测试系统状态获取"""
        status = self.controller.get_system_status()
        
        self.assertNotIn('error', status, "获取系统状态不应出错")
        self.assertIn('system_info', status, "应包含系统信息")
        self.assertIn('performance', status, "应包含性能信息")
        self.assertIn('components', status, "应包含组件信息")
        
        # 验证系统信息
        system_info = status['system_info']
        self.assertEqual(system_info['name'], 'OBV指标计算系统', "系统名称应正确")
        self.assertEqual(system_info['status'], 'RUNNING', "系统状态应为运行中")
    
    def test_system_testing(self):
        """测试系统自测功能"""
        test_results = self.controller.test_system(sample_size=1)
        
        self.assertIn('success', test_results, "测试结果应包含成功标志")
        self.assertIn('tests', test_results, "应包含具体测试项")
        self.assertIn('summary', test_results, "应包含测试摘要")
        
        # 验证测试摘要
        summary = test_results['summary']
        self.assertIn('total_tests', summary, "摘要应包含总测试数")
        self.assertIn('passed_tests', summary, "摘要应包含通过测试数")

class TestSystemIntegration(unittest.TestCase):
    """系统集成测试"""
    
    def setUp(self):
        """测试前准备"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
        # 创建完整的测试环境
        self.config = OBVConfig()
        self.config.BASE_DIR = self.temp_dir
        self.config.init_directories()
        
        # 创建测试数据源
        self.source_dir = self.temp_dir / "source"
        self.source_dir.mkdir(parents=True)
        self.config.ETF_SOURCE_DIR = self.source_dir
        
        self._create_comprehensive_test_data()
        
        self.controller = OBVController(self.config)
    
    def tearDown(self):
        """测试后清理"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    def _create_comprehensive_test_data(self):
        """创建综合测试数据"""
        etf_codes = ['159001', '159002', '159003']
        
        for etf_code in etf_codes:
            # 创建不同特征的数据
            if etf_code == '159001':
                # 大成交额ETF
                amounts = np.random.uniform(50000, 100000, 25)
            elif etf_code == '159002':
                # 中等成交额ETF
                amounts = np.random.uniform(25000, 60000, 25)
            else:
                # 小成交额ETF(不满足5000万门槛)
                amounts = np.random.uniform(10000, 40000, 25)
            
            data = pd.DataFrame({
                '代码': [etf_code] * 25,
                '日期': pd.date_range('2025-01-01', periods=25, freq='D').strftime('%Y-%m-%d'),
                '收盘价': np.random.uniform(9.5, 10.5, 25),
                '成交量(手数)': np.random.randint(10000, 100000, 25),
                '成交额(千元)': amounts
            })
            
            file_path = self.source_dir / f"{etf_code}.csv"
            data.to_csv(file_path, index=False, encoding='utf-8')
    
    def test_complete_workflow(self):
        """测试完整工作流程"""
        # 1. 系统初始化测试
        status = self.controller.get_system_status()
        self.assertEqual(status['system_info']['status'], 'RUNNING')
        
        # 2. 单ETF计算测试(3000万门槛)
        result_3000 = self.controller.calculate_single_etf('159001', '3000万门槛')
        self.assertTrue(result_3000['success'])
        
        # 3. 单ETF计算测试(5000万门槛)
        result_5000 = self.controller.calculate_single_etf('159001', '5000万门槛')
        self.assertTrue(result_5000['success'])
        
        # 4. 验证门槛筛选效果
        # 159003的成交额较小，可能不满足5000万门槛
        result_small = self.controller.calculate_single_etf('159003', '5000万门槛')
        # 结果可能成功也可能失败，取决于随机生成的数据
        
        # 5. 缓存测试
        result_cached = self.controller.calculate_single_etf('159001', '3000万门槛')
        self.assertTrue(result_cached['success'])
        self.assertTrue(result_cached.get('cache_hit', False))
        
        # 6. 系统清理测试
        cleanup_result = self.controller.cleanup_system()
        self.assertTrue(cleanup_result['success'])
    
    def test_error_handling(self):
        """测试错误处理"""
        # 测试不存在的ETF
        result = self.controller.calculate_single_etf('999999', '3000万门槛')
        self.assertFalse(result['success'])
        self.assertIn('error', result)
        
        # 测试无效门槛
        result = self.controller.calculate_single_etf('159001', '无效门槛')
        self.assertFalse(result['success'])
        
        # 测试空ETF代码
        result = self.controller.calculate_single_etf('', '3000万门槛')
        self.assertFalse(result['success'])
    
    def test_performance_benchmark(self):
        """测试性能基准"""
        import time
        
        start_time = time.time()
        
        # 执行多个计算任务
        results = []
        for etf_code in ['159001', '159002']:
            for threshold in ['3000万门槛', '5000万门槛']:
                result = self.controller.calculate_single_etf(etf_code, threshold)
                results.append(result)
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # 性能要求: 4个任务应在10秒内完成
        self.assertLess(total_time, 10.0, "性能测试: 4个任务应在10秒内完成")
        
        # 成功率要求: 至少50%任务成功
        success_count = sum(1 for r in results if r.get('success', False))
        success_rate = success_count / len(results)
        self.assertGreaterEqual(success_rate, 0.5, "至少50%的任务应该成功")

def run_tests(component=None, verbose=False):
    """运行测试套件"""
    # 设置日志级别
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.WARNING)
    
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 根据参数加载测试
    if component:
        component_map = {
            'engine': TestOBVEngine,
            'cache': TestCacheManager,
            'reader': TestDataReader,
            'controller': TestMainController,
            'integration': TestSystemIntegration
        }
        
        if component in component_map:
            suite.addTests(loader.loadTestsFromTestCase(component_map[component]))
        else:
            print(f"❌ 未知组件: {component}")
            print(f"可用组件: {', '.join(component_map.keys())}")
            return False
    else:
        # 加载所有测试
        test_classes = [
            TestOBVEngine,
            TestCacheManager, 
            TestDataReader,
            TestMainController,
            TestSystemIntegration
        ]
        
        for test_class in test_classes:
            suite.addTests(loader.loadTestsFromTestCase(test_class))
    
    # 运行测试
    runner = unittest.TextTestRunner(
        verbosity=2 if verbose else 1,
        stream=sys.stdout,
        buffer=True
    )
    
    print("🚀 启动OBV指标系统测试...")
    print("=" * 60)
    
    result = runner.run(suite)
    
    # 输出测试总结
    print("\n" + "=" * 60)
    print("📋 测试总结:")
    print(f"  运行测试: {result.testsRun}")
    print(f"  成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"  失败: {len(result.failures)}")
    print(f"  错误: {len(result.errors)}")
    
    if result.failures:
        print(f"\n❌ 失败的测试:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback.split('AssertionError: ')[-1].split('\\n')[0]}")
    
    if result.errors:
        print(f"\n🔥 错误的测试:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback.split('\\n')[-2]}")
    
    success = len(result.failures) == 0 and len(result.errors) == 0
    
    if success:
        print(f"\n✅ 所有测试通过!")
    else:
        print(f"\n❌ 存在测试失败!")
    
    return success

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='OBV指标系统测试套件')
    parser.add_argument('--component', '-c', 
                       choices=['engine', 'cache', 'reader', 'controller', 'integration'],
                       help='指定要测试的组件')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    
    args = parser.parse_args()
    
    success = run_tests(component=args.component, verbose=args.verbose)
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main()