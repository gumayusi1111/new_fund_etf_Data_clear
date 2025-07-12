#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD主控制器 - 重构版
====================

参照其他趋势类指标系统的主控制器架构
提供统一的MACD计算接口和业务流程协调
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional, Any
from ..infrastructure.config import MACDConfig
from ..infrastructure.data_reader import MACDDataReader
from ..infrastructure.cache_manager import MACDCacheManager
from ..engines.macd_engine import MACDEngine


class MACDMainController:
    """MACD主控制器 - 重构版（与其他系统保持一致的架构）"""
    
    def __init__(self, parameter_set: str = 'standard', adj_type: str = "前复权", 
                 output_dir: Optional[str] = None, enable_cache: bool = True, 
                 performance_mode: bool = False):
        """
        初始化MACD主控制器 - 重构版本
        
        Args:
            parameter_set: MACD参数组合 ('standard', 'sensitive', 'smooth')
            adj_type: 复权类型，默认"前复权"
            output_dir: 输出目录，默认None（使用配置中的默认目录）
            enable_cache: 是否启用缓存，默认True
            performance_mode: 是否启用性能模式（关闭调试输出）
        """
        print("=" * 60)
        print("🚀 MACD主控制器启动 - 重构版本")
        print("=" * 60)
        
        # 保存参数设置
        self.parameter_set = parameter_set
        
        # 初始化配置
        self.config = MACDConfig(
            parameter_set=parameter_set, 
            adj_type=adj_type,
            enable_cache=enable_cache, 
            performance_mode=performance_mode
        )
        
        # 创建基于参数的输出目录结构
        base_output_dir = output_dir or self.config.default_output_dir
        parameter_folder_map = {
            'standard': '标准',
            'sensitive': '敏感', 
            'smooth': '平滑'
        }
        parameter_folder = parameter_folder_map.get(parameter_set, '标准')
        
        # 设置完整的输出路径：data/{threshold}/{parameter_folder}
        self.output_dir = base_output_dir
        self.parameter_folder = parameter_folder
        
        # 确保输出目录结构存在
        self._ensure_output_directories()
        
        self.enable_cache = enable_cache
        self.performance_mode = performance_mode
        
        # 初始化组件
        print("📖 MACD数据读取器初始化完成")
        self.data_reader = MACDDataReader(self.config)
        
        print("⚙️ MACD计算引擎初始化完成")
        self.macd_engine = MACDEngine(self.config)
        
        # 初始化缓存管理器
        if self.enable_cache:
            self.cache_manager = MACDCacheManager(self.config)
        else:
            self.cache_manager = None
        
        # 输出初始化信息
        print("=" * 60)
        print("🚀 MACD主控制器初始化完成")
        print(f"   🔧 参数组合: {parameter_set}")
        print(f"   📊 MACD参数: EMA{self.config.get_macd_periods()}")
        print(f"   🗂️ 缓存: {'启用' if self.enable_cache else '禁用'}")
        print(f"   🚀 性能模式: {'启用' if self.performance_mode else '禁用'}")
    
    def _ensure_output_directories(self) -> None:
        """确保输出目录结构存在"""
        thresholds = ["3000万门槛", "5000万门槛"]
        parameters = ["标准", "敏感", "平滑"]
        
        # 创建基础输出目录
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        
        # 为每个门槛下创建三个参数文件夹
        for threshold in thresholds:
            for parameter in parameters:
                directory = os.path.join(self.output_dir, threshold, parameter)
                if not os.path.exists(directory):
                    os.makedirs(directory)
    
    def calculate_single_etf(self, etf_code: str, save_result: bool = True, 
                           threshold: Optional[str] = None, parameter_folder: Optional[str] = None, 
                           verbose: bool = False) -> Optional[Dict]:
        """
        计算单个ETF的MACD指标
        
        Args:
            etf_code: ETF代码
            save_result: 是否保存结果到文件
            threshold: 门槛类型（保留接口一致性）
            parameter_folder: 参数文件夹名称（如"标准"、"敏感"、"平滑"）
            verbose: 是否显示详细信息
            
        Returns:
            计算结果字典
        """
        try:
            if verbose:
                print(f"🔍 开始计算 {etf_code} 的MACD指标...")
            
            # 使用默认门槛类型进行缓存
            default_threshold = threshold or "3000万门槛"
            param_folder = parameter_folder or "标准"
            
            # 检查缓存
            if self.cache_manager and self.enable_cache:
                # 获取源文件路径
                source_file_path = self.data_reader.get_etf_file_path(etf_code)
                if source_file_path and self.cache_manager.is_cache_valid(etf_code, default_threshold, source_file_path, param_folder):
                    # 从缓存加载
                    cached_df = self.cache_manager.load_cached_etf_data(etf_code, default_threshold, param_folder)
                    if cached_df is not None and not cached_df.empty:
                        if verbose:
                            print(f"📦 {etf_code} 从缓存加载，共{len(cached_df)}个数据点")
                        
                        # 保存结果到输出目录（如果需要）
                        if save_result:
                            output_path = self._get_output_path(etf_code, default_threshold, param_folder)
                            os.makedirs(os.path.dirname(output_path), exist_ok=True)
                            cached_df.to_csv(output_path, index=False, encoding='utf-8')
                            
                            if verbose:
                                print(f"💾 结果已保存到: {output_path}")
                        
                        return {
                            'success': True,
                            'etf_code': etf_code,
                            'data_points': len(cached_df),
                            'result_df': cached_df,
                            'output_path': output_path if save_result else None,
                            'from_cache': True
                        }
            
            # 读取ETF数据
            df = self.data_reader.read_etf_data(etf_code)
            if df is None:
                return {'success': False, 'error': 'Failed to read data'}
            
            # 验证数据
            if not self.macd_engine.validate_calculation_requirements(df):
                return {'success': False, 'error': 'Data validation failed'}
            
            # 计算MACD指标
            result_df = self.macd_engine.calculate_macd_for_etf(df)
            if result_df.empty:
                return {'success': False, 'error': 'MACD calculation failed'}
            
            if verbose:
                print(f"✅ {etf_code} MACD计算完成，共{len(result_df)}个数据点")
            
            # 保存到缓存
            if self.cache_manager and self.enable_cache:
                self.cache_manager.save_etf_cache(etf_code, result_df, default_threshold, param_folder)
            
            # 保存结果到输出目录
            if save_result:
                output_path = self._get_output_path(etf_code, default_threshold, param_folder)
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
                result_df.to_csv(output_path, index=False, encoding='utf-8')
                
                if verbose:
                    print(f"💾 结果已保存到: {output_path}")
            
            return {
                'success': True,
                'etf_code': etf_code,
                'data_points': len(result_df),
                'result_df': result_df,
                'output_path': output_path if save_result else None,
                'from_cache': False
            }
            
        except Exception as e:
            error_msg = f"计算ETF {etf_code} MACD失败: {str(e)}"
            if verbose:
                print(f"❌ {error_msg}")
            return {'success': False, 'error': error_msg}
    
    def quick_analysis(self, etf_code: str, include_historical: bool = False) -> Optional[Dict]:
        """
        快速分析单个ETF的MACD指标
        
        Args:
            etf_code: ETF代码
            include_historical: 是否包含历史数据
            
        Returns:
            分析结果字典
        """
        try:
            print(f"⚡ 开始快速分析 {etf_code} 的MACD指标...")
            
            # 计算MACD
            result = self.calculate_single_etf(etf_code, save_result=False, verbose=True)
            if not result.get('success', False):
                return None
            
            df = result['result_df']
            latest = df.iloc[0]  # 获取第一行（最新数据，因为数据按日期倒序排列）
            
            # 基础分析
            analysis = {
                'etf_code': etf_code,
                'calculation_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_points': len(df),
                'latest_values': {
                    'date': latest['date'] if pd.notna(latest['date']) else 'N/A',
                    'DIF': float(latest['dif']) if pd.notna(latest['dif']) else None,
                    'DEA': float(latest['dea']) if pd.notna(latest['dea']) else None,
                    'MACD': float(latest['macd_bar']) if pd.notna(latest['macd_bar']) else None,
                    'EMA_FAST': float(latest['ema_fast']) if pd.notna(latest['ema_fast']) else None,
                    'EMA_SLOW': float(latest['ema_slow']) if pd.notna(latest['ema_slow']) else None
                },
                'statistics': {
                    'DIF_mean': float(df['dif'].mean()),
                    'DEA_mean': float(df['dea'].mean()),
                    'MACD_mean': float(df['macd_bar'].mean()),
                    'DIF_std': float(df['dif'].std()),
                    'DEA_std': float(df['dea'].std()),
                    'MACD_std': float(df['macd_bar'].std())
                }
            }
            
            if include_historical:
                analysis['historical_data'] = df.tail(10).to_dict('records')
            
            print(f"✅ {etf_code} 快速分析完成")
            print(f"📊 最新DIF: {analysis['latest_values']['DIF']:.6f}")
            print(f"📊 最新DEA: {analysis['latest_values']['DEA']:.6f}")
            print(f"📊 最新MACD: {analysis['latest_values']['MACD']:.6f}")
            
            return analysis
            
        except Exception as e:
            print(f"❌ 快速分析失败 {etf_code}: {str(e)}")
            return None
    
    def show_system_status(self):
        """显示系统状态"""
        print("\n🔧 MACD系统状态")
        print("=" * 40)
        
        system_info = self.config.get_system_info()
        print(f"📊 版本: {system_info['version']}")
        print(f"🔧 配置: {system_info['system_name']} - {system_info['adj_type']}")
        print(f"📁 数据路径: {self.config.get_data_source_path()}")
        print(f"📂 输出路径: {self.output_dir}")
        
        # 检查数据状态
        print(f"\n📁 数据状态:")
        available_etfs = self.data_reader.get_available_etfs()
        print(f"   可用ETF: {len(available_etfs)} 个")
        
        if os.path.exists(self.config.get_data_source_path()):
            print(f"   数据路径: ✅ 有效")
            if available_etfs:
                sample_etfs = available_etfs[:5]
                print(f"   样本ETF: {', '.join(sample_etfs)}")
        else:
            print(f"   数据路径: ❌ 无效")
        
        # 组件状态
        print(f"\n🔧 组件状态:")
        print(f"   Data Reader: ✅ Ready")
        print(f"   MACD Engine: ✅ Ready")
        
        # 计算引擎信息
        engine_info = self.macd_engine.get_calculation_info()
        print(f"\n📊 MACD引擎信息:")
        print(f"   参数: EMA({engine_info['parameters']['fast_period']}, "
              f"{engine_info['parameters']['slow_period']}, "
              f"{engine_info['parameters']['signal_period']})")
        print(f"   输出指标: {', '.join(engine_info['output_indicators'])}")
        print(f"   最小数据点: {engine_info['min_data_points']}")
    
    def get_available_etfs(self, threshold: str = "3000万门槛") -> List[str]:
        """获取可用ETF列表"""
        return self.data_reader.get_available_etfs(threshold)
    
    def validate_macd_calculation(self, etf_code: str) -> bool:
        """验证MACD计算正确性"""
        try:
            print(f"🔍 验证 {etf_code} MACD计算...")
            
            result = self.calculate_single_etf(etf_code, save_result=False, verbose=True)
            if not result.get('success', False):
                print(f"❌ 计算失败")
                return False
            
            df = result['result_df']
            
            # 验证数据完整性
            required_columns = ['DIF', 'DEA', 'MACD', 'EMA_FAST', 'EMA_SLOW']
            for col in required_columns:
                if col not in df.columns:
                    print(f"❌ 缺少列: {col}")
                    return False
                
                if df[col].isna().all():
                    print(f"❌ 列 {col} 全为空值")
                    return False
            
            # 验证MACD计算逻辑
            last_row = df.iloc[-1]
            expected_dif = last_row['EMA_FAST'] - last_row['EMA_SLOW']
            actual_dif = last_row['DIF']
            
            if abs(expected_dif - actual_dif) > 1e-10:
                print(f"❌ DIF计算错误: 期望{expected_dif}, 实际{actual_dif}")
                return False
            
            print(f"✅ {etf_code} MACD计算验证通过")
            return True
            
        except Exception as e:
            print(f"❌ 验证失败: {str(e)}")
            return False
    
    def calculate_historical_batch(self, etf_codes: Optional[List[str]] = None, 
                                 thresholds: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        计算并保存完整历史MACD数据 - 使用batch_processor确保缓存集成
        
        Args:
            etf_codes: ETF代码列表，None则处理所有可用ETF
            thresholds: 门槛列表，默认["3000万门槛", "5000万门槛"]
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        thresholds = thresholds or ["3000万门槛", "5000万门槛"]
        
        print(f"🚀 开始历史MACD数据计算和保存...")
        print(f"📊 门槛设置: {thresholds}")
        
        # 获取ETF列表
        if etf_codes is None:
            etf_codes = self.data_reader.get_available_etfs()
        
        print(f"📈 待处理ETF数量: {len(etf_codes)}")
        
        # 获取ETF文件路径字典
        etf_files_dict = {}
        for etf_code in etf_codes:
            file_path = self.data_reader.get_etf_file_path(etf_code)
            if file_path and os.path.exists(file_path):
                etf_files_dict[etf_code] = file_path
        
        print(f"📁 有效ETF文件数量: {len(etf_files_dict)}")
        
        all_stats = {}
        
        # 为每个门槛计算历史数据
        for threshold in thresholds:
            print(f"\n📈 计算门槛: {threshold}")
            
            # 使用历史计算器的批量计算方法
            from ..engines.historical_calculator import MACDHistoricalCalculator
            historical_calculator = MACDHistoricalCalculator(self.config)
            
            # 批量计算历史MACD
            results = historical_calculator.batch_calculate_historical_macd(
                etf_files_dict, list(etf_files_dict.keys())
            )
            
            if results:
                # 保存历史数据文件
                save_stats = historical_calculator.save_historical_results(
                    results, self.output_dir, threshold
                )
                all_stats[threshold] = save_stats
                
                print(f"✅ {threshold}: 历史数据计算和保存完成")
            else:
                print(f"❌ {threshold}: 历史数据计算失败")
                all_stats[threshold] = {}
        
        return {
            'processing_statistics': all_stats,
            'total_etfs_processed': len(etf_files_dict),
            'thresholds_processed': thresholds
        }
    
    def _get_output_path(self, etf_code: str, threshold: str, parameter_folder: str) -> str:
        """
        根据参数设置获取正确的输出路径
        
        Args:
            etf_code: ETF代码
            threshold: 门槛类型
            parameter_folder: 参数文件夹名称
            
        Returns:
            完整的输出路径
        """
        # 清理ETF代码
        clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
        
        # 构建路径：base_dir/threshold/parameter_folder/etf_code.csv
        output_path = os.path.join(
            self.output_dir,
            threshold,
            parameter_folder,
            f"{clean_code}.csv"
        )
        
        return output_path