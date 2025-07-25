"""
RSI指标优化数据读取器
基于威廉指标的优化数据读取架构

优化内容：
1. 高效的ETF数据文件检索和读取
2. 智能的数据类型转换和验证
3. 内存优化的数据处理流程
4. 支持多种复权类型的数据读取
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import traceback


class RSIDataReaderOptimized:
    """RSI指标优化数据读取器"""

    def __init__(self, config):
        """
        初始化RSI数据读取器
        
        Args:
            config: RSI配置对象
        """
        self.config = config
        self.data_source_path = config.data_source_path
        
        # 验证数据源路径
        if not os.path.exists(self.data_source_path):
            raise FileNotFoundError(f"数据源路径不存在: {self.data_source_path}")
        
        # 性能统计
        self.stats = {
            'files_read': 0,
            'data_validation_errors': 0,
            'read_time_ms': 0
        }
        
        print(f"✅ RSI优化数据读取器初始化完成")
        print(f"📁 数据源路径: {self.data_source_path}")

    def read_etf_data(self, etf_code):
        """
        读取指定ETF的OHLCV数据
        
        Args:
            etf_code: ETF代码
            
        Returns:
            DataFrame: ETF数据，包含RSI计算所需的涨跌幅字段
        """
        try:
            start_time = datetime.now()
            
            # 清理ETF代码
            clean_code = self._clean_etf_code(etf_code)
            
            # 查找数据文件
            file_path = self._find_etf_data_file(clean_code)
            if not file_path:
                print(f"⚠️ 未找到ETF数据文件: {etf_code}")
                return None
            
            # 读取CSV数据
            try:
                df = pd.read_csv(file_path, encoding='utf-8')
            except UnicodeDecodeError:
                # 备用编码
                df = pd.read_csv(file_path, encoding='gbk')
            
            if df.empty:
                print(f"⚠️ ETF数据文件为空: {etf_code}")
                return None
            
            # 数据验证和清洗
            df = self._validate_and_clean_data(df, etf_code)
            if df is None:
                return None
            
            # 添加RSI计算必需的涨跌幅字段
            df = self._add_price_change_fields(df, etf_code)
            
            # 性能统计
            read_time = (datetime.now() - start_time).total_seconds() * 1000
            self.stats['files_read'] += 1
            self.stats['read_time_ms'] += read_time
            
            print(f"📈 成功读取ETF数据: {etf_code} ({len(df)}行, {read_time:.2f}ms)")
            
            return df
            
        except Exception as e:
            print(f"❌ 读取ETF数据失败: {etf_code} - {str(e)}")
            print(f"🔍 异常详情: {traceback.format_exc()}")
            return None

    def _clean_etf_code(self, etf_code):
        """清理ETF代码格式"""
        if not etf_code:
            return ""
        
        # 去除空格和特殊字符
        clean_code = str(etf_code).strip()
        
        # 确保是6位数字格式
        if clean_code.isdigit() and len(clean_code) == 6:
            return clean_code
        
        # 尝试提取6位数字
        import re
        match = re.search(r'\d{6}', clean_code)
        if match:
            return match.group()
        
        return clean_code

    def _find_etf_data_file(self, etf_code):
        """
        查找ETF数据文件
        支持多种文件名格式
        """
        if not etf_code:
            return None
        
        # 可能的文件名格式
        possible_filenames = [
            f"{etf_code}.csv",
            f"{etf_code}.CSV"
        ]
        
        for filename in possible_filenames:
            file_path = os.path.join(self.data_source_path, filename)
            if os.path.exists(file_path):
                return file_path
        
        return None

    def _validate_and_clean_data(self, df, etf_code):
        """
        验证和清洗数据
        确保数据质量满足RSI计算要求
        """
        try:
            # 检查必需列 - 支持多种涨跌幅字段名
            required_columns = ['日期', '收盘价']
            change_columns = ['涨跌幅', '涨幅%', '涨跌']  # 可能的涨跌幅字段名
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            # 检查是否有任何一个涨跌幅字段
            has_change_column = any(col in df.columns for col in change_columns)
            
            if missing_columns:
                print(f"❌ ETF数据缺少必需列: {etf_code} - {missing_columns}")
                self.stats['data_validation_errors'] += 1
                return None
            
            if not has_change_column:
                print(f"❌ ETF数据缺少涨跌幅相关列: {etf_code} - 需要以下任一列: {change_columns}")
                self.stats['data_validation_errors'] += 1
                return None
            
            # 备份原始数据
            original_length = len(df)
            
            # 日期字段处理
            if '日期' in df.columns:
                try:
                    # 尝试多种日期格式转换
                    if df['日期'].dtype == 'object':
                        # 如果是字符串类型，尝试转换
                        df['日期'] = pd.to_datetime(df['日期'], format='%Y%m%d', errors='coerce')
                    elif df['日期'].dtype in ['int64', 'float64']:
                        # 如果是数值类型，先转换为字符串再转换为日期
                        df['日期'] = pd.to_datetime(df['日期'].astype(str), format='%Y%m%d', errors='coerce')
                    else:
                        df['日期'] = pd.to_datetime(df['日期'], errors='coerce')
                    
                    # 去除日期无效的行
                    df = df.dropna(subset=['日期'])
                    print(f"📅 日期转换成功: {etf_code} - 日期范围 {df['日期'].min()} 到 {df['日期'].max()}")
                except Exception as e:
                    print(f"⚠️ 日期转换失败: {etf_code} - {str(e)}")
                    return None
            
            # 数值字段处理
            numeric_columns = ['收盘价']
            
            # 找到实际的涨跌幅字段
            change_column = None
            for col in ['涨跌幅', '涨幅%', '涨跌']:
                if col in df.columns:
                    change_column = col
                    numeric_columns.append(col)
                    break
            
            for col in numeric_columns:
                if col in df.columns:
                    # 转换为数值类型
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 去除关键数值字段的空值
            dropna_columns = ['收盘价']
            if change_column:
                dropna_columns.append(change_column)
            df = df.dropna(subset=dropna_columns)
            
            # 检查数据量是否足够
            min_data_points = self.config.DATA_QUALITY_REQUIREMENTS['min_data_points']
            if len(df) < min_data_points:
                print(f"⚠️ ETF数据量不足: {etf_code} - 需要{min_data_points}行，实际{len(df)}行")
                return None
            
            # 按日期排序
            df = df.sort_values('日期').reset_index(drop=True)
            
            # 数据质量统计
            cleaned_rows = original_length - len(df)
            if cleaned_rows > 0:
                print(f"🧹 数据清洗: {etf_code} - 清洗了{cleaned_rows}行数据")
            
            return df
            
        except Exception as e:
            print(f"❌ 数据验证失败: {etf_code} - {str(e)}")
            self.stats['data_validation_errors'] += 1
            return None

    def _add_price_change_fields(self, df, etf_code):
        """
        添加RSI计算必需的价格变化字段
        
        Args:
            df: 清洗后的数据框
            etf_code: ETF代码
            
        Returns:
            DataFrame: 包含价格变化字段的数据
        """
        try:
            # 查找可用的涨跌幅字段
            change_column = None
            for col in ['涨跌幅', '涨幅%', '涨跌']:
                if col in df.columns:
                    change_column = col
                    break
            
            if change_column:
                # 使用涨跌幅字段
                if change_column == '涨幅%':
                    # 涨幅%已经是百分比形式，直接除以100转换为小数
                    df['price_change_pct'] = df[change_column] / 100.0
                else:
                    # 其他字段可能需要不同处理
                    df['price_change_pct'] = df[change_column] / 100.0
            else:
                # 如果没有涨跌幅，通过收盘价计算
                if '收盘价' in df.columns:
                    df['price_change_pct'] = df['收盘价'].pct_change()
                else:
                    print(f"❌ 无法计算价格变化: {etf_code} - 缺少涨跌幅或收盘价字段")
                    return None
            
            # 去除第一行（无法计算变化率）
            df = df.dropna(subset=['price_change_pct']).reset_index(drop=True)
            
            # 验证价格变化数据的合理性
            price_changes = df['price_change_pct']
            
            # 检查异常值（日涨跌幅超过50%的情况）
            extreme_changes = abs(price_changes) > 0.5
            if extreme_changes.any():
                extreme_count = extreme_changes.sum()
                print(f"⚠️ 发现{extreme_count}个极端涨跌幅数据: {etf_code}")
                
                # 可选：标记但不删除极端值，让RSI计算引擎处理
                df['has_extreme_change'] = extreme_changes
            
            return df
            
        except Exception as e:
            print(f"❌ 添加价格变化字段失败: {etf_code} - {str(e)}")
            return None

    def get_etf_file_list(self, threshold="3000万门槛"):
        """
        获取通过初筛的ETF列表(修复版)
        
        Args:
            threshold: 门槛类型 ("3000万门槛" 或 "5000万门槛")
        
        Returns:
            list: 通过初筛的ETF代码列表
        """
        try:
            etf_codes = []
            
            # 构建初筛结果文件路径
            filter_file_path = os.path.join(
                self.config.project_root, 
                "ETF_初筛", 
                "data", 
                threshold, 
                "通过筛选ETF.txt"
            )
            
            if not os.path.exists(filter_file_path):
                print(f"❌ 初筛结果文件不存在: {filter_file_path}")
                # 降级到原有逻辑
                return self._get_etf_file_list_fallback()
            
            # 读取初筛结果文件
            with open(filter_file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            # 解析ETF代码
            for line in lines:
                line = line.strip()
                # 跳过注释行和空行
                if line and not line.startswith('#'):
                    etf_code = line
                    if etf_code.isdigit() and len(etf_code) == 6:
                        etf_codes.append(etf_code)
            
            etf_codes.sort()  # 排序
            print(f"📊 读取{threshold}初筛结果: {len(etf_codes)}个ETF")
            
            return etf_codes
            
        except Exception as e:
            print(f"❌ 读取初筛结果失败: {str(e)}")
            print("🔄 降级到原有获取逻辑")
            return self._get_etf_file_list_fallback()

    def _get_etf_file_list_fallback(self):
        """
        降级获取逻辑：扫描数据源目录(原有逻辑)
        
        Returns:
            list: ETF代码列表
        """
        try:
            etf_codes = []
            
            if not os.path.exists(self.data_source_path):
                print(f"❌ 数据源路径不存在: {self.data_source_path}")
                return etf_codes
            
            # 遍历数据源目录
            for filename in os.listdir(self.data_source_path):
                if filename.endswith('.csv') or filename.endswith('.CSV'):
                    # 提取ETF代码
                    etf_code = filename.replace('.csv', '').replace('.CSV', '')
                    if etf_code.isdigit() and len(etf_code) == 6:
                        etf_codes.append(etf_code)
            
            etf_codes.sort()  # 排序
            print(f"📊 降级模式: 发现{len(etf_codes)}个ETF数据文件")
            
            return etf_codes
            
        except Exception as e:
            print(f"❌ 降级获取ETF文件列表失败: {str(e)}")
            return []

    def validate_data_source(self):
        """
        验证数据源完整性
        
        Returns:
            dict: 验证结果
        """
        try:
            result = {
                'is_valid': True,
                'total_files': 0,
                'valid_files': 0,
                'invalid_files': 0,
                'error_details': []
            }
            
            if not os.path.exists(self.data_source_path):
                result['is_valid'] = False
                result['error_details'].append(f"数据源路径不存在: {self.data_source_path}")
                return result
            
            # 获取所有文件
            all_files = [f for f in os.listdir(self.data_source_path) 
                        if f.endswith('.csv') or f.endswith('.CSV')]
            result['total_files'] = len(all_files)
            
            # 验证文件格式
            for filename in all_files[:10]:  # 只验证前10个文件以节省时间
                file_path = os.path.join(self.data_source_path, filename)
                try:
                    df = pd.read_csv(file_path, nrows=5)  # 只读取前5行
                    required_columns = ['日期', '收盘价']
                    if all(col in df.columns for col in required_columns):
                        result['valid_files'] += 1
                    else:
                        result['invalid_files'] += 1
                        result['error_details'].append(f"文件缺少必需列: {filename}")
                except Exception as e:
                    result['invalid_files'] += 1
                    result['error_details'].append(f"文件读取失败: {filename} - {str(e)}")
            
            # 如果抽样验证的文件都有问题，标记为无效
            if result['valid_files'] == 0 and result['invalid_files'] > 0:
                result['is_valid'] = False
            
            return result
            
        except Exception as e:
            return {
                'is_valid': False,
                'total_files': 0,
                'valid_files': 0,
                'invalid_files': 0,
                'error_details': [f"数据源验证失败: {str(e)}"]
            }

    def get_performance_stats(self):
        """获取性能统计信息"""
        if self.stats['files_read'] > 0:
            avg_read_time = self.stats['read_time_ms'] / self.stats['files_read']
        else:
            avg_read_time = 0
        
        return {
            'files_read': self.stats['files_read'],
            'average_read_time_ms': round(avg_read_time, 2),
            'total_read_time_ms': round(self.stats['read_time_ms'], 2),
            'data_validation_errors': self.stats['data_validation_errors'],
            'success_rate': (self.stats['files_read'] / 
                           max(1, self.stats['files_read'] + self.stats['data_validation_errors']))
        }

    def print_performance_summary(self):
        """打印性能摘要"""
        stats = self.get_performance_stats()
        
        print(f"\n{'=' * 60}")
        print("📊 RSI数据读取器性能摘要")
        print(f"{'=' * 60}")
        print(f"📁 已读取文件数: {stats['files_read']}")
        print(f"⏱️ 平均读取时间: {stats['average_read_time_ms']:.2f}ms")
        print(f"⚡ 总读取时间: {stats['total_read_time_ms']:.2f}ms")
        print(f"❌ 验证错误数: {stats['data_validation_errors']}")
        print(f"✅ 成功率: {stats['success_rate']:.2%}")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    # 数据读取器测试
    try:
        from config import RSIConfig
        
        print("🧪 RSI数据读取器测试")
        config = RSIConfig()
        reader = RSIDataReaderOptimized(config)
        
        # 验证数据源
        validation = reader.validate_data_source()
        print(f"数据源验证: {'✅' if validation['is_valid'] else '❌'}")
        
        # 获取ETF列表
        etf_list = reader.get_etf_file_list()
        if etf_list:
            print(f"发现{len(etf_list)}个ETF文件")
            
            # 测试读取第一个ETF
            test_etf = etf_list[0]
            test_data = reader.read_etf_data(test_etf)
            if test_data is not None:
                print(f"测试读取成功: {test_etf} ({len(test_data)}行数据)")
            
        # 打印性能摘要
        reader.print_performance_summary()
        
        print("✅ RSI数据读取器测试完成")
        
    except Exception as e:
        print(f"❌ RSI数据读取器测试失败: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")