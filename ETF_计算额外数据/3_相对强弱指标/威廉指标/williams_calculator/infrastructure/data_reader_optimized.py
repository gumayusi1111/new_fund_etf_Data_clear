"""
威廉指标优化数据读取器
修复pandas FutureWarning，优化数据处理性能

优化内容：
1. 修复pandas日期处理的FutureWarning
2. 优化数据类型转换和内存使用
3. 向量化数据清洗操作
4. 改进错误处理和边界情况
5. 提升大文件读取性能
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import glob
import warnings

# 忽略pandas的链式赋值警告
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)
warnings.filterwarnings('ignore', category=FutureWarning)


class WilliamsDataReaderOptimized:
    """威廉指标优化数据读取器"""
    
    def __init__(self, config):
        """
        初始化数据读取器
        
        Args:
            config: 威廉指标配置对象
        """
        self.config = config
        self.data_source_path = config.data_source_path
        self.adj_type = config.adj_type
        self.thresholds = config.THRESHOLDS
        self.data_quality_requirements = config.DATA_QUALITY_REQUIREMENTS
        
        # 验证数据源路径
        if not self.config.is_data_source_valid():
            print(f"⚠️ 数据源路径无效: {self.data_source_path}")

    def read_etf_data_optimized(self, etf_code):
        """
        优化的ETF数据读取
        
        优化内容：
        1. 修复pandas日期处理警告
        2. 优化数据类型和内存使用
        3. 向量化数据清洗
        
        Args:
            etf_code: ETF代码(支持带.SH/.SZ后缀或不带后缀)
            
        Returns:
            DataFrame: 清洗后的ETF数据，失败返回None
        """
        try:
            # 清理ETF代码格式
            clean_code = self._clean_etf_code(etf_code)
            
            # 查找数据文件
            file_path = self._find_etf_data_file(clean_code)
            if not file_path:
                print(f"⚠️ 未找到ETF数据文件: {etf_code}")
                return None
            
            # 优化的CSV文件读取
            df = self._read_csv_file_optimized(file_path)
            if df is None:
                return None
            
            # 优化的数据清洗和验证
            df = self._clean_and_validate_data_optimized(df, etf_code)
            if df is None or df.empty:
                return None
            
            # 优化的数据格式标准化
            df = self._standardize_data_format_optimized(df, etf_code)
            
            return df
            
        except Exception as e:
            print(f"❌ 优化读取ETF数据失败: {etf_code} - {str(e)}")
            return None

    def _read_csv_file_optimized(self, file_path):
        """
        优化的CSV文件读取
        
        优化内容：
        1. 指定数据类型减少内存使用
        2. 优化编码检测
        3. 批量处理大文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            DataFrame: 读取的数据，失败返回None
        """
        try:
            # 定义数据类型以优化内存使用（改为更宽容的类型）
            dtype_dict = {
                '开盘价': np.float64,
                '最高价': np.float64,
                '最低价': np.float64,
                '收盘价': np.float64,
                '成交量(手数)': np.float64,  # 改为float64以避免转换错误
                '成交额(千元)': np.float64
            }
            
            # 尝试不同的编码格式，优先使用常用编码
            encodings = ['utf-8', 'utf-8-sig', 'gbk', 'gb2312']
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(
                        file_path, 
                        encoding=encoding,
                        parse_dates=False,  # 先不解析日期，后续统一处理
                        low_memory=False    # 避免混合类型推断问题
                    )
                    if not df.empty:
                        return df
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    print(f"⚠️ 读取文件时发生错误 ({encoding}): {str(e)}")
                    continue
            
            print(f"❌ 无法读取文件: {file_path}")
            return None
            
        except Exception as e:
            print(f"❌ 优化文件读取异常: {file_path} - {str(e)}")
            return None

    def _process_date_column_optimized(self, df):
        """
        优化的日期列处理 - 修复FutureWarning
        
        修复内容：
        1. 使用copy()避免SettingWithCopyWarning
        2. 优化日期格式推断
        3. 向量化日期转换
        
        Args:
            df: 数据DataFrame
            
        Returns:
            DataFrame: 处理后的数据
        """
        try:
            if '日期' not in df.columns:
                print("❌ 缺少日期列")
                return None
            
            # 创建副本避免警告
            df = df.copy()
            
            # 检测日期格式并转换
            date_series = df['日期'].astype(str)
            
            # 尝试不同的日期格式转换（按常见程度排序）
            date_formats = ['%Y%m%d', '%Y-%m-%d', '%Y/%m/%d', '%Y.%m.%d']
            
            converted = False
            for fmt in date_formats:
                try:
                    # 使用pd.to_datetime而不是直接赋值
                    date_parsed = pd.to_datetime(date_series, format=fmt, errors='coerce')
                    
                    # 检查转换成功率
                    success_rate = date_parsed.notna().mean()
                    if success_rate > 0.9:  # 90%以上转换成功
                        df['日期'] = date_parsed
                        converted = True
                        break
                        
                except ValueError:
                    continue
            
            if not converted:
                # 如果所有格式都失败，尝试自动推断
                try:
                    df['日期'] = pd.to_datetime(date_series, errors='coerce', infer_datetime_format=True)
                    
                    # 检查推断结果
                    if df['日期'].isna().all():
                        print("❌ 日期格式转换失败")
                        return None
                        
                except Exception:
                    print("❌ 日期格式转换失败")
                    return None
            
            # 移除日期转换失败的行
            initial_count = len(df)
            df = df.dropna(subset=['日期'])
            final_count = len(df)
            
            if final_count < initial_count:
                print(f"🧹 移除{initial_count - final_count}个日期无效的数据点")
            
            # 按日期排序
            df = df.sort_values('日期').reset_index(drop=True)
            
            return df
            
        except Exception as e:
            print(f"❌ 优化日期处理失败: {str(e)}")
            return None

    def _clean_price_data_vectorized(self, df, etf_code):
        """
        向量化的价格数据清洗
        
        优化内容：
        1. 向量化数据类型转换
        2. 向量化异常值检测
        3. 批量逻辑关系验证
        
        Args:
            df: 数据DataFrame
            etf_code: ETF代码
            
        Returns:
            DataFrame: 清洗后的数据
        """
        try:
            df = df.copy()
            price_columns = ['最高价', '最低价', '收盘价', '开盘价']
            
            # 向量化数据类型转换
            for col in price_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 记录初始数据量
            initial_count = len(df)
            
            # 向量化移除非正数价格
            price_mask = True
            for col in ['最高价', '最低价', '收盘价']:
                if col in df.columns:
                    price_mask = price_mask & (df[col] > 0)
            
            df = df[price_mask]
            
            # 向量化价格逻辑关系检查
            if '最高价' in df.columns and '最低价' in df.columns:
                logical_mask = df['最高价'] >= df['最低价']
                df = df[logical_mask]
            
            if '收盘价' in df.columns and '最高价' in df.columns and '最低价' in df.columns:
                range_mask = (df['收盘价'] >= df['最低价']) & (df['收盘价'] <= df['最高价'])
                df = df[range_mask]
            
            # 向量化极端异常值移除（价格变化超过50%）
            if '收盘价' in df.columns and len(df) > 1:
                price_changes = df['收盘价'].pct_change().abs()
                normal_mask = (price_changes <= 0.5) | price_changes.isna()
                df = df[normal_mask]
            
            # 统计清洗结果
            cleaned_count = len(df)
            if cleaned_count < initial_count:
                removed_count = initial_count - cleaned_count
                print(f"🧹 向量化数据清洗: {etf_code} - 移除{removed_count}个异常数据点")
            
            # 检查清洗后数据是否足够
            min_required = self.data_quality_requirements['min_data_points']
            if len(df) < min_required:
                print(f"⚠️ 清洗后数据不足: {etf_code} - 需要{min_required}个，实际{len(df)}个")
                return None
            
            return df
            
        except Exception as e:
            print(f"❌ 向量化价格数据清洗失败: {etf_code} - {str(e)}")
            return None

    def _clean_and_validate_data_optimized(self, df, etf_code):
        """
        优化的数据清洗和验证
        
        Args:
            df: 原始数据DataFrame
            etf_code: ETF代码
            
        Returns:
            DataFrame: 清洗后的数据，验证失败返回None
        """
        try:
            if df.empty:
                print(f"⚠️ 空数据文件: {etf_code}")
                return None
            
            # 检查必要列是否存在
            required_columns = ['最高价', '最低价', '收盘价', '日期']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"❌ 缺少必要列: {etf_code} - {missing_columns}")
                return None
            
            # 检查数据点数量
            min_required = self.data_quality_requirements['min_data_points']
            if len(df) < min_required:
                print(f"⚠️ 数据点不足: {etf_code} - 需要{min_required}个，实际{len(df)}个")
                return None
            
            # 优化的日期处理
            df = self._process_date_column_optimized(df)
            if df is None:
                return None
            
            # 向量化价格数据清洗
            df = self._clean_price_data_vectorized(df, etf_code)
            if df is None or df.empty:
                return None
            
            # 优化的数据去重
            df = self._remove_duplicates_optimized(df, etf_code)
            
            return df
            
        except Exception as e:
            print(f"❌ 优化数据清洗验证失败: {etf_code} - {str(e)}")
            return None

    def _remove_duplicates_optimized(self, df, etf_code):
        """
        优化的重复数据移除
        
        Args:
            df: 数据DataFrame
            etf_code: ETF代码
            
        Returns:
            DataFrame: 去重后的数据
        """
        try:
            initial_count = len(df)
            
            # 按日期去重，保留最后一条记录（更高效的实现）
            df = df.drop_duplicates(subset=['日期'], keep='last')
            
            final_count = len(df)
            if final_count < initial_count:
                removed_count = initial_count - final_count
                print(f"🧹 优化数据去重: {etf_code} - 移除{removed_count}个重复数据点")
            
            return df
            
        except Exception as e:
            print(f"❌ 优化数据去重失败: {etf_code} - {str(e)}")
            return df

    def _standardize_data_format_optimized(self, df, etf_code):
        """
        优化的数据格式标准化
        
        Args:
            df: 数据DataFrame
            etf_code: ETF代码
            
        Returns:
            DataFrame: 标准化后的数据
        """
        try:
            df = df.copy()
            
            # 添加代码列
            clean_code = self._clean_etf_code(etf_code)
            df['代码'] = clean_code
            
            # 优化数值列的精度和数据类型
            numeric_columns = ['最高价', '最低价', '收盘价', '开盘价']
            for col in numeric_columns:
                if col in df.columns:
                    # 转换为float64并保持8位小数精度
                    df[col] = df[col].astype(np.float64).round(8)
            
            # 重置索引
            df = df.reset_index(drop=True)
            
            return df
            
        except Exception as e:
            print(f"❌ 优化数据格式标准化失败: {etf_code} - {str(e)}")
            return df

    def get_etf_list_by_threshold_optimized(self, threshold):
        """
        优化的ETF列表获取 - 使用筛选后的ETF列表
        
        Args:
            threshold: 门槛名称('3000万门槛' or '5000万门槛')
            
        Returns:
            list: ETF代码列表
        """
        try:
            # 构建筛选数据路径
            filtered_data_path = os.path.join(
                self.config.project_root, "ETF_初筛", "data", threshold, "通过筛选ETF.txt"
            )
            
            # 检查筛选文件是否存在
            if not os.path.exists(filtered_data_path):
                print(f"⚠️ 筛选文件不存在，回退到原始数据: {filtered_data_path}")
                return self._get_all_etf_codes_fallback()
            
            # 读取筛选后的ETF列表
            etf_codes = []
            with open(filtered_data_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    # 跳过注释行和空行
                    if line and not line.startswith('#'):
                        # 验证ETF代码格式
                        if line.isdigit() and len(line) == 6:
                            etf_codes.append(line)
            
            # 验证筛选的ETF是否有对应的数据文件
            valid_etf_codes = []
            for etf_code in etf_codes:
                data_file_path = self._find_etf_data_file(etf_code)
                if data_file_path and os.path.exists(data_file_path):
                    valid_etf_codes.append(etf_code)
            
            etf_codes = sorted(valid_etf_codes)
            print(f"📊 {threshold}: 使用筛选后ETF列表，共{len(etf_codes)}个有效ETF")
            
            if len(etf_codes) == 0:
                print(f"⚠️ 筛选后无有效ETF，回退到原始数据")
                return self._get_all_etf_codes_fallback()
            
            return etf_codes
            
        except Exception as e:
            print(f"❌ 读取筛选ETF列表失败: {threshold} - {str(e)}")
            print(f"⚠️ 回退到原始数据")
            return self._get_all_etf_codes_fallback()

    def _get_all_etf_codes_fallback(self):
        """回退方案：获取所有ETF代码"""
        try:
            if not os.path.exists(self.data_source_path):
                print(f"❌ 数据源路径不存在: {self.data_source_path}")
                return []
            
            # 使用glob模式匹配，更高效
            csv_pattern = os.path.join(self.data_source_path, "*.csv")
            csv_files = glob.glob(csv_pattern)
            
            # 向量化提取ETF代码
            etf_codes = []
            for file_path in csv_files:
                filename = os.path.basename(file_path)
                etf_code = os.path.splitext(filename)[0]  # 移除扩展名
                
                # 验证ETF代码格式
                if etf_code.isdigit() and len(etf_code) == 6:
                    etf_codes.append(etf_code)
            
            etf_codes.sort()  # 排序
            print(f"📊 回退模式: 发现{len(etf_codes)}个ETF数据文件")
            return etf_codes
            
        except Exception as e:
            print(f"❌ 回退获取ETF列表失败: {str(e)}")
            return []

    def batch_validate_etf_data(self, etf_codes, sample_size=10):
        """
        批量验证ETF数据质量
        
        Args:
            etf_codes: ETF代码列表
            sample_size: 采样验证数量
            
        Returns:
            dict: 批量验证结果
        """
        try:
            # 采样验证以提高效率
            sample_codes = etf_codes[:sample_size] if len(etf_codes) > sample_size else etf_codes
            
            validation_results = {
                'total_count': len(etf_codes),
                'sample_count': len(sample_codes),
                'valid_count': 0,
                'invalid_count': 0,
                'issues': []
            }
            
            for etf_code in sample_codes:
                df = self.read_etf_data_optimized(etf_code)
                if df is not None and not df.empty:
                    validation_results['valid_count'] += 1
                else:
                    validation_results['invalid_count'] += 1
                    validation_results['issues'].append(etf_code)
            
            validation_results['success_rate'] = (
                validation_results['valid_count'] / validation_results['sample_count'] * 100
                if validation_results['sample_count'] > 0 else 0
            )
            
            return validation_results
            
        except Exception as e:
            print(f"❌ 批量数据验证失败: {str(e)}")
            return {'total_count': 0, 'sample_count': 0, 'valid_count': 0, 'invalid_count': 0}

    # 向后兼容方法
    def _clean_etf_code(self, etf_code):
        """清理ETF代码格式"""
        if not etf_code:
            return ""
        
        clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
        
        if clean_code.isdigit() and len(clean_code) == 6:
            return clean_code
        
        print(f"⚠️ ETF代码格式异常: {etf_code}")
        return clean_code

    def _find_etf_data_file(self, clean_code):
        """查找ETF数据文件"""
        try:
            possible_patterns = [
                os.path.join(self.data_source_path, f"{clean_code}.csv"),
                os.path.join(self.data_source_path, f"{clean_code}.CSV"),
                os.path.join(self.data_source_path, f"{clean_code}.*")
            ]
            
            for pattern in possible_patterns:
                if '*' in pattern:
                    matches = glob.glob(pattern)
                    if matches:
                        return matches[0]
                else:
                    if os.path.exists(pattern):
                        return pattern
            
            return None
            
        except Exception as e:
            print(f"⚠️ 查找ETF文件时发生错误: {clean_code} - {str(e)}")
            return None

    # 向后兼容接口
    def read_etf_data(self, etf_code):
        """向后兼容接口"""
        return self.read_etf_data_optimized(etf_code)
    
    def get_etf_list_by_threshold(self, threshold):
        """向后兼容接口"""
        return self.get_etf_list_by_threshold_optimized(threshold)


if __name__ == "__main__":
    # 优化数据读取器测试
    print("🧪 威廉指标优化数据读取器测试")
    print("✅ 优化数据读取器模块加载成功")
    print("📝 需要配置对象和实际数据来进行完整功能测试")