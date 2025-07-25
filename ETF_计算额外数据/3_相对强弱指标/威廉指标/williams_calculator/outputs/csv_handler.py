"""
威廉指标CSV输出处理模块
基于趋势指标和波动性指标的统一输出架构

CSV处理器，提供：
- 标准化CSV输出格式
- 8位小数精度统一处理
- 双门槛文件组织管理
- UTF-8编码输出
- 数据完整性验证
- 批量文件操作支持
"""

import os
import pandas as pd
import numpy as np
from datetime import datetime
import warnings

# 忽略pandas的链式赋值警告
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)


class WilliamsCSVHandler:
    """威廉指标CSV输出处理器"""
    
    def __init__(self, config):
        """
        初始化CSV处理器
        
        Args:
            config: 威廉指标配置对象
        """
        self.config = config
        self.data_output_path = config.data_output_path
        self.csv_config = config.CSV_CONFIG
        self.output_fields = config.get_output_fields()
        self.decimal_precision = config.CSV_CONFIG['decimal_places']
        
        # 确保输出目录存在
        self._ensure_output_directories()

    def _ensure_output_directories(self):
        """确保输出目录结构存在"""
        directories = [
            self.data_output_path,
            os.path.join(self.data_output_path, "3000万门槛"),
            os.path.join(self.data_output_path, "5000万门槛")
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)

    def save_etf_williams_data(self, etf_code, df, threshold):
        """
        保存ETF威廉指标数据到CSV文件
        
        Args:
            etf_code: ETF代码
            df: 威廉指标数据DataFrame
            threshold: 门槛值('3000万门槛' or '5000万门槛')
            
        Returns:
            bool: 保存是否成功
        """
        try:
            if df.empty:
                print(f"⚠️ 空数据，跳过保存: {etf_code}")
                return False
            
            # 格式化输出数据
            formatted_df = self._format_output_data(df, etf_code)
            if formatted_df.empty:
                print(f"⚠️ 数据格式化失败: {etf_code}")
                return False
            
            # 构建输出文件路径
            output_file_path = self._get_output_file_path(etf_code, threshold)
            
            # 保存到CSV文件
            self._save_to_csv_file(formatted_df, output_file_path, etf_code)
            
            return True
            
        except Exception as e:
            print(f"❌ 保存ETF威廉指标数据失败: {etf_code} - {str(e)}")
            return False

    def _format_output_data(self, df, etf_code):
        """
        格式化输出数据
        
        Args:
            df: 原始数据DataFrame
            etf_code: ETF代码
            
        Returns:
            DataFrame: 格式化后的数据
        """
        try:
            if df.empty:
                return pd.DataFrame()
            
            # 创建输出DataFrame
            output_df = pd.DataFrame()
            
            # 添加基础字段
            clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
            output_df['code'] = [clean_code] * len(df)
            
            # 处理日期字段
            if 'date' in df.columns:
                output_df['date'] = pd.to_datetime(df['date']).dt.strftime(self.csv_config['date_format'])
            elif '日期' in df.columns:
                output_df['date'] = pd.to_datetime(df['日期']).dt.strftime(self.csv_config['date_format'])
            else:
                print(f"⚠️ 未找到日期字段: {etf_code}")
                return pd.DataFrame()
            
            # 添加威廉指标字段
            williams_fields = ['wr_9', 'wr_14', 'wr_21', 'wr_diff_9_21', 'wr_range', 'wr_change_rate']
            for field in williams_fields:
                if field in df.columns:
                    # 确保8位小数精度
                    output_df[field] = df[field].round(self.decimal_precision)
                else:
                    # 如果字段不存在，填充NaN
                    output_df[field] = np.nan
                    print(f"⚠️ 缺少威廉指标字段: {field} - {etf_code}")
            
            # 添加计算时间戳
            if 'calc_time' in df.columns:
                output_df['calc_time'] = df['calc_time']
            else:
                output_df['calc_time'] = datetime.now().strftime(self.csv_config['time_format'])
            
            # 按日期倒序排列（最新数据在前）
            output_df = output_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            return output_df
            
        except Exception as e:
            print(f"❌ 数据格式化失败: {etf_code} - {str(e)}")
            return pd.DataFrame()

    def _get_output_file_path(self, etf_code, threshold):
        """
        获取输出文件路径
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            
        Returns:
            str: 输出文件路径
        """
        clean_code = etf_code.replace('.SH', '').replace('.SZ', '')
        filename = f"{clean_code}.csv"
        return os.path.join(self.data_output_path, threshold, filename)

    def _save_to_csv_file(self, df, file_path, etf_code):
        """
        保存DataFrame到CSV文件
        
        Args:
            df: 数据DataFrame
            file_path: 文件路径
            etf_code: ETF代码（用于日志）
        """
        try:
            # 创建目录（如果不存在）
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # 保存到CSV文件，使用UTF-8编码
            df.to_csv(
                file_path, 
                index=False, 
                encoding=self.csv_config['encoding'],
                float_format=f'%.{self.decimal_precision}f'
            )
            
            print(f"💾 CSV文件已保存: {etf_code} -> {file_path}")
            
        except Exception as e:
            print(f"❌ CSV文件保存失败: {etf_code} - {str(e)}")
            raise

    def load_existing_williams_data(self, etf_code, threshold):
        """
        加载现有的威廉指标数据
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            
        Returns:
            DataFrame: 现有数据，不存在返回空DataFrame
        """
        try:
            file_path = self._get_output_file_path(etf_code, threshold)
            
            if not os.path.exists(file_path):
                return pd.DataFrame()
            
            # 读取现有数据
            existing_df = pd.read_csv(file_path, encoding=self.csv_config['encoding'])
            
            # 验证数据完整性
            if not self._validate_csv_data_integrity(existing_df, etf_code):
                print(f"⚠️ 现有CSV数据完整性验证失败: {etf_code}")
                return pd.DataFrame()
            
            return existing_df
            
        except Exception as e:
            print(f"⚠️ 加载现有威廉指标数据失败: {etf_code} - {str(e)}")
            return pd.DataFrame()

    def _validate_csv_data_integrity(self, df, etf_code):
        """
        验证CSV数据完整性
        
        Args:
            df: 数据DataFrame
            etf_code: ETF代码
            
        Returns:
            bool: 数据完整性是否通过
        """
        try:
            if df.empty:
                return False
            
            # 检查必要字段
            required_fields = ['code', 'date', 'wr_9', 'wr_14', 'wr_21']
            for field in required_fields:
                if field not in df.columns:
                    print(f"⚠️ CSV缺少必要字段: {field} - {etf_code}")
                    return False
            
            # 检查数据类型
            try:
                pd.to_datetime(df['date'])
            except:
                print(f"⚠️ 日期格式错误: {etf_code}")
                return False
            
            # 检查威廉指标数值范围（-100到0之间）
            williams_columns = ['wr_9', 'wr_14', 'wr_21']
            for col in williams_columns:
                if col in df.columns:
                    valid_values = df[col].dropna()
                    if not valid_values.empty:
                        if (valid_values < -100).any() or (valid_values > 0).any():
                            print(f"⚠️ 威廉指标数值超出正常范围: {col} - {etf_code}")
                            # 不返回False，只是警告
            
            return True
            
        except Exception as e:
            print(f"⚠️ CSV数据完整性验证异常: {etf_code} - {str(e)}")
            return False

    def merge_incremental_data(self, etf_code, threshold, new_df):
        """
        增量合并数据
        
        将新计算的数据与现有数据合并，避免重复
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            new_df: 新计算的数据
            
        Returns:
            DataFrame: 合并后的完整数据
        """
        try:
            # 加载现有数据
            existing_df = self.load_existing_williams_data(etf_code, threshold)
            
            if existing_df.empty:
                # 如果没有现有数据，直接返回新数据
                return new_df
            
            if new_df.empty:
                # 如果没有新数据，返回现有数据
                return existing_df
            
            # 合并数据
            combined_df = pd.concat([existing_df, new_df], ignore_index=True)
            
            # 按日期去重，保留最新的记录
            combined_df = combined_df.drop_duplicates(subset=['date'], keep='last')
            
            # 按日期倒序排列
            combined_df = combined_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            print(f"🔄 数据合并完成: {etf_code} - 现有{len(existing_df)}条，新增{len(new_df)}条，合并后{len(combined_df)}条")
            
            return combined_df
            
        except Exception as e:
            print(f"❌ 增量数据合并失败: {etf_code} - {str(e)}")
            # 合并失败时返回新数据
            return new_df

    def batch_save_williams_data(self, etf_data_dict, threshold):
        """
        批量保存威廉指标数据
        
        Args:
            etf_data_dict: ETF数据字典 {etf_code: dataframe}
            threshold: 门槛值
            
        Returns:
            dict: 保存结果统计
        """
        try:
            results = {
                'success_count': 0,
                'fail_count': 0,
                'total_count': len(etf_data_dict),
                'failed_etfs': []
            }
            
            print(f"🚀 开始批量保存威廉指标数据: {threshold}")
            print(f"📊 待保存ETF数量: {results['total_count']}")
            
            for etf_code, df in etf_data_dict.items():
                try:
                    if self.save_etf_williams_data(etf_code, df, threshold):
                        results['success_count'] += 1
                    else:
                        results['fail_count'] += 1
                        results['failed_etfs'].append(etf_code)
                        
                except Exception as e:
                    print(f"❌ 批量保存中出现异常: {etf_code} - {str(e)}")
                    results['fail_count'] += 1
                    results['failed_etfs'].append(etf_code)
            
            # 打印保存结果
            success_rate = (results['success_count'] / results['total_count'] * 100) if results['total_count'] > 0 else 0
            print(f"✅ 批量保存完成: {threshold}")
            print(f"📈 成功率: {success_rate:.1f}% ({results['success_count']}/{results['total_count']})")
            
            if results['failed_etfs']:
                print(f"❌ 失败的ETF: {', '.join(results['failed_etfs'][:10])}")
                if len(results['failed_etfs']) > 10:
                    print(f"   ... 以及其他{len(results['failed_etfs']) - 10}个")
            
            return results
            
        except Exception as e:
            print(f"❌ 批量保存过程发生异常: {str(e)}")
            return {'success_count': 0, 'fail_count': len(etf_data_dict), 'total_count': len(etf_data_dict)}

    def get_output_statistics(self, threshold):
        """
        获取输出文件统计信息
        
        Args:
            threshold: 门槛值
            
        Returns:
            dict: 统计信息
        """
        try:
            output_dir = os.path.join(self.data_output_path, threshold)
            
            if not os.path.exists(output_dir):
                return {'file_count': 0, 'total_size_mb': 0}
            
            # 统计CSV文件
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            total_size = 0
            
            for filename in csv_files:
                file_path = os.path.join(output_dir, filename)
                total_size += os.path.getsize(file_path)
            
            return {
                'file_count': len(csv_files),
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'output_directory': output_dir
            }
            
        except Exception as e:
            print(f"⚠️ 获取输出统计失败: {threshold} - {str(e)}")
            return {'file_count': 0, 'total_size_mb': 0}

    def validate_output_format(self, etf_code, threshold):
        """
        验证输出文件格式
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            
        Returns:
            dict: 验证结果
        """
        try:
            file_path = self._get_output_file_path(etf_code, threshold)
            
            if not os.path.exists(file_path):
                return {'is_valid': False, 'error': '文件不存在'}
            
            # 读取文件
            df = pd.read_csv(file_path, encoding=self.csv_config['encoding'])
            
            validation_result = {
                'is_valid': True,
                'file_path': file_path,
                'record_count': len(df),
                'columns': list(df.columns),
                'issues': []
            }
            
            # 验证字段完整性
            expected_fields = ['code', 'date', 'wr_9', 'wr_14', 'wr_21', 'wr_diff_9_21', 'wr_range', 'wr_change_rate', 'calc_time']
            for field in expected_fields:
                if field not in df.columns:
                    validation_result['issues'].append(f"缺少字段: {field}")
            
            # 验证数据精度
            williams_fields = ['wr_9', 'wr_14', 'wr_21', 'wr_diff_9_21', 'wr_range', 'wr_change_rate']
            for field in williams_fields:
                if field in df.columns:
                    # 检查小数位数是否符合要求
                    sample_values = df[field].dropna().head(5)
                    for value in sample_values:
                        if pd.notna(value):
                            decimal_places = len(str(value).split('.')[-1]) if '.' in str(value) else 0
                            if decimal_places > self.decimal_precision:
                                validation_result['issues'].append(f"{field}字段精度过高: {decimal_places}位小数")
                                break
            
            if validation_result['issues']:
                validation_result['is_valid'] = False
            
            return validation_result
            
        except Exception as e:
            return {
                'is_valid': False,
                'error': f"验证过程发生异常: {str(e)}"
            }

    def print_output_summary(self):
        """打印输出处理器摘要"""
        print("=" * 60)
        print("📤 威廉指标CSV输出处理器摘要")
        print("=" * 60)
        print(f"📁 输出路径: {self.data_output_path}")
        print(f"📊 数值精度: {self.decimal_precision}位小数")
        print(f"🔤 编码格式: {self.csv_config['encoding']}")
        print(f"📅 日期格式: {self.csv_config['date_format']}")
        print(f"⏰ 时间格式: {self.csv_config['time_format']}")
        
        # 统计各门槛的输出文件
        for threshold in ["3000万门槛", "5000万门槛"]:
            stats = self.get_output_statistics(threshold)
            print(f"📈 {threshold}: {stats['file_count']}个文件, {stats['total_size_mb']}MB")
        
        print("=" * 60)


if __name__ == "__main__":
    # CSV处理器测试
    print("🧪 威廉指标CSV处理器测试")
    
    # 注意：这里需要实际的配置对象来完整测试
    # 这个测试主要用于验证代码语法和基本逻辑
    print("✅ CSV处理器模块加载成功")
    print("📝 需要配置对象来进行完整功能测试")