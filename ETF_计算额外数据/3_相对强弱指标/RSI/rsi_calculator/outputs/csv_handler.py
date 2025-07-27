"""
RSI指标CSV输出处理器
基于威廉指标的CSV处理架构

功能特性：
1. 标准化的CSV输出格式
2. 支持增量数据合并
3. 多门槛数据组织
4. 数据完整性验证
"""

import os
import pandas as pd
from datetime import datetime
import traceback


class RSICSVHandler:
    """RSI指标CSV输出处理器"""

    def __init__(self, config):
        """
        初始化RSI CSV处理器
        
        Args:
            config: RSI配置对象
        """
        self.config = config
        self.data_output_path = config.data_output_path
        
        # 输出统计
        self.output_stats = {
            'files_written': 0,
            'total_rows_written': 0,
            'merge_operations': 0,
            'validation_errors': 0
        }
        
        # 确保输出目录存在
        self._ensure_output_directories()
        
        print("✅ RSI CSV处理器初始化完成")
        print(f"📤 输出路径: {self.data_output_path}")

    def _ensure_output_directories(self):
        """确保输出目录结构存在"""
        try:
            directories = [
                self.data_output_path,
                self.config.get_data_output_path("3000万门槛"),
                self.config.get_data_output_path("5000万门槛")
            ]
            
            for directory in directories:
                if not os.path.exists(directory):
                    os.makedirs(directory, exist_ok=True)
                    print(f"📁 创建输出目录: {directory}")
                    
        except Exception as e:
            print(f"❌ 创建输出目录失败: {str(e)}")

    def save_etf_rsi_data(self, etf_code, rsi_data, threshold):
        """
        保存ETF的RSI数据到CSV文件
        
        Args:
            etf_code: ETF代码
            rsi_data: RSI计算结果数据
            threshold: 门槛值
            
        Returns:
            bool: 保存是否成功
        """
        try:
            if rsi_data is None or rsi_data.empty:
                print(f"⚠️ RSI数据为空，跳过保存: {etf_code}")
                return False
            
            # 获取输出文件路径
            output_file_path = self._get_output_file_path(etf_code, threshold)
            
            # 确保输出目录存在
            output_dir = os.path.dirname(output_file_path)
            if not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            
            # 验证数据格式
            if not self._validate_rsi_data(rsi_data, etf_code):
                return False
            
            # 设置数值列的格式化精度（8位小数）
            numeric_columns = ['rsi_6', 'rsi_12', 'rsi_24', 'rsi_diff_6_24', 'rsi_change_rate']
            format_dict = {}
            for col in numeric_columns:
                if col in rsi_data.columns:
                    format_dict[col] = '%.8f'
            
            # 保存CSV文件，使用8位小数精度格式
            rsi_data.to_csv(
                output_file_path, 
                index=False, 
                encoding=self.config.CSV_CONFIG['encoding'],
                date_format=self.config.CSV_CONFIG['date_format'],
                float_format='%.8f'  # 强制使用8位小数格式
            )
            
            # 更新统计
            self.output_stats['files_written'] += 1
            self.output_stats['total_rows_written'] += len(rsi_data)
            
            print(f"📤 RSI数据保存成功: {etf_code} ({len(rsi_data)}行数据)")
            
            return True
            
        except Exception as e:
            print(f"❌ RSI数据保存失败: {etf_code} - {str(e)}")
            print(f"🔍 异常详情: {traceback.format_exc()}")
            self.output_stats['validation_errors'] += 1
            return False

    def _get_output_file_path(self, etf_code, threshold):
        """获取输出文件路径"""
        output_dir = self.config.get_data_output_path(threshold)
        return os.path.join(output_dir, f"{etf_code}.csv")

    def _validate_rsi_data(self, rsi_data, etf_code):
        """
        验证RSI数据格式
        
        Args:
            rsi_data: RSI数据
            etf_code: ETF代码
            
        Returns:
            bool: 数据是否有效
        """
        try:
            # 检查必需字段
            required_fields = ['code', 'date', 'rsi_6', 'rsi_12', 'rsi_24']
            missing_fields = [field for field in required_fields if field not in rsi_data.columns]
            
            if missing_fields:
                print(f"❌ RSI数据缺少必需字段: {etf_code} - {missing_fields}")
                self.output_stats['validation_errors'] += 1
                return False
            
            # 检查数据类型
            if rsi_data['code'].isna().any() or (rsi_data['code'] == '').any():
                print(f"⚠️ RSI数据包含空的ETF代码: {etf_code}")
                # 不返回False，只给出警告，允许数据保存
            
            if rsi_data['date'].isna().any():
                print(f"⚠️ RSI数据包含空的日期: {etf_code}")
                return False
            
            # 检查RSI数值范围（0-100）
            rsi_fields = ['rsi_6', 'rsi_12', 'rsi_24']
            for field in rsi_fields:
                if field in rsi_data.columns:
                    valid_values = rsi_data[field].dropna()
                    if not valid_values.empty:
                        out_of_range = ((valid_values < 0) | (valid_values > 100))
                        if out_of_range.any():
                            print(f"⚠️ RSI数据超出范围(0-100): {etf_code} - {field}")
                            # 不返回False，允许数据保存但给出警告
            
            return True
            
        except Exception as e:
            print(f"❌ RSI数据验证失败: {etf_code} - {str(e)}")
            self.output_stats['validation_errors'] += 1
            return False

    def merge_incremental_data(self, etf_code, threshold, incremental_data):
        """
        合并增量数据到现有文件
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            incremental_data: 增量数据
            
        Returns:
            DataFrame: 合并后的完整数据
        """
        try:
            if incremental_data is None or incremental_data.empty:
                print(f"📊 无增量数据需要合并: {etf_code}")
                return pd.DataFrame()
            
            output_file_path = self._get_output_file_path(etf_code, threshold)
            
            # 检查现有文件
            if os.path.exists(output_file_path):
                try:
                    existing_data = pd.read_csv(output_file_path)
                    print(f"📊 加载现有数据: {etf_code} ({len(existing_data)}行)")
                except Exception as e:
                    print(f"⚠️ 读取现有文件失败: {etf_code} - {str(e)}")
                    existing_data = pd.DataFrame()
            else:
                existing_data = pd.DataFrame()
            
            # 合并数据
            if not existing_data.empty:
                # 去重合并（基于日期）
                if 'date' in existing_data.columns and 'date' in incremental_data.columns:
                    # 移除现有数据中与增量数据日期重复的行
                    incremental_dates = set(incremental_data['date'].unique())
                    existing_data = existing_data[~existing_data['date'].isin(incremental_dates)]
                    
                    # 合并数据
                    merged_data = pd.concat([existing_data, incremental_data], ignore_index=True)
                    
                    # 按日期排序
                    merged_data = merged_data.sort_values('date').reset_index(drop=True)
                    
                    print(f"📊 数据合并完成: {etf_code} ({len(existing_data)}+{len(incremental_data)}={len(merged_data)}行)")
                else:
                    print(f"⚠️ 无法基于日期合并，使用增量数据: {etf_code}")
                    merged_data = incremental_data
            else:
                merged_data = incremental_data
                print(f"📊 使用增量数据作为完整数据: {etf_code} ({len(merged_data)}行)")
            
            # 保存合并后的数据
            if self.save_etf_rsi_data(etf_code, merged_data, threshold):
                self.output_stats['merge_operations'] += 1
                return merged_data
            else:
                return pd.DataFrame()
            
        except Exception as e:
            print(f"❌ 增量数据合并失败: {etf_code} - {str(e)}")
            print(f"🔍 异常详情: {traceback.format_exc()}")
            return pd.DataFrame()

    def load_existing_data(self, etf_code, threshold):
        """
        加载现有的RSI数据
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            
        Returns:
            DataFrame: 现有数据
        """
        try:
            output_file_path = self._get_output_file_path(etf_code, threshold)
            
            if not os.path.exists(output_file_path):
                print(f"📁 输出文件不存在: {etf_code}")
                return pd.DataFrame()
            
            # 读取现有数据
            existing_data = pd.read_csv(output_file_path)
            
            if existing_data.empty:
                print(f"⚠️ 输出文件为空: {etf_code}")
                return pd.DataFrame()
            
            print(f"📊 加载现有数据: {etf_code} ({len(existing_data)}行)")
            return existing_data
            
        except Exception as e:
            print(f"❌ 加载现有数据失败: {etf_code} - {str(e)}")
            return pd.DataFrame()

    def batch_export_rsi_data(self, rsi_results_dict, threshold):
        """
        批量导出RSI数据
        
        Args:
            rsi_results_dict: RSI结果字典 {etf_code: rsi_data}
            threshold: 门槛值
            
        Returns:
            dict: 导出统计信息
        """
        try:
            export_stats = {
                'total_etfs': len(rsi_results_dict),
                'successful_exports': 0,
                'failed_exports': 0,
                'total_rows': 0
            }
            
            print(f"📦 开始批量导出RSI数据: {threshold}")
            print(f"📊 待导出ETF数量: {export_stats['total_etfs']}")
            
            for etf_code, rsi_data in rsi_results_dict.items():
                try:
                    if self.save_etf_rsi_data(etf_code, rsi_data, threshold):
                        export_stats['successful_exports'] += 1
                        export_stats['total_rows'] += len(rsi_data) if rsi_data is not None else 0
                    else:
                        export_stats['failed_exports'] += 1
                        
                except Exception as e:
                    print(f"❌ 导出单个ETF失败: {etf_code} - {str(e)}")
                    export_stats['failed_exports'] += 1
            
            # 打印导出摘要
            success_rate = (export_stats['successful_exports'] / export_stats['total_etfs'] * 100) if export_stats['total_etfs'] > 0 else 0
            
            print(f"📦 批量导出完成:")
            print(f"   总ETF数: {export_stats['total_etfs']}")
            print(f"   成功导出: {export_stats['successful_exports']}")
            print(f"   失败导出: {export_stats['failed_exports']}")
            print(f"   成功率: {success_rate:.1f}%")
            print(f"   总行数: {export_stats['total_rows']}")
            
            return export_stats
            
        except Exception as e:
            print(f"❌ 批量导出失败: {str(e)}")
            return {'error': str(e)}

    def get_file_list(self, threshold):
        """
        获取指定门槛的所有RSI文件列表
        
        Args:
            threshold: 门槛值
            
        Returns:
            list: 文件列表
        """
        try:
            output_dir = self.config.get_data_output_path(threshold)
            
            if not os.path.exists(output_dir):
                print(f"📁 输出目录不存在: {output_dir}")
                return []
            
            # 获取所有CSV文件
            csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
            
            # 提取ETF代码
            etf_codes = []
            for filename in csv_files:
                etf_code = filename.replace('.csv', '')
                if etf_code.isdigit() and len(etf_code) == 6:
                    etf_codes.append(etf_code)
            
            etf_codes.sort()
            print(f"📊 发现{len(etf_codes)}个RSI数据文件: {threshold}")
            
            return etf_codes
            
        except Exception as e:
            print(f"❌ 获取文件列表失败: {threshold} - {str(e)}")
            return []

    def validate_output_data(self, threshold, sample_size=10):
        """
        验证输出数据质量
        
        Args:
            threshold: 门槛值
            sample_size: 抽样验证的文件数量
            
        Returns:
            dict: 验证结果
        """
        try:
            validation_result = {
                'total_files': 0,
                'validated_files': 0,
                'valid_files': 0,
                'invalid_files': 0,
                'validation_errors': []
            }
            
            # 获取文件列表
            etf_codes = self.get_file_list(threshold)
            validation_result['total_files'] = len(etf_codes)
            
            if not etf_codes:
                return validation_result
            
            # 抽样验证
            sample_codes = etf_codes[:min(sample_size, len(etf_codes))]
            
            for etf_code in sample_codes:
                validation_result['validated_files'] += 1
                
                try:
                    # 读取文件
                    rsi_data = self.load_existing_data(etf_code, threshold)
                    
                    if rsi_data.empty:
                        validation_result['invalid_files'] += 1
                        validation_result['validation_errors'].append(f"{etf_code}: 文件为空")
                        continue
                    
                    # 验证数据格式
                    if self._validate_rsi_data(rsi_data, etf_code):
                        validation_result['valid_files'] += 1
                    else:
                        validation_result['invalid_files'] += 1
                        validation_result['validation_errors'].append(f"{etf_code}: 数据格式无效")
                        
                except Exception as e:
                    validation_result['invalid_files'] += 1
                    validation_result['validation_errors'].append(f"{etf_code}: 读取失败 - {str(e)}")
            
            # 计算验证成功率
            if validation_result['validated_files'] > 0:
                validation_result['success_rate'] = (
                    validation_result['valid_files'] / validation_result['validated_files'] * 100
                )
            else:
                validation_result['success_rate'] = 0
            
            print(f"🔍 数据验证完成: {threshold}")
            print(f"   验证文件: {validation_result['validated_files']}/{validation_result['total_files']}")
            print(f"   有效文件: {validation_result['valid_files']}")
            print(f"   无效文件: {validation_result['invalid_files']}")
            print(f"   成功率: {validation_result['success_rate']:.1f}%")
            
            return validation_result
            
        except Exception as e:
            print(f"❌ 数据验证失败: {threshold} - {str(e)}")
            return {'error': str(e)}

    def get_output_stats(self):
        """获取输出统计信息"""
        return {
            'files_written': self.output_stats['files_written'],
            'total_rows_written': self.output_stats['total_rows_written'],
            'merge_operations': self.output_stats['merge_operations'],
            'validation_errors': self.output_stats['validation_errors'],
            'average_rows_per_file': (
                self.output_stats['total_rows_written'] / max(1, self.output_stats['files_written'])
            )
        }

    def print_output_summary(self):
        """打印输出处理器摘要"""
        stats = self.get_output_stats()
        
        print(f"\n{'=' * 60}")
        print("📤 RSI CSV处理器摘要")
        print(f"{'=' * 60}")
        print(f"📁 已写入文件: {stats['files_written']}")
        print(f"📊 总写入行数: {stats['total_rows_written']}")
        print(f"📈 平均每文件行数: {stats['average_rows_per_file']:.1f}")
        print(f"🔄 合并操作次数: {stats['merge_operations']}")
        print(f"❌ 验证错误数: {stats['validation_errors']}")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    # CSV处理器测试
    try:
        from infrastructure.config import RSIConfig
        
        print("🧪 RSI CSV处理器测试")
        config = RSIConfig()
        csv_handler = RSICSVHandler(config)
        
        # 打印处理器摘要
        csv_handler.print_output_summary()
        
        # 测试获取文件列表
        for threshold in ["3000万门槛", "5000万门槛"]:
            file_list = csv_handler.get_file_list(threshold)
            print(f"📊 {threshold}: {len(file_list)}个文件")
        
        print("✅ RSI CSV处理器测试完成")
        
    except Exception as e:
        print(f"❌ RSI CSV处理器测试失败: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")