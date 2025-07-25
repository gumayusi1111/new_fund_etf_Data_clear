"""
RSI指标优化计算引擎
基于威廉指标的优化计算架构

优化内容：
1. 向量化RSI计算，提升计算效率
2. 智能的数据验证和异常处理
3. 支持增量更新的计算逻辑
4. 多周期RSI指标批量计算
5. 衍生指标计算（差值、变化率）
"""

import pandas as pd
import numpy as np
from datetime import datetime


class RSIEngineOptimized:
    """RSI指标优化计算引擎"""

    def __init__(self, config):
        """
        初始化RSI计算引擎
        
        Args:
            config: RSI配置对象
        """
        self.config = config
        self.rsi_periods = config.get_rsi_periods()
        
        # 性能统计
        self.calculation_stats = {
            'calculations_performed': 0,
            'total_calculation_time_ms': 0,
            'vectorization_benefits': 0,
            'data_validation_errors': 0
        }
        
        print("✅ RSI优化计算引擎初始化完成")
        print(f"🔢 RSI周期参数: {self.rsi_periods}")

    def calculate_rsi_indicators_batch(self, etf_data):
        """
        批量计算RSI指标（多周期）
        
        Args:
            etf_data: ETF价格数据，必须包含price_change_pct字段
            
        Returns:
            DataFrame: 包含所有RSI指标的数据
        """
        try:
            start_time = datetime.now()
            
            if etf_data is None or etf_data.empty:
                print("❌ 输入数据为空")
                return pd.DataFrame()
            
            # 验证数据完整性
            if not self._validate_input_data(etf_data):
                return pd.DataFrame()
            
            # 准备结果数据框
            result_df = etf_data[['日期']].copy()
            
            # 获取价格变化数据
            price_changes = etf_data['price_change_pct'].fillna(0)
            
            # 批量计算多周期RSI
            rsi_results = self._calculate_multi_period_rsi(price_changes)
            
            # 添加RSI结果到数据框
            for period_name, period_value in self.rsi_periods.items():
                rsi_column = f"rsi_{period_value}"
                if rsi_column in rsi_results:
                    result_df[rsi_column] = rsi_results[rsi_column]
            
            # 计算衍生指标
            result_df = self._calculate_derived_indicators(result_df)
            
            # 性能统计
            calculation_time = (datetime.now() - start_time).total_seconds() * 1000
            self.calculation_stats['calculations_performed'] += 1
            self.calculation_stats['total_calculation_time_ms'] += calculation_time
            
            print(f"🚀 RSI批量计算完成: {len(result_df)}行数据, 耗时{calculation_time:.2f}ms")
            
            return result_df
            
        except Exception as e:
            print(f"❌ RSI批量计算失败: {str(e)}")
            print(f"🔍 异常详情: {traceback.format_exc()}")
            self.calculation_stats['data_validation_errors'] += 1
            return pd.DataFrame()

    def _validate_input_data(self, etf_data):
        """
        验证输入数据的完整性
        
        Args:
            etf_data: ETF数据
            
        Returns:
            bool: 数据是否有效
        """
        try:
            # 检查必需字段
            required_fields = ['日期', 'price_change_pct']
            missing_fields = [field for field in required_fields if field not in etf_data.columns]
            
            if missing_fields:
                print(f"❌ 输入数据缺少必需字段: {missing_fields}")
                return False
            
            # 检查数据量
            min_data_points = self.config.DATA_QUALITY_REQUIREMENTS['min_data_points']
            if len(etf_data) < min_data_points:
                print(f"❌ 数据量不足: 需要{min_data_points}行，实际{len(etf_data)}行")
                return False
            
            # 检查价格变化数据
            price_changes = etf_data['price_change_pct']
            if price_changes.isna().all():
                print("❌ 价格变化数据全部为空")
                return False
            
            return True
            
        except Exception as e:
            print(f"❌ 数据验证失败: {str(e)}")
            return False

    def _calculate_multi_period_rsi(self, price_changes):
        """
        计算多周期RSI指标（向量化计算）
        
        Args:
            price_changes: 价格变化率序列
            
        Returns:
            dict: 包含各周期RSI的字典
        """
        try:
            rsi_results = {}
            
            # 分离涨跌
            gains = price_changes.where(price_changes > 0, 0)
            losses = -price_changes.where(price_changes < 0, 0)
            
            # 对每个周期计算RSI
            for period_name, period_value in self.rsi_periods.items():
                try:
                    # 使用威尔德平滑方法的向量化实现（标准RSI计算方法）
                    # alpha = 1/period (威尔德平滑系数)
                    alpha = 1.0 / period_value
                    
                    # 使用指数加权移动平均（EWM）实现威尔德平滑
                    # adjust=False确保使用递归公式：y[t] = (1-alpha)*y[t-1] + alpha*x[t]
                    avg_gains = gains.ewm(alpha=alpha, adjust=False).mean()
                    avg_losses = losses.ewm(alpha=alpha, adjust=False).mean()
                    
                    # 计算RS（相对强度）
                    # 避免除零错误
                    rs = avg_gains / avg_losses.replace(0, np.nan)
                    
                    # 计算RSI
                    rsi = 100 - (100 / (1 + rs))
                    
                    # 处理特殊情况
                    rsi = rsi.fillna(50)  # 无法计算时设为中性值50
                    
                    # 确保RSI在0-100范围内
                    rsi = rsi.clip(0, 100)
                    
                    # 保存结果（保留8位小数）
                    rsi_column = f"rsi_{period_value}"
                    rsi_results[rsi_column] = rsi.round(8)
                    
                    print(f"📊 RSI{period_value}计算完成: 有效数据{rsi.notna().sum()}行")
                    
                except Exception as e:
                    print(f"⚠️ RSI{period_value}计算失败: {str(e)}")
                    # 创建空的结果列
                    rsi_results[f"rsi_{period_value}"] = pd.Series([np.nan] * len(price_changes))
            
            return rsi_results
            
        except Exception as e:
            print(f"❌ 多周期RSI计算失败: {str(e)}")
            return {}

    def _calculate_derived_indicators(self, result_df):
        """
        计算RSI衍生指标
        
        Args:
            result_df: 包含基础RSI指标的数据框
            
        Returns:
            DataFrame: 包含衍生指标的数据框
        """
        try:
            # 获取RSI字段
            rsi_6_col = 'rsi_6'
            rsi_12_col = 'rsi_12'
            rsi_24_col = 'rsi_24'
            
            # 1. 计算RSI6与RSI24的差值
            if rsi_6_col in result_df.columns and rsi_24_col in result_df.columns:
                result_df['rsi_diff_6_24'] = (
                    result_df[rsi_6_col] - result_df[rsi_24_col]
                ).round(8)
                print("📊 RSI差值计算完成")
            else:
                print("⚠️ 无法计算RSI差值：缺少RSI6或RSI24")
                result_df['rsi_diff_6_24'] = np.nan
            
            # 2. 计算RSI12的变化率
            if rsi_12_col in result_df.columns:
                rsi_12 = result_df[rsi_12_col]
                rsi_12_prev = rsi_12.shift(1)
                
                # 避免除零错误，使用绝对值避免负数分母
                rsi_change_rate = ((rsi_12 - rsi_12_prev) / rsi_12_prev.abs().replace(0, np.nan) * 100)
                result_df['rsi_change_rate'] = rsi_change_rate.round(8)
                print("📊 RSI变化率计算完成")
            else:
                print("⚠️ 无法计算RSI变化率：缺少RSI12")
                result_df['rsi_change_rate'] = np.nan
            
            return result_df
            
        except Exception as e:
            print(f"❌ 衍生指标计算失败: {str(e)}")
            return result_df

    def calculate_incremental_update(self, existing_data, new_data):
        """
        计算增量更新的RSI指标
        
        Args:
            existing_data: 现有数据
            new_data: 新增数据
            
        Returns:
            DataFrame: 增量计算结果
        """
        try:
            if new_data is None or new_data.empty:
                print("📊 无新数据需要增量计算")
                return pd.DataFrame()
            
            # 合并数据进行增量计算
            if existing_data is not None and not existing_data.empty:
                # 获取足够的历史数据用于计算
                max_period = max(self.rsi_periods.values())
                
                # 取最近的历史数据
                recent_existing = existing_data.tail(max_period * 2) if len(existing_data) > max_period * 2 else existing_data
                combined_data = pd.concat([recent_existing, new_data], ignore_index=True)
            else:
                combined_data = new_data
            
            # 对合并后的数据进行完整计算
            full_result = self.calculate_rsi_indicators_batch(combined_data)
            
            if full_result.empty:
                return pd.DataFrame()
            
            # 只返回新数据对应的部分
            if existing_data is not None and not existing_data.empty:
                new_rows_count = len(new_data)
                incremental_result = full_result.tail(new_rows_count).copy()
            else:
                incremental_result = full_result
            
            print(f"⚡ RSI增量更新完成: {len(incremental_result)}行新数据")
            
            return incremental_result
            
        except Exception as e:
            print(f"❌ RSI增量更新失败: {str(e)}")
            return pd.DataFrame()

    def format_output_data(self, rsi_result, etf_code):
        """
        格式化输出数据
        
        Args:
            rsi_result: RSI计算结果
            etf_code: ETF代码
            
        Returns:
            DataFrame: 格式化后的输出数据
        """
        try:
            if rsi_result is None or rsi_result.empty:
                print("❌ RSI计算结果为空，无法格式化")
                return pd.DataFrame()
            
            # 准备输出数据
            output_df = pd.DataFrame()
            
            # 基础字段 - 确保每行都有ETF代码
            etf_code_str = str(etf_code) if etf_code else 'UNKNOWN'
            output_df['code'] = [etf_code_str] * len(rsi_result)
            
            # 处理日期字段
            if pd.api.types.is_datetime64_any_dtype(rsi_result['日期']):
                output_df['date'] = rsi_result['日期'].dt.strftime('%Y-%m-%d')
            else:
                # 如果不是datetime类型，尝试转换
                try:
                    date_series = pd.to_datetime(rsi_result['日期'])
                    output_df['date'] = date_series.dt.strftime('%Y-%m-%d')
                except:
                    output_df['date'] = rsi_result['日期'].astype(str)
            
            # RSI指标字段
            rsi_fields = ['rsi_6', 'rsi_12', 'rsi_24']
            for field in rsi_fields:
                if field in rsi_result.columns:
                    output_df[field] = rsi_result[field]
                else:
                    output_df[field] = np.nan
            
            # 衍生指标字段
            derived_fields = ['rsi_diff_6_24', 'rsi_change_rate']
            for field in derived_fields:
                if field in rsi_result.columns:
                    output_df[field] = rsi_result[field]
                else:
                    output_df[field] = np.nan
            
            # 添加计算时间戳
            output_df['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 移除包含NaN的行（前几行可能因为滚动计算而无效）
            initial_rows = len(output_df)
            output_df = output_df.dropna(subset=['rsi_6', 'rsi_12', 'rsi_24'], how='all')
            
            if len(output_df) < initial_rows:
                removed_rows = initial_rows - len(output_df)
                print(f"🧹 移除了{removed_rows}行无效数据")
            
            # ⚠️ 关键修复：按日期降序排列，最新数据在最上面
            if 'date' in output_df.columns and len(output_df) > 0:
                try:
                    # 转换日期列为datetime进行排序
                    output_df['date_sort'] = pd.to_datetime(output_df['date'])
                    output_df = output_df.sort_values('date_sort', ascending=False)
                    output_df = output_df.drop('date_sort', axis=1)  # 删除临时排序列
                    print(f"✅ 数据已按日期降序排列，最新数据在顶部")
                except Exception as sort_error:
                    print(f"⚠️ 日期排序警告: {str(sort_error)}")
            
            # 重置索引
            output_df = output_df.reset_index(drop=True)
            
            print(f"📋 RSI数据格式化完成: {len(output_df)}行有效数据")
            
            return output_df
            
        except Exception as e:
            print(f"❌ RSI数据格式化失败: {str(e)}")
            return pd.DataFrame()

    def calculate_rsi_statistics(self, rsi_result):
        """
        计算RSI统计信息
        
        Args:
            rsi_result: RSI计算结果
            
        Returns:
            dict: RSI统计信息
        """
        try:
            if rsi_result is None or rsi_result.empty:
                return {}
            
            stats = {}
            
            # 对每个RSI指标计算统计信息
            rsi_fields = ['rsi_6', 'rsi_12', 'rsi_24']
            
            for field in rsi_fields:
                if field in rsi_result.columns:
                    rsi_values = rsi_result[field].dropna()
                    
                    if not rsi_values.empty:
                        field_stats = {
                            'count': len(rsi_values),
                            'mean': round(rsi_values.mean(), 4),
                            'std': round(rsi_values.std(), 4),
                            'min': round(rsi_values.min(), 4),
                            'max': round(rsi_values.max(), 4),
                            'median': round(rsi_values.median(), 4),
                            'current': round(rsi_values.iloc[-1], 4) if len(rsi_values) > 0 else None
                        }
                        
                        # 计算分布统计
                        high_count = (rsi_values >= 70).sum()
                        low_count = (rsi_values <= 30).sum()
                        neutral_count = ((rsi_values > 30) & (rsi_values < 70)).sum()
                        
                        field_stats['distribution'] = {
                            'high_level_count': int(high_count),
                            'low_level_count': int(low_count),
                            'neutral_count': int(neutral_count),
                            'high_level_pct': round(high_count / len(rsi_values) * 100, 2),
                            'low_level_pct': round(low_count / len(rsi_values) * 100, 2),
                            'neutral_pct': round(neutral_count / len(rsi_values) * 100, 2)
                        }
                        
                        stats[field] = field_stats
            
            # 衍生指标统计
            if 'rsi_diff_6_24' in rsi_result.columns:
                diff_values = rsi_result['rsi_diff_6_24'].dropna()
                if not diff_values.empty:
                    stats['rsi_diff_6_24'] = {
                        'mean': round(diff_values.mean(), 4),
                        'std': round(diff_values.std(), 4),
                        'current': round(diff_values.iloc[-1], 4) if len(diff_values) > 0 else None
                    }
            
            if 'rsi_change_rate' in rsi_result.columns:
                change_values = rsi_result['rsi_change_rate'].dropna()
                if not change_values.empty:
                    stats['rsi_change_rate'] = {
                        'mean': round(change_values.mean(), 4),
                        'std': round(change_values.std(), 4),
                        'current': round(change_values.iloc[-1], 4) if len(change_values) > 0 else None
                    }
            
            return stats
            
        except Exception as e:
            print(f"❌ RSI统计计算失败: {str(e)}")
            return {}

    def get_performance_stats(self):
        """获取计算引擎性能统计"""
        if self.calculation_stats['calculations_performed'] > 0:
            avg_time = (self.calculation_stats['total_calculation_time_ms'] / 
                       self.calculation_stats['calculations_performed'])
        else:
            avg_time = 0
        
        return {
            'calculations_performed': self.calculation_stats['calculations_performed'],
            'average_calculation_time_ms': round(avg_time, 2),
            'total_calculation_time_ms': round(self.calculation_stats['total_calculation_time_ms'], 2),
            'vectorization_benefits': self.calculation_stats['vectorization_benefits'],
            'data_validation_errors': self.calculation_stats['data_validation_errors']
        }

    def print_performance_summary(self):
        """打印计算引擎性能摘要"""
        stats = self.get_performance_stats()
        
        print(f"\n{'=' * 60}")
        print("🚀 RSI计算引擎性能摘要")
        print(f"{'=' * 60}")
        print(f"🔢 执行计算次数: {stats['calculations_performed']}")
        print(f"⏱️ 平均计算时间: {stats['average_calculation_time_ms']:.2f}ms")
        print(f"⚡ 总计算时间: {stats['total_calculation_time_ms']:.2f}ms")
        print(f"🚀 向量化优化: {stats['vectorization_benefits']}")
        print(f"❌ 验证错误数: {stats['data_validation_errors']}")
        print(f"{'=' * 60}")


if __name__ == "__main__":
    # RSI计算引擎测试
    try:
        from infrastructure.config import RSIConfig
        
        print("🧪 RSI计算引擎测试")
        config = RSIConfig()
        engine = RSIEngineOptimized(config)
        
        # 创建测试数据
        import numpy as np
        test_dates = pd.date_range('2023-01-01', periods=50, freq='D')
        test_changes = np.random.normal(0, 0.02, 50)  # 模拟日收益率
        
        test_data = pd.DataFrame({
            '日期': test_dates,
            'price_change_pct': test_changes
        })
        
        # 测试RSI计算
        result = engine.calculate_rsi_indicators_batch(test_data)
        if not result.empty:
            print(f"✅ RSI计算测试成功: {len(result)}行结果")
            
            # 测试数据格式化
            formatted = engine.format_output_data(result, '000001')
            print(f"✅ 数据格式化测试成功: {len(formatted)}行")
            
            # 测试统计计算
            stats = engine.calculate_rsi_statistics(result)
            if stats:
                print(f"✅ 统计计算测试成功: {len(stats)}个指标")
        
        # 打印性能摘要
        engine.print_performance_summary()
        
        print("✅ RSI计算引擎测试完成")
        
    except Exception as e:
        print(f"❌ RSI计算引擎测试失败: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")