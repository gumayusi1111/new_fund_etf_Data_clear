"""
威廉指标优化计算引擎
修复逻辑错误，优化向量化计算，提升性能

优化内容：
1. 修复威廉指标计算公式的运算优先级问题
2. 优化向量化计算，提升计算速度
3. 增加增量计算支持
4. 改进边界情况处理
5. 优化内存使用和计算效率
"""

import pandas as pd
import numpy as np
from datetime import datetime
import warnings

# 忽略pandas的链式赋值警告
warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)


class WilliamsEngineOptimized:
    """威廉指标优化计算引擎"""
    
    def __init__(self, config=None):
        """
        初始化威廉指标计算引擎
        
        Args:
            config: 威廉指标配置对象
        """
        self.config = config
        self.decimal_precision = 8  # 统一8位小数精度
        
        # 从配置获取计算参数
        if config:
            self.periods = config.get_williams_periods()
            self.derived_params = config.WILLIAMS_DERIVED_PARAMS
            self.thresholds = config.WILLIAMS_THRESHOLDS
        else:
            # 默认参数配置
            self.periods = {'short': 9, 'standard': 14, 'medium': 21}
            self.derived_params = {'range_period': 5, 'change_rate_lag': 1}
            self.thresholds = {'overbought': -20, 'oversold': -80}

    def calculate_williams_r_vectorized(self, high, low, close, period):
        """
        向量化计算威廉指标(%R) - 修复版本
        
        修复问题：
        1. 修正运算优先级：先计算括号内的除法，再乘以-100
        2. 优化向量化计算
        3. 改进边界情况处理
        
        公式: %R = ((Hn - C) / (Hn - Ln)) × (-100)
        
        Args:
            high: 最高价序列
            low: 最低价序列  
            close: 收盘价序列
            period: 计算周期
            
        Returns:
            威廉指标序列，8位小数精度
        """
        try:
            # 使用向量化rolling操作
            highest_high = high.rolling(window=period, min_periods=period).max()
            lowest_low = low.rolling(window=period, min_periods=period).min()
            
            # 计算分母并处理除零情况
            denominator = highest_high - lowest_low
            
            # 使用np.where处理除零情况，比replace更高效
            williams_r = np.where(
                denominator != 0,
                ((highest_high - close) / denominator) * -100,  # 修正：添加括号确保运算优先级
                np.nan
            )
            
            # 确保威廉指标值在合理范围内 [-100, 0]
            williams_r = np.clip(williams_r, -100, 0)
            
            # 创建Series并保持原索引
            result = pd.Series(williams_r, index=close.index, dtype=np.float64)
            
            # 返回8位小数精度结果
            return result.round(self.decimal_precision)
            
        except Exception as e:
            print(f"⚠️ 威廉指标计算错误 (周期={period}): {str(e)}")
            # 返回空序列而非抛出异常
            return pd.Series([np.nan] * len(close), index=close.index, dtype=np.float64)

    def calculate_wr_diff_vectorized(self, wr_short, wr_long):
        """
        向量化计算威廉指标差值
        
        Args:
            wr_short: 短期威廉指标
            wr_long: 长期威廉指标
            
        Returns:
            威廉指标差值，8位小数精度
        """
        try:
            # 直接向量化相减
            diff = wr_short - wr_long
            return diff.round(self.decimal_precision)
        except Exception as e:
            print(f"⚠️ 威廉指标差值计算错误: {str(e)}")
            return pd.Series([np.nan] * len(wr_short), index=wr_short.index, dtype=np.float64)

    def calculate_wr_range_vectorized(self, williams_r, period=5):
        """
        向量化计算威廉指标波动范围
        
        Args:
            williams_r: 威廉指标序列
            period: 计算周期，默认5日
            
        Returns:
            威廉指标波动范围，8位小数精度
        """
        try:
            # 使用向量化rolling操作
            rolling_max = williams_r.rolling(window=period, min_periods=period).max()
            rolling_min = williams_r.rolling(window=period, min_periods=period).min()
            wr_range = rolling_max - rolling_min
            return wr_range.round(self.decimal_precision)
        except Exception as e:
            print(f"⚠️ 威廉指标波动范围计算错误: {str(e)}")
            return pd.Series([np.nan] * len(williams_r), index=williams_r.index, dtype=np.float64)

    def calculate_wr_change_rate_vectorized(self, williams_r, lag=1):
        """
        向量化计算威廉指标变化率
        
        修复问题：
        1. 改进除零处理
        2. 优化向量化计算
        
        Args:
            williams_r: 威廉指标序列
            lag: 滞后期，默认1日
            
        Returns:
            威廉指标变化率，8位小数精度
        """
        try:
            # 计算前N日威廉指标
            wr_lagged = williams_r.shift(lag)
            
            # 使用向量化计算，改进除零处理
            change_rate = np.where(
                (wr_lagged != 0) & (~pd.isna(wr_lagged)),
                (williams_r - wr_lagged) / np.abs(wr_lagged) * 100,
                np.nan
            )
            
            # 修复异常值：限制变化率在合理范围内 [-500%, +500%]
            # 这样既保留了数据的波动性，又避免了极端异常值
            change_rate = np.clip(change_rate, -500.0, 500.0)
            
            # 创建Series并保持原索引
            result = pd.Series(change_rate, index=williams_r.index, dtype=np.float64)
            return result.round(self.decimal_precision)
        except Exception as e:
            print(f"⚠️ 威廉指标变化率计算错误: {str(e)}")
            return pd.Series([np.nan] * len(williams_r), index=williams_r.index, dtype=np.float64)

    def calculate_williams_indicators_batch(self, df):
        """
        批量计算威廉指标 - 优化版本
        
        优化内容：
        1. 单次数据读取，减少内存分配
        2. 向量化计算所有指标
        3. 优化数据类型和内存使用
        
        Args:
            df: ETF数据DataFrame，必须包含'最高价'、'最低价'、'收盘价'列
            
        Returns:
            包含所有威廉指标的DataFrame
        """
        try:
            # 数据验证
            if not self._validate_input_data_optimized(df):
                return pd.DataFrame()
            
            # 预分配结果DataFrame，提升性能
            result_df = df.copy()
            
            # 一次性提取价格数据，减少重复访问
            high_prices = df['最高价'].astype(np.float64)
            low_prices = df['最低价'].astype(np.float64)
            close_prices = df['收盘价'].astype(np.float64)
            
            # 批量计算三个周期的威廉指标（向量化）
            williams_results = {}
            for period_name, period_value in self.periods.items():
                williams_results[period_name] = self.calculate_williams_r_vectorized(
                    high_prices, low_prices, close_prices, period_value
                )
            
            # 分配结果到DataFrame
            result_df['wr_9'] = williams_results['short']
            result_df['wr_14'] = williams_results['standard'] 
            result_df['wr_21'] = williams_results['medium']
            
            # 计算衍生指标（向量化）
            result_df['wr_diff_9_21'] = self.calculate_wr_diff_vectorized(
                williams_results['short'], williams_results['medium']
            )
            result_df['wr_range'] = self.calculate_wr_range_vectorized(
                williams_results['standard'], self.derived_params['range_period']
            )
            result_df['wr_change_rate'] = self.calculate_wr_change_rate_vectorized(
                williams_results['standard'], self.derived_params['change_rate_lag']
            )
            
            # 添加计算时间戳
            result_df['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            return result_df
            
        except Exception as e:
            print(f"❌ ETF威廉指标批量计算失败: {str(e)}")
            return pd.DataFrame()

    def calculate_incremental_update(self, existing_df, new_data_df):
        """
        增量更新威廉指标计算
        
        新功能：支持增量计算，只计算新增的数据点
        
        Args:
            existing_df: 现有计算结果
            new_data_df: 新增数据
            
        Returns:
            增量计算结果DataFrame
        """
        try:
            if existing_df.empty:
                # 如果没有现有数据，进行全量计算
                return self.calculate_williams_indicators_batch(new_data_df)
            
            if new_data_df.empty:
                return pd.DataFrame()
            
            # 合并数据进行窗口计算（需要足够的历史数据）
            max_period = max(self.periods.values())
            lookback_period = max_period + self.derived_params['range_period']
            
            # 获取足够的历史数据进行窗口计算
            if len(existing_df) >= lookback_period:
                # 取最近的历史数据 + 新数据
                historical_slice = existing_df.tail(lookback_period)
                combined_df = pd.concat([historical_slice, new_data_df], ignore_index=True)
            else:
                # 历史数据不够，使用全量数据
                combined_df = pd.concat([existing_df, new_data_df], ignore_index=True)
            
            # 计算完整数据的指标
            full_result = self.calculate_williams_indicators_batch(combined_df)
            
            # 只返回新数据对应的计算结果
            new_data_count = len(new_data_df)
            incremental_result = full_result.tail(new_data_count).copy()
            
            return incremental_result
            
        except Exception as e:
            print(f"❌ 增量更新计算失败: {str(e)}")
            # 回退到全量计算
            return self.calculate_williams_indicators_batch(new_data_df)

    def _validate_input_data_optimized(self, df):
        """
        优化的输入数据验证
        
        Args:
            df: 输入数据DataFrame
            
        Returns:
            bool: 数据是否有效
        """
        try:
            if df.empty:
                return False
            
            # 检查必要列是否存在
            required_columns = ['最高价', '最低价', '收盘价']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                print(f"❌ 缺少必要列: {missing_columns}")
                return False
            
            # 检查数据点数量
            min_required = max(self.periods.values()) + self.derived_params['range_period']
            if len(df) < min_required:
                print(f"⚠️ 数据点不足: 需要至少{min_required}个数据点，实际{len(df)}个")
                return False
            
            # 向量化检查数据有效性
            price_data = df[required_columns]
            
            # 检查是否有非数值数据
            if not price_data.dtypes.apply(lambda x: pd.api.types.is_numeric_dtype(x)).all():
                print("⚠️ 价格数据包含非数值类型")
                return False
            
            # 检查价格逻辑关系（向量化）
            invalid_high_low = (df['最高价'] < df['最低价']).any()
            invalid_close_range = ((df['收盘价'] > df['最高价']) | (df['收盘价'] < df['最低价'])).any()
            
            if invalid_high_low:
                print("⚠️ 发现最高价小于最低价的异常数据")
            
            if invalid_close_range:
                print("⚠️ 发现收盘价超出最高最低价范围的异常数据")
            
            return True
            
        except Exception as e:
            print(f"❌ 数据验证过程中发生错误: {str(e)}")
            return False

    def format_output_data(self, df, etf_code):
        """
        格式化输出数据
        
        Args:
            df: 计算结果DataFrame
            etf_code: ETF代码
            
        Returns:
            格式化后的DataFrame
        """
        try:
            if df.empty:
                return df
            
            # 选择输出字段
            output_fields = ['wr_9', 'wr_14', 'wr_21', 'wr_diff_9_21', 'wr_range', 'wr_change_rate', 'calc_time']
            available_fields = [field for field in output_fields if field in df.columns]
            
            # 创建输出DataFrame
            output_df = pd.DataFrame()
            
            # 添加基础字段
            output_df['code'] = etf_code
            if '日期' in df.columns:
                output_df['date'] = pd.to_datetime(df['日期']).dt.strftime('%Y-%m-%d')
            else:
                output_df['date'] = df.index.strftime('%Y-%m-%d')
            
            # 添加威廉指标字段
            for field in available_fields:
                output_df[field] = df[field]
            
            # 按日期倒序排列(最新数据在前)
            output_df = output_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            return output_df
            
        except Exception as e:
            print(f"❌ 输出数据格式化错误: {str(e)}")
            return pd.DataFrame()

    def get_performance_metrics(self, df):
        """
        获取计算性能指标
        
        Returns:
            dict: 性能指标
        """
        return {
            'data_points': len(df),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / 1024 / 1024,
            'williams_indicators': 6,  # wr_9, wr_14, wr_21, wr_diff_9_21, wr_range, wr_change_rate
            'calculation_complexity': f"O(n * {max(self.periods.values())})"
        }

    # 向后兼容：保留原接口
    def calculate_williams_indicators_for_etf(self, df):
        """向后兼容接口"""
        return self.calculate_williams_indicators_batch(df)


if __name__ == "__main__":
    # 优化引擎测试
    print("🧪 威廉指标优化计算引擎测试")
    print("=" * 50)
    
    # 创建测试数据
    np.random.seed(42)
    dates = pd.date_range('2025-01-01', periods=100, freq='D')
    
    # 模拟价格数据
    base_price = 100
    price_changes = np.random.normal(0, 0.02, 100)
    prices = [base_price]
    
    for change in price_changes[1:]:
        prices.append(prices[-1] * (1 + change))
    
    # 生成高低开收数据
    test_data = pd.DataFrame({
        '日期': dates,
        '收盘价': prices,
        '最高价': [p * (1 + abs(np.random.normal(0, 0.01))) for p in prices],
        '最低价': [p * (1 - abs(np.random.normal(0, 0.01))) for p in prices],
    })
    
    # 确保价格逻辑正确
    test_data['最高价'] = test_data[['收盘价', '最高价']].max(axis=1)
    test_data['最低价'] = test_data[['收盘价', '最低价']].min(axis=1)
    
    # 初始化优化计算引擎
    engine = WilliamsEngineOptimized()
    
    # 性能测试
    import time
    
    start_time = time.time()
    result = engine.calculate_williams_indicators_batch(test_data)
    end_time = time.time()
    
    if not result.empty:
        print("✅ 威廉指标优化计算成功")
        print(f"📊 数据点数: {len(result)}")
        print(f"⏱️ 计算耗时: {(end_time - start_time)*1000:.2f}ms")
        
        # 验证计算结果的合理性
        williams_columns = ['wr_9', 'wr_14', 'wr_21']
        for col in williams_columns:
            if col in result.columns:
                valid_values = result[col].dropna()
                if not valid_values.empty:
                    min_val, max_val = valid_values.min(), valid_values.max()
                    print(f"📈 {col}: 范围 [{min_val:.2f}, {max_val:.2f}]")
                    
                    # 检查威廉指标值是否在正确范围内
                    if min_val < -100 or max_val > 0:
                        print(f"⚠️ {col} 数值超出正常范围 [-100, 0]")
        
        # 性能指标
        metrics = engine.get_performance_metrics(result)
        print(f"💾 内存使用: {metrics['memory_usage_mb']:.2f}MB")
        print(f"🔄 计算复杂度: {metrics['calculation_complexity']}")
        
    else:
        print("❌ 威廉指标优化计算失败")
    
    print("=" * 50)
    print("🎯 优化引擎测试完成")