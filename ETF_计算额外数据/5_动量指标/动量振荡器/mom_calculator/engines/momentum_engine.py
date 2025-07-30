"""
动量振荡器计算引擎 - 核心计算逻辑
=================================

实现13个核心动量指标的向量化计算
基于"动量先于价格"理论，针对中国ETF市场优化

核心算法:
- 基础动量指标 (momentum_10, momentum_20)
- ROC变动率指标 (roc_5, roc_12, roc_25)
- PMO价格动量振荡器 (pmo, pmo_signal)
- 威廉指标 (williams_r)
- 综合动量指标 (5个复合指标)

技术特点:
- NumPy向量化计算，高性能
- 8位小数精度
- 滑动窗口优化
- 内存友好设计
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Any
import logging
from datetime import datetime
import warnings

from ..infrastructure.config import MomentumConfig

warnings.filterwarnings('ignore', category=pd.errors.PerformanceWarning)

class MomentumEngine:
    """动量振荡器计算引擎"""
    
    def __init__(self, precision: Optional[int] = None):
        """
        初始化动量振荡器计算引擎
        
        Args:
            precision: 计算精度(小数位数)，None则使用配置默认值
        """
        self.config = MomentumConfig.MOMENTUM_CONFIG
        self.precision = precision or self.config['precision']
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 从配置获取计算参数
        self.momentum_periods = self.config['momentum_periods']
        self.roc_periods = self.config['roc_periods']
        self.pmo_config = self.config['pmo_config']
        self.williams_period = self.config['williams_period']
        self.composite_config = self.config['composite_config']
        self.min_data_points = self.config['min_data_points']
        
        # 数值计算常量
        self.epsilon = MomentumConfig.CONSTANTS['EPSILON']
        
        self.logger.info(f"动量引擎初始化完成 - 精度:{self.precision}位小数")
    
    def calculate_momentum_indicators(self, data: pd.DataFrame, etf_code: str) -> Dict[str, Any]:
        """
        计算完整的动量振荡器指标
        
        Args:
            data: 包含OHLC数据的DataFrame
            etf_code: ETF代码
            
        Returns:
            包含计算结果和统计信息的字典
        """
        try:
            start_time = datetime.now()
            
            # 数据预处理和验证
            processed_data = self._preprocess_data(data)
            if processed_data is None:
                return {'success': False, 'error': '数据预处理失败'}
            
            # 检查数据量
            if len(processed_data) < self.min_data_points:
                return {
                    'success': False, 
                    'error': f'数据量不足，需要至少{self.min_data_points}个交易日'
                }
            
            # 创建结果DataFrame
            result_df = processed_data[['date']].copy()
            
            # 1. 基础动量指标组 (2个)
            momentum_indicators = self._calculate_basic_momentum(processed_data)
            result_df = result_df.join(momentum_indicators)
            
            # 2. 变动率指标组 (3个) 
            roc_indicators = self._calculate_roc_indicators(processed_data)
            result_df = result_df.join(roc_indicators)
            
            # 3. PMO振荡器组 (3个)
            pmo_indicators = self._calculate_pmo_indicators(processed_data)
            result_df = result_df.join(pmo_indicators)
            
            # 4. 综合指标组 (5个)
            composite_indicators = self._calculate_composite_indicators(
                processed_data, roc_indicators, momentum_indicators)
            result_df = result_df.join(composite_indicators)
            
            # 添加ETF代码和计算时间
            result_df['code'] = etf_code
            result_df['calc_time'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # 重新排列列顺序
            result_df = result_df[MomentumConfig.OUTPUT_FIELDS]
            
            # 确保最新数据在最上面（按日期降序排序）
            result_df = result_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            # 数值精度处理
            result_df = self._apply_precision(result_df)
            
            # 计算统计信息
            processing_time = (datetime.now() - start_time).total_seconds()
            stats = self._calculate_statistics(result_df, processing_time)
            
            return {
                'success': True,
                'data': result_df,
                'statistics': stats,
                'processing_time': processing_time,
                'data_points': len(result_df)
            }
            
        except ValueError as e:
            self.logger.error(f"动量指标计算数据错误 {etf_code}: {str(e)}")
            return {'success': False, 'error': f'数据错误: {str(e)}'}
        except KeyError as e:
            self.logger.error(f"动量指标计算缺少必需字段 {etf_code}: {str(e)}")
            return {'success': False, 'error': f'缺少字段: {str(e)}'}
        except MemoryError as e:
            self.logger.error(f"动量指标计算内存不足 {etf_code}: {str(e)}")
            return {'success': False, 'error': '内存不足，请减少数据量或检查系统资源'}
        except Exception as e:
            self.logger.error(f"动量指标计算异常 {etf_code}: {type(e).__name__}: {str(e)}")
            return {'success': False, 'error': f'未知错误: {type(e).__name__}: {str(e)}'}
    
    def _preprocess_data(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """数据预处理和验证"""
        try:
            # 检查必要的列
            required_columns = ['date', 'close', 'high', 'low']
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                self.logger.error(f"缺少必要列: {missing_columns}")
                return None
            
            # 数据类型转换
            df = df.copy()
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
            df['close'] = pd.to_numeric(df['close'], errors='coerce')
            df['high'] = pd.to_numeric(df['high'], errors='coerce')
            df['low'] = pd.to_numeric(df['low'], errors='coerce')
            
            # 按日期排序并去重
            df = df.sort_values('date').drop_duplicates(subset=['date']).reset_index(drop=True)
            
            # 处理异常值
            df = self._handle_outliers(df)
            
            # 基本清理
            df = df.dropna(subset=required_columns)
            
            return df
            
        except Exception as e:
            self.logger.error(f"数据预处理失败: {str(e)}")
            return None
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """处理异常值（涨跌停、停牌等）"""
        try:
            # 标记涨跌停
            daily_return = df['close'].pct_change()
            limit_threshold = MomentumConfig.ANOMALY_CONFIG['limit_move_threshold']
            
            limit_up = daily_return > limit_threshold
            limit_down = daily_return < -limit_threshold
            
            df['is_limit_move'] = limit_up | limit_down
            
            # 标记停牌（价格不变）
            df['is_suspended'] = daily_return == 0
            
            return df
            
        except Exception as e:
            self.logger.error(f"异常值处理失败: {str(e)}")
            return df
    
    def _calculate_basic_momentum(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算基础动量指标
        
        公式: Momentum = Close[t] - Close[t-n]
        """
        result = pd.DataFrame(index=df.index)
        
        try:
            # 10日动量
            result['momentum_10'] = (df['close'] - df['close'].shift(10))
            
            # 20日动量  
            result['momentum_20'] = (df['close'] - df['close'].shift(20))
            
            return result
            
        except Exception as e:
            self.logger.error(f"基础动量计算失败: {str(e)}")
            return pd.DataFrame(index=df.index)
    
    def _calculate_roc_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算变动率指标 (Rate of Change)
        
        公式: ROC = ((Close[t] - Close[t-n]) / Close[t-n]) * 100
        """
        result = pd.DataFrame(index=df.index)
        
        try:
            for period in self.roc_periods:
                col_name = f'roc_{period}'
                close_shift = df['close'].shift(period)
                # 防止除零
                close_shift_safe = close_shift.replace(0, np.nan)
                result[col_name] = ((df['close'] - close_shift) / close_shift_safe * 100)
            
            return result
            
        except Exception as e:
            self.logger.error(f"ROC指标计算失败: {str(e)}")
            return pd.DataFrame(index=df.index)
    
    def _calculate_pmo_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算价格动量振荡器 (Price Momentum Oscillator)
        
        PMO使用DecisionPoint方法：双重EMA平滑的ROC
        """
        result = pd.DataFrame(index=df.index)
        
        try:
            pmo_config = self.pmo_config
            
            # 计算10日ROC并放大1000倍（标准PMO做法）
            close_shift_10 = df['close'].shift(pmo_config['roc_period'])
            close_shift_safe = close_shift_10.replace(0, np.nan)
            roc_10 = ((df['close'] - close_shift_10) / close_shift_safe * pmo_config['scale_factor'])
            
            # 第一次EMA平滑 (35日)
            ema1 = roc_10.ewm(span=pmo_config['ema1_period'], adjust=False).mean()
            
            # 第二次EMA平滑 (20日) = PMO主线
            result['pmo'] = ema1.ewm(span=pmo_config['ema2_period'], adjust=False).mean()
            
            # PMO信号线 (9日EMA)
            result['pmo_signal'] = result['pmo'].ewm(span=pmo_config['signal_period'], adjust=False).mean()
            
            # Williams %R指标 (确保范围在-100到0之间)
            highest_n = df['high'].rolling(window=self.williams_period).max()
            lowest_n = df['low'].rolling(window=self.williams_period).min()
            range_hl = highest_n - lowest_n
            # 防止除零
            range_hl_safe = range_hl.replace(0, np.nan)
            wr_raw = (((highest_n - df['close']) / range_hl_safe) * (-100))
            # 处理边界值 - 保持NaN值，不用0填充
            williams_min = MomentumConfig.CONSTANTS['WILLIAMS_R_MIN']
            williams_max = MomentumConfig.CONSTANTS['WILLIAMS_R_MAX']
            result['williams_r'] = wr_raw.clip(williams_min, williams_max)
            
            return result
            
        except Exception as e:
            self.logger.error(f"PMO指标计算失败: {str(e)}")
            return pd.DataFrame(index=df.index)
    
    def _calculate_composite_indicators(self, 
                                      df: pd.DataFrame,
                                      roc_indicators: pd.DataFrame,
                                      momentum_indicators: pd.DataFrame) -> pd.DataFrame:
        """
        计算综合动量指标
        """
        result = pd.DataFrame(index=df.index)
        
        try:
            # 计算20日ROC（用于强度计算）
            close_shift_20 = df['close'].shift(20)
            close_shift_safe = close_shift_20.replace(0, np.nan)
            roc_20 = ((df['close'] - close_shift_20) / close_shift_safe * 100)
            
            # 1. 动量强度 = |ROC_20|
            result['momentum_strength'] = abs(roc_20)
            
            # 2. 动量加速度 = Momentum_10 - Momentum_20
            result['momentum_acceleration'] = (
                momentum_indicators['momentum_10'] - momentum_indicators['momentum_20']
            )
            
            # 3. 动量波动率 (20日ROC的10日标准差)
            volatility_window = self.composite_config['volatility_window']
            result['momentum_volatility'] = roc_20.rolling(window=volatility_window).std()
            
            return result
            
        except Exception as e:
            self.logger.error(f"综合指标计算失败: {str(e)}")
            return pd.DataFrame(index=df.index)
    
    def _apply_precision(self, df: pd.DataFrame) -> pd.DataFrame:
        """应用数值精度 - 严格8位小数"""
        try:
            # 所有连续数值指标都要严格8位小数
            continuous_columns = [
                'momentum_10', 'momentum_20', 'roc_5', 'roc_12', 'roc_25',
                'pmo', 'pmo_signal', 'williams_r', 'momentum_strength', 
                'momentum_acceleration', 'momentum_volatility'
            ]
            
            # 强制应用8位小数精度到所有连续数值列
            for col in continuous_columns:
                if col in df.columns:
                    # 转换为浮点数并强制8位精度
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    df[col] = df[col].apply(lambda x: round(float(x), 8) if pd.notna(x) else x)
            
            return df
            
        except Exception as e:
            self.logger.error(f"精度应用失败: {str(e)}")
            return df
    
    def _calculate_statistics(self, result_df: pd.DataFrame, processing_time: float) -> Dict[str, Any]:
        """计算统计信息"""
        try:
            stats = {
                'record_count': len(result_df),
                'processing_time_seconds': round(processing_time, 4),
                'indicators_calculated': 11,  # 移除2个主观字段后
                'data_quality': {
                    'completeness': {},
                    'value_ranges': {}
                }
            }
            
            # 计算完整性统计
            numeric_columns = [
                'momentum_10', 'momentum_20', 'roc_5', 'roc_12', 'roc_25',
                'pmo', 'pmo_signal', 'williams_r', 'momentum_strength', 
                'momentum_acceleration', 'momentum_volatility'
            ]
            
            for col in numeric_columns:
                if col in result_df.columns:
                    valid_count = result_df[col].notna().sum()
                    total_count = len(result_df)
                    completeness = (valid_count / total_count * 100) if total_count > 0 else 0
                    
                    stats['data_quality']['completeness'][col] = round(completeness, 2)
                    
                    # 值域统计
                    if valid_count > 0:
                        stats['data_quality']['value_ranges'][col] = {
                            'min': float(result_df[col].min()),
                            'max': float(result_df[col].max()),
                            'mean': float(result_df[col].mean())
                        }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"统计信息计算失败: {str(e)}")
            return {'error': str(e)}
    
    def get_engine_info(self) -> Dict[str, Any]:
        """获取引擎信息"""
        return {
            'name': '动量振荡器计算引擎',
            'version': '2.0.0',
            'precision': self.precision,
            'indicators_count': 11,  # 纯客观指标数量
            'min_data_points': self.min_data_points,
            'parameters': {
                'momentum_periods': self.momentum_periods,
                'roc_periods': self.roc_periods,
                'pmo_config': self.pmo_config,
                'williams_period': self.williams_period,
                'composite_config': self.composite_config
            }
        }