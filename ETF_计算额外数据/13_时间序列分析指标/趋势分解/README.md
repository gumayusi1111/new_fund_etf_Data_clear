# 趋势分解指标

## 📊 指标概述

趋势分解是将时间序列分解为趋势、季节性、周期性和随机成分的技术，通过分离不同的成分来更好地理解ETF价格的内在结构。趋势分解有助于识别长期方向、消除噪音干扰，为投资决策提供更清晰的信号。

## 🔧 核心分解方法

### 1. 线性趋势分解
**最简单的趋势分析方法**
```python
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

def linear_trend_decomposition(price_series):
    """
    线性趋势分解
    """
    # 准备数据
    x = np.arange(len(price_series)).reshape(-1, 1)
    y = price_series.values
    
    # 线性回归拟合
    model = LinearRegression()
    model.fit(x, y)
    
    # 计算趋势成分
    trend = model.predict(x)
    
    # 计算残差（去趋势后的序列）
    detrended = y - trend
    
    # 趋势强度
    r_squared = model.score(x, y)
    
    return {
        'trend': pd.Series(trend, index=price_series.index),
        'detrended': pd.Series(detrended, index=price_series.index),
        'slope': model.coef_[0],
        'trend_strength': r_squared
    }
```

### 2. HP滤波分解 (Hodrick-Prescott Filter)
**经典的趋势-周期分解方法**
```python
from statsmodels.tsa.filters.hp_filter import hpfilter

def hp_filter_decomposition(price_series, lamb=1600):
    """
    HP滤波趋势分解
    
    参数:
    lamb: 平滑参数，越大趋势越平滑
    - 日数据: 1600
    - 月数据: 14400
    - 季度数据: 1600
    """
    # HP滤波
    cycle, trend = hpfilter(price_series, lamb=lamb)
    
    # 计算趋势变化率
    trend_change = trend.pct_change()
    
    # 周期性成分的波动性
    cycle_volatility = cycle.std()
    
    return {
        'trend': trend,
        'cycle': cycle,
        'trend_change': trend_change,
        'cycle_volatility': cycle_volatility
    }
```

### 3. 移动平均趋势分解
**灵活的趋势分离方法**
```python
def moving_average_decomposition(price_series, windows=[5, 20, 60]):
    """
    多重移动平均趋势分解
    """
    components = {}
    
    for window in windows:
        ma = price_series.rolling(window=window).mean()
        components[f'MA_{window}'] = ma
    
    # 长期趋势 (60日均线)
    long_trend = components['MA_60']
    
    # 中期趋势 (20日均线相对于60日均线)
    medium_trend = components['MA_20'] - components['MA_60']
    
    # 短期波动 (价格相对于20日均线)
    short_fluctuation = price_series - components['MA_20']
    
    # 噪音 (价格相对于5日均线)
    noise = price_series - components['MA_5']
    
    return {
        'long_trend': long_trend,
        'medium_trend': medium_trend,
        'short_fluctuation': short_fluctuation,
        'noise': noise,
        'all_ma': components
    }
```

### 4. STL分解 (Seasonal and Trend decomposition using Loess)
**处理季节性的高级方法**
```python
from statsmodels.tsa.seasonal import STL

def stl_decomposition(price_series, seasonal=7, robust=True):
    """
    STL季节性趋势分解
    
    参数:
    seasonal: 季节性周期长度
    robust: 是否使用鲁棒估计
    """
    # STL分解
    stl = STL(price_series, seasonal=seasonal, robust=robust)
    result = stl.fit()
    
    # 提取成分
    trend = result.trend
    seasonal = result.seasonal
    residual = result.resid
    
    # 计算各成分的相对重要性
    total_var = price_series.var()
    trend_importance = trend.var() / total_var
    seasonal_importance = seasonal.var() / total_var
    residual_importance = residual.var() / total_var
    
    return {
        'trend': trend,
        'seasonal': seasonal,
        'residual': residual,
        'trend_importance': trend_importance,
        'seasonal_importance': seasonal_importance,
        'residual_importance': residual_importance
    }
```

## 📈 趋势变化点检测

### 1. 结构断点检测
```python
def structural_break_detection(price_series, min_size=30):
    """
    结构性断点检测
    """
    breaks = []
    n = len(price_series)
    
    for i in range(min_size, n - min_size):
        # 分割序列
        series1 = price_series.iloc[:i]
        series2 = price_series.iloc[i:]
        
        # 计算两段的均值差异
        mean1 = series1.mean()
        mean2 = series2.mean()
        
        # 计算t统计量
        var1 = series1.var()
        var2 = series2.var()
        pooled_var = ((len(series1)-1)*var1 + (len(series2)-1)*var2) / (n-2)
        
        t_stat = abs(mean1 - mean2) / np.sqrt(pooled_var * (1/len(series1) + 1/len(series2)))
        
        # 如果t统计量足够大，认为存在结构断点
        if t_stat > 2.5:  # 可调整阈值
            breaks.append({
                'index': i,
                'date': price_series.index[i],
                't_statistic': t_stat,
                'mean_before': mean1,
                'mean_after': mean2
            })
    
    return breaks
```

### 2. 趋势变化强度分析
```python
def trend_change_analysis(price_series, window=20):
    """
    趋势变化强度分析
    """
    # 计算滚动斜率
    rolling_slopes = []
    
    for i in range(window, len(price_series)):
        subset = price_series.iloc[i-window:i]
        x = np.arange(len(subset))
        slope = np.polyfit(x, subset.values, 1)[0]
        rolling_slopes.append(slope)
    
    rolling_slopes = pd.Series(rolling_slopes, 
                              index=price_series.index[window:])
    
    # 趋势变化点
    slope_changes = rolling_slopes.diff().abs()
    
    # 趋势加速/减速
    trend_acceleration = rolling_slopes.diff()
    
    return {
        'rolling_slopes': rolling_slopes,
        'slope_changes': slope_changes,
        'trend_acceleration': trend_acceleration,
        'avg_slope': rolling_slopes.mean(),
        'slope_volatility': rolling_slopes.std()
    }
```

## 🎯 交易策略应用

### 1. 趋势跟踪策略
```python
def trend_following_strategy(decomposition_result):
    """
    基于趋势分解的跟踪策略
    """
    trend = decomposition_result['trend']
    
    # 趋势方向
    trend_direction = np.sign(trend.diff())
    
    # 趋势强度
    trend_strength = abs(trend.diff()) / trend.abs()
    
    # 买入信号：趋势向上且强度较强
    buy_signal = (trend_direction > 0) & (trend_strength > trend_strength.quantile(0.7))
    
    # 卖出信号：趋势向下且强度较强
    sell_signal = (trend_direction < 0) & (trend_strength > trend_strength.quantile(0.7))
    
    # 持有信号：趋势不明确
    hold_signal = trend_strength <= trend_strength.quantile(0.3)
    
    return {
        'buy_signals': buy_signal,
        'sell_signals': sell_signal,
        'hold_signals': hold_signal,
        'trend_strength': trend_strength
    }
```

### 2. 均值回归策略
```python
def mean_reversion_strategy(decomposition_result, threshold=2):
    """
    基于周期成分的均值回归策略
    """
    if 'cycle' in decomposition_result:
        cycle = decomposition_result['cycle']
    else:
        cycle = decomposition_result['short_fluctuation']
    
    # 标准化周期成分
    cycle_std = cycle.std()
    cycle_normalized = cycle / cycle_std
    
    # 极端偏离买入信号
    buy_signal = cycle_normalized < -threshold
    
    # 极端偏离卖出信号
    sell_signal = cycle_normalized > threshold
    
    # 回归中性信号
    neutral_signal = abs(cycle_normalized) < 0.5
    
    return {
        'buy_signals': buy_signal,
        'sell_signals': sell_signal,
        'neutral_signals': neutral_signal,
        'cycle_normalized': cycle_normalized
    }
```

## 📊 实际应用案例

### ETF趋势分解分析
```python
def comprehensive_trend_analysis(df, etf_name):
    """
    ETF综合趋势分析
    """
    close = df['close']
    
    # 多种分解方法
    linear_result = linear_trend_decomposition(close)
    hp_result = hp_filter_decomposition(close)
    ma_result = moving_average_decomposition(close)
    
    # 综合分析
    analysis = {
        'ETF名称': etf_name,
        '线性趋势斜率': round(linear_result['slope'], 4),
        '趋势强度(R²)': round(linear_result['trend_strength'], 3),
        'HP趋势方向': '上升' if hp_result['trend'].iloc[-1] > hp_result['trend'].iloc[-20] else '下降',
        '周期波动率': round(hp_result['cycle_volatility'], 3),
        '当前趋势位置': '上轨' if ma_result['short_fluctuation'].iloc[-1] > 0 else '下轨'
    }
    
    return analysis
```

### 趋势质量评估
```python
def trend_quality_assessment(decomposition_results):
    """
    趋势质量评估
    """
    trend = decomposition_results['trend']
    
    # 趋势一致性
    trend_direction_changes = (np.sign(trend.diff()) != np.sign(trend.diff().shift(1))).sum()
    consistency_score = 1 - (trend_direction_changes / len(trend))
    
    # 趋势平滑度
    trend_volatility = trend.diff().std()
    smoothness_score = 1 / (1 + trend_volatility)
    
    # 趋势显著性
    if 'trend_strength' in decomposition_results:
        significance_score = decomposition_results['trend_strength']
    else:
        significance_score = abs(trend.corr(pd.Series(range(len(trend)))))
    
    # 综合质量评分
    quality_score = (consistency_score + smoothness_score + significance_score) / 3
    
    return {
        'consistency_score': consistency_score,
        'smoothness_score': smoothness_score,
        'significance_score': significance_score,
        'overall_quality': quality_score,
        'quality_grade': 'A' if quality_score > 0.8 else 'B' if quality_score > 0.6 else 'C'
    }
```

## ⚠️ 使用注意事项

### 方法选择指南
- **线性趋势**: 适合短期分析，计算简单
- **HP滤波**: 适合中长期分析，处理周期性较好
- **移动平均**: 适合实时分析，滞后性较小
- **STL分解**: 适合有明显季节性的数据

### 参数优化
```python
def parameter_optimization(price_series):
    """
    分解参数优化建议
    """
    data_length = len(price_series)
    
    # HP滤波参数建议
    if data_length < 100:
        hp_lambda = 400  # 短期数据用较小参数
    elif data_length < 500:
        hp_lambda = 1600  # 标准参数
    else:
        hp_lambda = 6400  # 长期数据用较大参数
    
    # 移动平均窗口建议
    short_window = max(5, data_length // 50)
    medium_window = max(20, data_length // 20)
    long_window = max(60, data_length // 10)
    
    return {
        'hp_lambda': hp_lambda,
        'ma_windows': [short_window, medium_window, long_window],
        'stl_seasonal': 7 if data_length > 50 else max(3, data_length // 10)
    }
```

### 结果验证
```python
def decomposition_validation(original_series, decomposed_components):
    """
    分解结果验证
    """
    # 重构原序列
    reconstructed = sum(decomposed_components.values())
    
    # 计算重构误差
    reconstruction_error = np.mean((original_series - reconstructed) ** 2)
    
    # 各成分方差占比
    total_variance = original_series.var()
    component_contributions = {
        name: component.var() / total_variance 
        for name, component in decomposed_components.items()
    }
    
    return {
        'reconstruction_error': reconstruction_error,
        'component_contributions': component_contributions,
        'total_explained_variance': sum(component_contributions.values())
    }
```

趋势分解是理解ETF价格结构的重要工具，通过分离不同成分可以更准确地识别投资机会和风险！ 