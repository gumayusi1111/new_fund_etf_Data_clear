# 周期性检测指标

## 📊 指标概述

周期性检测是时间序列分析的重要组成部分，通过识别ETF价格数据中的周期性模式，帮助投资者发现规律性的价格波动并预测未来趋势。周期性分析能够揭示市场的内在节律，为择时投资提供科学依据。

## 🔍 核心检测方法

### 1. 傅里叶分析 (Fourier Analysis)
**原理**: 将价格时间序列分解为不同频率的正弦波组合
```python
import numpy as np
from scipy.fft import fft, fftfreq

def fourier_analysis(price_series, sampling_rate=1):
    """
    傅里叶频谱分析
    """
    # 去除趋势
    detrended = price_series - price_series.rolling(50).mean()
    detrended = detrended.dropna()
    
    # FFT变换
    fft_values = fft(detrended.values)
    frequencies = fftfreq(len(detrended), d=sampling_rate)
    
    # 功率谱
    power_spectrum = np.abs(fft_values) ** 2
    
    # 找到主要周期
    positive_freq_idx = frequencies > 0
    dominant_freq = frequencies[positive_freq_idx][np.argmax(power_spectrum[positive_freq_idx])]
    dominant_period = 1 / dominant_freq if dominant_freq > 0 else np.inf
    
    return {
        'dominant_period': dominant_period,
        'frequencies': frequencies[positive_freq_idx],
        'power_spectrum': power_spectrum[positive_freq_idx]
    }
```

### 2. 自相关分析 (Autocorrelation Analysis)
**用途**: 发现时间序列中的重复模式
```python
def autocorrelation_analysis(price_series, max_lags=250):
    """
    自相关周期检测
    """
    returns = price_series.pct_change().dropna()
    
    # 计算自相关系数
    autocorr = [returns.autocorr(lag=i) for i in range(1, max_lags+1)]
    autocorr_series = pd.Series(autocorr, index=range(1, max_lags+1))
    
    # 寻找显著的正自相关峰值
    threshold = 2 / np.sqrt(len(returns))  # 95%置信区间
    significant_lags = autocorr_series[autocorr_series > threshold]
    
    return {
        'autocorr_values': autocorr_series,
        'significant_lags': significant_lags,
        'potential_cycles': significant_lags.index.tolist()
    }
```

### 3. 小波分析 (Wavelet Analysis)
**优势**: 能同时分析时域和频域特征
```python
import pywt

def wavelet_analysis(price_series, wavelet='morlet', scales=None):
    """
    小波分析检测周期
    """
    if scales is None:
        scales = np.arange(1, 128)
    
    # 小波变换
    coefficients, frequencies = pywt.cwt(price_series.values, scales, wavelet)
    
    # 计算功率
    power = np.abs(coefficients) ** 2
    
    # 时间平均功率谱
    global_power = np.mean(power, axis=1)
    
    # 找到主导周期
    dominant_scale_idx = np.argmax(global_power)
    dominant_period = scales[dominant_scale_idx]
    
    return {
        'coefficients': coefficients,
        'scales': scales,
        'global_power': global_power,
        'dominant_period': dominant_period
    }
```

## 📈 周期识别策略

### 1. 多周期组合分析
```python
def multi_cycle_analysis(df):
    """
    多周期综合分析
    """
    close = df['close']
    
    # 短期周期 (5-20天)
    short_cycles = []
    for period in range(5, 21):
        ma_short = close.rolling(period).mean()
        correlation = close.corr(ma_short.shift(period))
        if correlation > 0.7:
            short_cycles.append(period)
    
    # 中期周期 (20-60天)
    medium_cycles = []
    for period in range(20, 61):
        ma_medium = close.rolling(period).mean()
        correlation = close.corr(ma_medium.shift(period))
        if correlation > 0.6:
            medium_cycles.append(period)
    
    # 长期周期 (60-250天)
    long_cycles = []
    for period in range(60, 251):
        ma_long = close.rolling(period).mean()
        correlation = close.corr(ma_long.shift(period))
        if correlation > 0.5:
            long_cycles.append(period)
    
    return {
        'short_cycles': short_cycles,
        'medium_cycles': medium_cycles,
        'long_cycles': long_cycles
    }
```

### 2. 周期强度评估
```python
def cycle_strength_assessment(price_series, detected_cycles):
    """
    评估检测到的周期的强度
    """
    cycle_strengths = {}
    
    for cycle in detected_cycles:
        # 计算周期性移动平均
        cyclic_ma = price_series.rolling(cycle).mean()
        
        # 计算周期偏离度
        deviations = abs(price_series - cyclic_ma) / cyclic_ma
        avg_deviation = deviations.mean()
        
        # 计算周期一致性
        cyclic_returns = price_series.pct_change(cycle)
        consistency = len(cyclic_returns[cyclic_returns > 0]) / len(cyclic_returns.dropna())
        
        # 综合强度评分
        strength_score = (1 - avg_deviation) * consistency
        
        cycle_strengths[cycle] = {
            'strength_score': strength_score,
            'average_deviation': avg_deviation,
            'consistency': consistency
        }
    
    return cycle_strengths
```

## 🎯 交易应用

### 1. 周期性择时策略
```python
def cyclical_timing_strategy(df, dominant_cycle):
    """
    基于主导周期的择时策略
    """
    close = df['close']
    
    # 计算周期性信号
    cycle_ma = close.rolling(dominant_cycle).mean()
    cycle_position = (close - cycle_ma) / cycle_ma
    
    # 周期底部买入信号
    cycle_bottom = (cycle_position < -0.05) & (cycle_position.shift(1) >= -0.05)
    
    # 周期顶部卖出信号  
    cycle_top = (cycle_position > 0.05) & (cycle_position.shift(1) <= 0.05)
    
    # 周期中性区域
    cycle_neutral = abs(cycle_position) <= 0.02
    
    return {
        'buy_signals': cycle_bottom,
        'sell_signals': cycle_top,
        'neutral_zones': cycle_neutral,
        'cycle_position': cycle_position
    }
```

### 2. 多周期叠加策略
```python
def multi_cycle_overlay_strategy(df, cycles_dict):
    """
    多周期叠加交易策略
    """
    close = df['close']
    signals = pd.DataFrame(index=df.index)
    
    # 为每个周期计算信号
    for cycle_type, cycles in cycles_dict.items():
        if cycles:
            avg_cycle = int(np.mean(cycles))
            cycle_ma = close.rolling(avg_cycle).mean()
            
            # 周期趋势信号
            signals[f'{cycle_type}_trend'] = close > cycle_ma
            
            # 周期动量信号
            signals[f'{cycle_type}_momentum'] = cycle_ma > cycle_ma.shift(5)
    
    # 综合信号
    long_signal = signals.filter(like='_trend').all(axis=1) & \
                 signals.filter(like='_momentum').all(axis=1)
    
    short_signal = (~signals.filter(like='_trend')).all(axis=1) & \
                  (~signals.filter(like='_momentum')).all(axis=1)
    
    return long_signal, short_signal
```

## 📊 实际应用案例

### ETF周期性特征分析
```python
def etf_cyclical_characteristics(df, etf_name):
    """
    ETF周期性特征分析
    """
    close = df['close']
    
    # 傅里叶分析
    fourier_result = fourier_analysis(close)
    
    # 自相关分析
    autocorr_result = autocorrelation_analysis(close)
    
    # 多周期分析
    cycles = multi_cycle_analysis(df)
    
    analysis = {
        'ETF名称': etf_name,
        '主导周期(傅里叶)': round(fourier_result['dominant_period'], 1),
        '显著自相关周期': autocorr_result['potential_cycles'][:3],
        '短期周期': cycles['short_cycles'],
        '中期周期': cycles['medium_cycles'],
        '长期周期': cycles['long_cycles']
    }
    
    return analysis
```

## ⚠️ 使用注意事项

### 数据要求
- **样本量**: 至少需要待检测周期长度3倍以上的数据
- **数据质量**: 缺失值和异常值会严重影响周期检测
- **去趋势**: 长期趋势会掩盖周期性特征

### 检测局限性
- **伪周期**: 随机噪音可能产生虚假的周期信号
- **非平稳性**: 市场制度变化会改变周期特征
- **多重周期**: 实际市场往往存在多个重叠的周期

### 优化建议
```python
def cycle_detection_optimization(df):
    """
    周期检测优化建议
    """
    close = df['close']
    
    # 1. 数据预处理
    # 去除异常值
    q1, q3 = close.quantile([0.25, 0.75])
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    clean_data = close[(close >= lower_bound) & (close <= upper_bound)]
    
    # 2. 多方法验证
    methods_results = []
    
    # 傅里叶方法
    fourier_period = fourier_analysis(clean_data)['dominant_period']
    methods_results.append(fourier_period)
    
    # 自相关方法
    autocorr_cycles = autocorrelation_analysis(clean_data)['potential_cycles']
    if autocorr_cycles:
        methods_results.extend(autocorr_cycles[:2])
    
    # 3. 一致性检验
    consistent_cycles = []
    for cycle in set(methods_results):
        if methods_results.count(cycle) >= 2 or abs(cycle - np.mean(methods_results)) < 5:
            consistent_cycles.append(cycle)
    
    return consistent_cycles
```

周期性检测是发现市场内在规律的重要工具，结合多种分析方法能够提高检测的准确性和可靠性！ 