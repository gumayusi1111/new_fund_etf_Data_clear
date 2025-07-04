# 异常检测指标

## 概述
异常检测指标用于识别ETF价格或成交量中的异常事件，这些异常往往对应重要的市场信息、突发事件或交易机会。通过统计学方法自动识别偏离正常模式的数据点。

## 主要检测方法

### 1. Z-Score异常检测
```python
import pandas as pd
import numpy as np
from scipy import stats

def zscore_anomaly_detection(data, window=20, threshold=2.5):
    """
    基于Z-Score的异常检测
    
    Parameters:
    data: 数据序列
    window: 滚动窗口大小
    threshold: 异常阈值 (通常2-3)
    """
    # 计算滚动均值和标准差
    rolling_mean = data.rolling(window).mean()
    rolling_std = data.rolling(window).std()
    
    # 计算Z-Score
    z_scores = (data - rolling_mean) / rolling_std
    
    # 识别异常点
    anomalies = abs(z_scores) > threshold
    
    return {
        'z_scores': z_scores,
        'anomalies': anomalies,
        'anomaly_values': data[anomalies],
        'anomaly_dates': data.index[anomalies],
        'anomaly_count': anomalies.sum()
    }
```

### 2. IQR异常检测
```python
def iqr_anomaly_detection(data, window=20, k=1.5):
    """
    基于四分位数范围(IQR)的异常检测
    """
    def detect_outliers(series):
        Q1 = series.quantile(0.25)
        Q3 = series.quantile(0.75)
        IQR = Q3 - Q1
        
        lower_bound = Q1 - k * IQR
        upper_bound = Q3 + k * IQR
        
        return (series < lower_bound) | (series > upper_bound)
    
    # 滚动IQR异常检测
    anomalies = data.rolling(window).apply(
        lambda x: detect_outliers(x).iloc[-1] if len(x) == window else False
    ).astype(bool)
    
    return {
        'anomalies': anomalies,
        'anomaly_values': data[anomalies],
        'anomaly_dates': data.index[anomalies],
        'anomaly_count': anomalies.sum()
    }
```

### 3. 移动中位数绝对偏差(MAD)
```python
def mad_anomaly_detection(data, window=20, threshold=3):
    """
    基于移动中位数绝对偏差的异常检测
    """
    def calculate_mad(series):
        median = series.median()
        mad = np.median(np.abs(series - median))
        return mad
    
    rolling_median = data.rolling(window).median()
    rolling_mad = data.rolling(window).apply(calculate_mad)
    
    # 计算修正Z-Score
    modified_z_scores = 0.6745 * (data - rolling_median) / rolling_mad
    
    # 识别异常
    anomalies = abs(modified_z_scores) > threshold
    
    return {
        'modified_z_scores': modified_z_scores,
        'anomalies': anomalies,
        'anomaly_values': data[anomalies],
        'rolling_mad': rolling_mad
    }
```

## 价格异常检测

### 价格跳跃检测
```python
def price_jump_detection(price_data, threshold_pct=0.05):
    """
    检测价格异常跳跃
    
    Parameters:
    price_data: 价格序列
    threshold_pct: 跳跃阈值百分比
    """
    # 计算日收益率
    returns = price_data.pct_change()
    
    # 检测跳跃
    jumps_up = returns > threshold_pct
    jumps_down = returns < -threshold_pct
    
    all_jumps = jumps_up | jumps_down
    
    jump_analysis = []
    for date in returns.index[all_jumps]:
        jump_size = returns[date]
        jump_type = "向上跳跃" if jump_size > 0 else "向下跳跃"
        
        jump_analysis.append({
            'date': date,
            'jump_size': jump_size,
            'jump_percentage': jump_size * 100,
            'jump_type': jump_type,
            'price_before': price_data[date - pd.Timedelta(days=1)] if date != price_data.index[0] else None,
            'price_after': price_data[date]
        })
    
    return {
        'jumps_up': jumps_up,
        'jumps_down': jumps_down,
        'all_jumps': all_jumps,
        'jump_analysis': jump_analysis,
        'jump_count': all_jumps.sum()
    }
```

### 价格偏离检测
```python
def price_deviation_detection(price_data, benchmark_data, window=20, threshold=0.1):
    """
    检测相对于基准的价格偏离异常
    """
    # 计算相对表现
    price_returns = price_data.pct_change()
    benchmark_returns = benchmark_data.pct_change()
    
    relative_performance = price_returns - benchmark_returns
    
    # 使用Z-Score检测异常偏离
    result = zscore_anomaly_detection(relative_performance, window, threshold)
    
    # 增加偏离方向和程度
    deviation_analysis = []
    for date in result['anomaly_dates']:
        deviation = relative_performance[date]
        deviation_type = "超额表现" if deviation > 0 else "落后表现"
        
        deviation_analysis.append({
            'date': date,
            'deviation': deviation,
            'deviation_percentage': deviation * 100,
            'deviation_type': deviation_type,
            'z_score': result['z_scores'][date]
        })
    
    result['deviation_analysis'] = deviation_analysis
    return result
```

## 成交量异常检测

### 成交量激增检测
```python
def volume_surge_detection(volume_data, multiplier=3, window=20):
    """
    检测成交量异常激增
    """
    # 计算滚动平均成交量
    volume_ma = volume_data.rolling(window).mean()
    
    # 检测激增
    volume_surge = volume_data > (volume_ma * multiplier)
    
    surge_analysis = []
    for date in volume_data.index[volume_surge]:
        current_volume = volume_data[date]
        avg_volume = volume_ma[date]
        surge_ratio = current_volume / avg_volume
        
        surge_analysis.append({
            'date': date,
            'current_volume': current_volume,
            'average_volume': avg_volume,
            'surge_ratio': surge_ratio,
            'surge_percentage': (surge_ratio - 1) * 100
        })
    
    return {
        'volume_surge': volume_surge,
        'surge_analysis': surge_analysis,
        'surge_count': volume_surge.sum(),
        'volume_ma': volume_ma
    }
```

### 成交量枯竭检测
```python
def volume_drought_detection(volume_data, percentile=10, window=20):
    """
    检测成交量异常枯竭
    """
    # 计算滚动分位数
    volume_threshold = volume_data.rolling(window).quantile(percentile/100)
    
    # 检测枯竭
    volume_drought = volume_data < volume_threshold
    
    drought_analysis = []
    for date in volume_data.index[volume_drought]:
        current_volume = volume_data[date]
        threshold = volume_threshold[date]
        
        drought_analysis.append({
            'date': date,
            'current_volume': current_volume,
            'threshold': threshold,
            'drought_severity': (threshold - current_volume) / threshold
        })
    
    return {
        'volume_drought': volume_drought,
        'drought_analysis': drought_analysis,
        'drought_count': volume_drought.sum()
    }
```

## 复合异常检测

### 价量背离异常
```python
def price_volume_divergence_detection(price_data, volume_data):
    """
    检测价量背离异常
    """
    price_returns = price_data.pct_change()
    volume_changes = volume_data.pct_change()
    
    # 定义背离条件
    # 价涨量跌背离
    price_up_volume_down = (price_returns > 0.02) & (volume_changes < -0.1)
    
    # 价跌量涨背离  
    price_down_volume_up = (price_returns < -0.02) & (volume_changes > 0.1)
    
    all_divergences = price_up_volume_down | price_down_volume_up
    
    divergence_analysis = []
    for date in price_data.index[all_divergences]:
        price_change = price_returns[date]
        volume_change = volume_changes[date]
        
        if price_up_volume_down[date]:
            divergence_type = "价涨量跌背离"
        else:
            divergence_type = "价跌量涨背离"
        
        divergence_analysis.append({
            'date': date,
            'divergence_type': divergence_type,
            'price_change': price_change * 100,
            'volume_change': volume_change * 100
        })
    
    return {
        'price_up_volume_down': price_up_volume_down,
        'price_down_volume_up': price_down_volume_up,
        'all_divergences': all_divergences,
        'divergence_analysis': divergence_analysis
    }
```

### 多维度异常检测
```python
def multi_dimensional_anomaly_detection(price_data, volume_data, window=20):
    """
    多维度综合异常检测
    """
    # 各种异常检测
    price_anomalies = zscore_anomaly_detection(price_data.pct_change(), window)
    volume_anomalies = zscore_anomaly_detection(volume_data, window)
    jump_anomalies = price_jump_detection(price_data)
    divergence_anomalies = price_volume_divergence_detection(price_data, volume_data)
    
    # 综合异常评分
    all_dates = price_data.index
    anomaly_scores = pd.Series(0.0, index=all_dates)
    
    # 价格异常 (权重30%)
    anomaly_scores[price_anomalies['anomalies']] += 0.3
    
    # 成交量异常 (权重20%)
    anomaly_scores[volume_anomalies['anomalies']] += 0.2
    
    # 价格跳跃 (权重30%)
    anomaly_scores[jump_anomalies['all_jumps']] += 0.3
    
    # 价量背离 (权重20%)
    anomaly_scores[divergence_anomalies['all_divergences']] += 0.2
    
    # 识别高异常评分的日期
    high_anomaly_days = anomaly_scores[anomaly_scores >= 0.5]
    
    return {
        'anomaly_scores': anomaly_scores,
        'high_anomaly_days': high_anomaly_days,
        'price_anomalies': price_anomalies,
        'volume_anomalies': volume_anomalies,
        'jump_anomalies': jump_anomalies,
        'divergence_anomalies': divergence_anomalies
    }
```

## 实际应用策略

### 事件驱动交易
```python
def event_driven_trading_signals(anomaly_results, price_data):
    """
    基于异常检测的事件驱动交易信号
    """
    signals = []
    
    for date, score in anomaly_results['high_anomaly_days'].items():
        price_change = price_data.pct_change()[date]
        
        # 根据异常类型和价格变化生成信号
        if score >= 0.7:  # 强异常
            if price_change > 0.03:  # 大涨
                signals.append(('SELL_ALERT', date, '异常大涨，警惕回调'))
            elif price_change < -0.03:  # 大跌
                signals.append(('BUY_OPPORTUNITY', date, '异常大跌，抄底机会'))
        
        elif score >= 0.5:  # 中等异常
            if abs(price_change) > 0.02:
                signals.append(('MONITOR', date, '中等异常，密切关注'))
    
    return signals
```

### 风险预警系统
```python
def risk_warning_system(anomaly_results, threshold=0.6):
    """
    基于异常检测的风险预警系统
    """
    warnings = []
    
    # 近期异常频率分析
    recent_anomalies = anomaly_results['anomaly_scores'].tail(20)
    anomaly_frequency = (recent_anomalies > 0.3).mean()
    
    if anomaly_frequency > 0.3:
        warnings.append({
            'type': '高频异常警告',
            'message': f'近20日异常频率达{anomaly_frequency:.1%}，市场可能存在系统性风险',
            'severity': 'HIGH'
        })
    
    # 连续异常检测
    consecutive_anomalies = 0
    max_consecutive = 0
    
    for score in recent_anomalies:
        if score > threshold:
            consecutive_anomalies += 1
            max_consecutive = max(max_consecutive, consecutive_anomalies)
        else:
            consecutive_anomalies = 0
    
    if max_consecutive >= 3:
        warnings.append({
            'type': '连续异常警告',
            'message': f'检测到连续{max_consecutive}日异常，建议降低仓位',
            'severity': 'MEDIUM'
        })
    
    return warnings
```

## 注意事项

### ⚠️ 方法局限性
- **假阳性**: 正常波动可能被误识别为异常
- **滞后性**: 基于历史数据，无法预测未来异常
- **参数敏感**: 阈值设置影响检测效果
- **市场环境**: 不同市场环境下的异常标准不同

### 🔍 最佳实践
- **多方法结合**: 使用多种检测方法交叉验证
- **参数优化**: 根据历史数据优化检测参数
- **人工验证**: 异常检测结果需要人工确认
- **动态调整**: 根据市场变化调整检测标准

### 💡 实用建议
- **分类处理**: 区分技术性异常和基本面异常
- **时效性**: 异常检测要求快速响应
- **组合分析**: 结合多个ETF的异常模式分析
- **历史回溯**: 定期回测异常检测的有效性 