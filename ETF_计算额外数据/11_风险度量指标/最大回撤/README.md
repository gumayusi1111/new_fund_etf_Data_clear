# 最大回撤 (Maximum Drawdown) 指标

## 概述
最大回撤是衡量投资风险最直观的指标，表示在指定时期内，投资组合从最高峰值到最低谷底的最大跌幅。它反映了投资者可能面临的最坏情况。

## 计算方法

### 基础计算
```python
import pandas as pd
import numpy as np

def calculate_max_drawdown(price_series):
    # 计算累计收益
    cumulative_returns = (1 + price_series.pct_change()).cumprod()
    
    # 计算历史最高点
    peak = cumulative_returns.expanding().max()
    
    # 计算回撤
    drawdown = (cumulative_returns - peak) / peak
    
    # 最大回撤
    max_drawdown = drawdown.min()
    
    return max_drawdown, drawdown
```

### 详细计算步骤
```python
def detailed_drawdown_analysis(price_series):
    """
    详细的回撤分析，包括回撤持续期等信息
    """
    cumret = (1 + price_series.pct_change()).cumprod()
    peak = cumret.expanding().max()
    drawdown = (cumret - peak) / peak
    
    # 找到最大回撤的时间点
    max_dd_idx = drawdown.idxmin()
    max_dd_value = drawdown.min()
    
    # 找到最大回撤的开始点（前一个高点）
    peak_before = cumret[:max_dd_idx].idxmax()
    
    # 找到恢复点（如果有）
    recovery_idx = None
    peak_value = cumret[peak_before]
    for i, val in cumret[max_dd_idx:].items():
        if val >= peak_value:
            recovery_idx = i
            break
    
    # 计算持续期
    if recovery_idx:
        duration = (recovery_idx - peak_before).days
    else:
        duration = (cumret.index[-1] - peak_before).days
    
    return {
        'max_drawdown': max_dd_value,
        'peak_date': peak_before,
        'trough_date': max_dd_idx,
        'recovery_date': recovery_idx,
        'duration_days': duration,
        'drawdown_series': drawdown
    }
```

## 关键指标

### 最大回撤幅度
- **轻微回撤**: < 5%
- **中等回撤**: 5% - 15%
- **严重回撤**: 15% - 30%
- **极端回撤**: > 30%

### 回撤持续期
- **短期回撤**: < 30天
- **中期回撤**: 30 - 90天
- **长期回撤**: 90 - 365天
- **超长期回撤**: > 365天

### 回撤频率
```python
def drawdown_frequency(drawdown_series, threshold=-0.05):
    """
    计算超过阈值的回撤频率
    """
    below_threshold = drawdown_series < threshold
    transitions = below_threshold.diff()
    num_periods = (transitions == True).sum()
    return num_periods
```

## 应用策略

### 🎯 风险控制策略
```python
def risk_control_signal(current_drawdown, max_tolerance=-0.15):
    """
    基于回撤的风险控制信号
    """
    if current_drawdown <= max_tolerance:
        return "STOP_LOSS"  # 强制止损
    elif current_drawdown <= max_tolerance * 0.8:
        return "REDUCE_POSITION"  # 减仓
    elif current_drawdown <= max_tolerance * 0.5:
        return "WARNING"  # 警告
    else:
        return "NORMAL"  # 正常
```

### 💰 动态仓位管理
```python
def dynamic_position_sizing(max_drawdown, target_drawdown=-0.10):
    """
    基于历史最大回撤的动态仓位调整
    """
    if abs(max_drawdown) > abs(target_drawdown):
        # 历史回撤超过目标，降低仓位
        position_ratio = abs(target_drawdown) / abs(max_drawdown)
    else:
        # 历史回撤在可接受范围内，正常仓位
        position_ratio = 1.0
    
    return min(position_ratio, 1.0)
```

### 📊 投资组合优化
```python
def portfolio_optimization_with_drawdown(returns_matrix, max_dd_limit=-0.20):
    """
    考虑最大回撤约束的投资组合优化
    """
    # 对每个资产计算历史最大回撤
    asset_max_dd = {}
    for asset in returns_matrix.columns:
        dd_result = detailed_drawdown_analysis(returns_matrix[asset])
        asset_max_dd[asset] = dd_result['max_drawdown']
    
    # 筛选满足回撤要求的资产
    eligible_assets = [asset for asset, dd in asset_max_dd.items() 
                      if dd >= max_dd_limit]
    
    return eligible_assets, asset_max_dd
```

## 实际应用案例

### ETF风险分级
```python
def etf_risk_classification(max_drawdown):
    """
    基于最大回撤的ETF风险分级
    """
    if max_drawdown >= -0.05:
        return "低风险", "适合保守投资者"
    elif max_drawdown >= -0.15:
        return "中低风险", "适合稳健投资者"
    elif max_drawdown >= -0.30:
        return "中高风险", "适合积极投资者"
    else:
        return "高风险", "适合激进投资者"
```

### 择时策略
```python
def market_timing_strategy(current_drawdown, historical_avg_dd):
    """
    基于回撤的市场择时策略
    """
    relative_dd = current_drawdown / historical_avg_dd
    
    if relative_dd > 1.5:
        return "OVERSOLD", "可能的买入机会"
    elif relative_dd > 1.2:
        return "CAUTION", "谨慎观望"
    elif relative_dd < 0.5:
        return "STRONG", "市场表现强劲"
    else:
        return "NORMAL", "正常市场状态"
```

## 高级分析

### 条件回撤分析
```python
def conditional_drawdown_analysis(price_series, market_condition):
    """
    不同市场条件下的回撤分析
    """
    results = {}
    
    for condition in market_condition.unique():
        mask = market_condition == condition
        subset_prices = price_series[mask]
        
        if len(subset_prices) > 10:  # 确保有足够数据
            dd_result = detailed_drawdown_analysis(subset_prices)
            results[condition] = dd_result
    
    return results
```

### 回撤恢复分析
```python
def recovery_time_analysis(price_series):
    """
    分析回撤恢复时间的统计特征
    """
    cumret = (1 + price_series.pct_change()).cumprod()
    peak = cumret.expanding().max()
    drawdown = (cumret - peak) / peak
    
    recovery_times = []
    in_drawdown = False
    drawdown_start = None
    
    for date, dd in drawdown.items():
        if dd < -0.01 and not in_drawdown:  # 开始回撤
            in_drawdown = True
            drawdown_start = date
        elif dd >= -0.001 and in_drawdown:  # 恢复
            if drawdown_start:
                recovery_time = (date - drawdown_start).days
                recovery_times.append(recovery_time)
            in_drawdown = False
            drawdown_start = None
    
    if recovery_times:
        return {
            'avg_recovery_days': np.mean(recovery_times),
            'median_recovery_days': np.median(recovery_times),
            'max_recovery_days': np.max(recovery_times),
            'recovery_count': len(recovery_times)
        }
    else:
        return None
```

## 优缺点分析

### ✅ 优点
- **直观性**: 最容易理解的风险指标
- **实用性**: 直接关系投资者的心理承受能力
- **稳健性**: 不依赖分布假设
- **历史性**: 反映实际发生过的最坏情况

### ❌ 缺点
- **滞后性**: 只反映已发生的历史情况
- **路径依赖**: 相同收益率不同路径可能有不同回撤
- **时间敏感**: 计算期间的选择影响结果
- **恢复忽略**: 不考虑回撤后的恢复能力

## 注意事项

### ⚠️ 计算要点
- **基准选择**: 选择合适的基准点（通常是期初）
- **频率影响**: 数据频率影响回撤的精确度
- **复权处理**: 确保使用复权价格数据

### 🔍 分析技巧
- **滚动计算**: 观察不同时期的最大回撤变化
- **分解分析**: 区分技术性回撤和基本面回撤
- **比较分析**: 与同类资产和市场基准比较

### 💡 最佳实践
- **组合使用**: 与其他风险指标结合使用
- **情景分析**: 考虑不同市场环境下的回撤表现
- **定期更新**: 随着新数据更新回撤分析
- **前瞻思考**: 结合压力测试预测潜在回撤 