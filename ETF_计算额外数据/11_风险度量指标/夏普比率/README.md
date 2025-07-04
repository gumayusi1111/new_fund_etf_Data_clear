# 夏普比率 (Sharpe Ratio) 指标

## 概述
夏普比率是衡量投资组合风险调整后收益的核心指标，由诺贝尔经济学奖得主威廉·夏普提出。它衡量每单位风险能获得多少超额收益。

## 计算公式

### 基础夏普比率
```python
夏普比率 = (投资组合收益率 - 无风险收益率) / 投资组合标准差
```

### 实际计算实现
```python
import pandas as pd
import numpy as np

def calculate_sharpe_ratio(returns, risk_free_rate=0.0, periods=252):
    """
    计算夏普比率
    
    Parameters:
    returns: 收益率序列
    risk_free_rate: 无风险收益率 (年化)
    periods: 年化调整系数 (252个交易日)
    """
    # 计算超额收益
    excess_returns = returns - risk_free_rate/periods
    
    # 计算年化收益率
    annual_return = excess_returns.mean() * periods
    
    # 计算年化波动率
    annual_volatility = excess_returns.std() * np.sqrt(periods)
    
    # 计算夏普比率
    if annual_volatility == 0:
        return np.nan
    
    sharpe_ratio = annual_return / annual_volatility
    
    return {
        'sharpe_ratio': sharpe_ratio,
        'annual_return': annual_return,
        'annual_volatility': annual_volatility,
        'excess_return': annual_return
    }
```

## 夏普比率等级标准

### 📊 评级标准
- **> 2.0**: 优秀 - 非常好的风险调整收益
- **1.0 - 2.0**: 良好 - 不错的风险调整收益  
- **0.5 - 1.0**: 一般 - 可接受的风险调整收益
- **0 - 0.5**: 较差 - 风险调整收益偏低
- **< 0**: 很差 - 连无风险收益都跑不赢

### 🎯 市场背景参考
- **大盘指数**: 通常0.3-0.8
- **优质ETF**: 通常0.5-1.2
- **量化基金**: 通常0.8-1.5
- **顶级对冲基金**: 可达1.5-3.0

## 改进版本

### 1. 索提诺比率 (Sortino Ratio)
```python
def calculate_sortino_ratio(returns, risk_free_rate=0.0, periods=252):
    """
    索提诺比率 - 只考虑下行风险
    """
    excess_returns = returns - risk_free_rate/periods
    
    # 只计算负收益的标准差
    downside_returns = excess_returns[excess_returns < 0]
    downside_deviation = downside_returns.std() * np.sqrt(periods)
    
    annual_return = excess_returns.mean() * periods
    
    if downside_deviation == 0:
        return np.nan
    
    return annual_return / downside_deviation
```

### 2. 卡尔马比率 (Calmar Ratio)
```python
def calculate_calmar_ratio(price_series, periods=252):
    """
    卡尔马比率 = 年化收益率 / 最大回撤
    """
    returns = price_series.pct_change().dropna()
    
    # 计算年化收益率
    total_return = (price_series.iloc[-1] / price_series.iloc[0]) - 1
    years = len(returns) / periods
    annual_return = (1 + total_return) ** (1/years) - 1
    
    # 计算最大回撤
    cumulative = (1 + returns).cumprod()
    peak = cumulative.expanding().max()
    drawdown = (cumulative - peak) / peak
    max_drawdown = abs(drawdown.min())
    
    if max_drawdown == 0:
        return np.nan
    
    return annual_return / max_drawdown
```

### 3. 信息比率 (Information Ratio)
```python
def calculate_information_ratio(returns, benchmark_returns, periods=252):
    """
    信息比率 = 超额收益 / 跟踪误差
    """
    excess_returns = returns - benchmark_returns
    
    annual_excess_return = excess_returns.mean() * periods
    tracking_error = excess_returns.std() * np.sqrt(periods)
    
    if tracking_error == 0:
        return np.nan
    
    return annual_excess_return / tracking_error
```

## 动态夏普比率分析

### 滚动夏普比率
```python
def rolling_sharpe_ratio(returns, window=252, risk_free_rate=0.0):
    """
    计算滚动夏普比率
    """
    rolling_sharpe = []
    
    for i in range(window, len(returns) + 1):
        period_returns = returns[i-window:i]
        sharpe_result = calculate_sharpe_ratio(period_returns, risk_free_rate)
        rolling_sharpe.append(sharpe_result['sharpe_ratio'])
    
    return pd.Series(rolling_sharpe, 
                    index=returns.index[window-1:])
```

### 夏普比率稳定性分析
```python
def sharpe_stability_analysis(returns, window=63):
    """
    分析夏普比率的稳定性
    """
    rolling_sharpe = rolling_sharpe_ratio(returns, window)
    
    return {
        'mean_sharpe': rolling_sharpe.mean(),
        'std_sharpe': rolling_sharpe.std(),
        'min_sharpe': rolling_sharpe.min(),
        'max_sharpe': rolling_sharpe.max(),
        'stability_score': rolling_sharpe.mean() / rolling_sharpe.std() if rolling_sharpe.std() > 0 else 0,
        'positive_periods': (rolling_sharpe > 0).sum() / len(rolling_sharpe)
    }
```

## 实战应用策略

### 📈 ETF筛选策略
```python
def etf_screening_by_sharpe(etf_data_dict, min_sharpe=0.5, min_periods=252):
    """
    基于夏普比率筛选ETF
    """
    qualified_etfs = {}
    
    for etf_code, data in etf_data_dict.items():
        if len(data) < min_periods:
            continue
            
        returns = data['close'].pct_change().dropna()
        sharpe_result = calculate_sharpe_ratio(returns)
        
        if sharpe_result['sharpe_ratio'] >= min_sharpe:
            qualified_etfs[etf_code] = {
                'sharpe_ratio': sharpe_result['sharpe_ratio'],
                'annual_return': sharpe_result['annual_return'],
                'annual_volatility': sharpe_result['annual_volatility']
            }
    
    # 按夏普比率排序
    sorted_etfs = dict(sorted(qualified_etfs.items(), 
                             key=lambda x: x[1]['sharpe_ratio'], 
                             reverse=True))
    
    return sorted_etfs
```

### 🎯 组合优化策略
```python
def portfolio_optimization_sharpe(returns_matrix, risk_free_rate=0.0):
    """
    基于夏普比率的投资组合优化
    """
    from scipy.optimize import minimize
    
    def negative_sharpe(weights, returns, risk_free_rate):
        portfolio_return = np.sum(returns.mean() * weights) * 252
        portfolio_volatility = np.sqrt(np.dot(weights.T, 
                                              np.dot(returns.cov() * 252, weights)))
        return -(portfolio_return - risk_free_rate) / portfolio_volatility
    
    num_assets = len(returns_matrix.columns)
    constraints = ({'type': 'eq', 'fun': lambda x: np.sum(x) - 1})
    bounds = tuple((0, 1) for _ in range(num_assets))
    initial_guess = num_assets * [1. / num_assets]
    
    result = minimize(negative_sharpe, initial_guess, 
                     args=(returns_matrix, risk_free_rate),
                     method='SLSQP', bounds=bounds, constraints=constraints)
    
    return result.x
```

### ⚡ 择时策略
```python
def market_timing_by_sharpe(returns, benchmark_returns, threshold=0.3):
    """
    基于相对夏普比率的择时策略
    """
    signals = []
    window = 63  # 季度窗口
    
    for i in range(window, len(returns)):
        period_returns = returns[i-window:i]
        benchmark_period = benchmark_returns[i-window:i]
        
        etf_sharpe = calculate_sharpe_ratio(period_returns)['sharpe_ratio']
        benchmark_sharpe = calculate_sharpe_ratio(benchmark_period)['sharpe_ratio']
        
        relative_sharpe = etf_sharpe - benchmark_sharpe
        
        if relative_sharpe > threshold:
            signals.append(('BUY', returns.index[i]))
        elif relative_sharpe < -threshold:
            signals.append(('SELL', returns.index[i]))
        else:
            signals.append(('HOLD', returns.index[i]))
    
    return signals
```

## 实际应用案例

### 案例1: ETF风险收益评估
某投资者比较三只科技ETF：
- **ETF A**: 夏普比率 0.85，年化收益15%，年化波动25%
- **ETF B**: 夏普比率 1.20，年化收益12%，年化波动18%  
- **ETF C**: 夏普比率 0.45，年化收益18%，年化波动35%

**结论**: ETF B的风险调整收益最优

### 案例2: 市场周期分析
通过滚动夏普比率发现：
- **牛市期间**: 夏普比率普遍较高 (>1.0)
- **熊市期间**: 夏普比率转负 (<0)
- **震荡期间**: 夏普比率在0-0.5波动

## 注意事项

### ⚠️ 使用限制
- **历史依赖**: 基于历史数据，不能预测未来
- **正态分布假设**: 假设收益率呈正态分布
- **无风险利率选择**: 不同无风险利率影响结果
- **时间窗口敏感**: 计算周期选择影响结果

### 🔍 改进建议
- **多时间框架**: 同时查看不同周期的夏普比率
- **分市场环境**: 分别计算牛熊市的夏普比率
- **结合其他指标**: 与最大回撤、索提诺比率等综合分析
- **动态调整**: 根据市场环境调整无风险利率

### 💡 最佳实践
- **最小样本**: 至少252个交易日数据
- **定期更新**: 每月或每季度更新计算
- **基准比较**: 与市场基准或同类产品比较
- **风险预算**: 结合夏普比率制定投资组合风险预算 