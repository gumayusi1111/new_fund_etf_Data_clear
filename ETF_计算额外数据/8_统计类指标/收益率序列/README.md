# 收益率序列 (Return Series)

## 指标概述
收益率是衡量投资表现的核心指标，通过计算不同时间周期的收益率可以全面评估ETF的投资价值和风险特征。

## 包含指标

### 基础收益率
- **Daily_Return**: 日收益率
- **Weekly_Return**: 周收益率  
- **Monthly_Return**: 月收益率
- **Quarterly_Return**: 季度收益率
- **Annual_Return**: 年收益率

### 累计收益率
- **Cumulative_Return**: 累计收益率
- **Rolling_Return**: 滚动收益率
- **Period_Return**: 区间收益率

### 年化收益率
- **Annualized_Return**: 年化收益率
- **Geometric_Return**: 几何平均收益率
- **Arithmetic_Return**: 算术平均收益率

## 计算方法

### 日收益率
```python
# 简单收益率
Daily_Return = (今日收盘价 - 昨日收盘价) / 昨日收盘价

# 对数收益率 (更适合复利计算)
Log_Return = ln(今日收盘价 / 昨日收盘价)

# 基于涨跌幅计算 (现有数据)
Daily_Return = 涨跌幅% / 100
```

### 周收益率
```python
# 周K线收益率
Weekly_Return = (周收盘价 - 上周收盘价) / 上周收盘价

# 基于日收益率计算
Weekly_Return = (1 + R1) × (1 + R2) × ... × (1 + R5) - 1
```

### 月收益率
```python
# 月K线收益率
Monthly_Return = (月收盘价 - 上月收盘价) / 上月收盘价

# 基于日收益率复利计算
Monthly_Return = ∏(1 + Daily_Return_i) - 1
```

### 累计收益率
```python
# 从起始日期到当前的总收益率
Cumulative_Return = (当前价格 - 起始价格) / 起始价格

# 基于日收益率计算
Cumulative_Return = ∏(1 + Daily_Return_i) - 1
```

### 年化收益率
```python
# 基于累计收益率年化
Annualized_Return = (1 + Cumulative_Return)^(252/交易日数) - 1

# 基于平均日收益率年化
Annualized_Return = (1 + 平均日收益率)^252 - 1

# 几何平均年化收益率
Geometric_Return = (∏(1 + Daily_Return_i))^(252/n) - 1
```

## 应用分析

### 1. 投资业绩评估
- **绝对收益**: 评估投资的盈亏情况
- **相对收益**: 与基准指数比较
- **超额收益**: 超越无风险利率的部分
- **阿尔法收益**: 超越市场基准的部分

### 2. 风险收益特征
- **收益波动性**: 收益率的标准差
- **收益分布**: 收益率的统计分布特征
- **极值分析**: 最大/最小收益率
- **收益连续性**: 连续正/负收益的特征

### 3. 时间序列分析
- **收益趋势**: 收益率的长期趋势
- **周期性**: 收益率的周期性特征
- **季节性**: 特定时期的收益表现
- **自相关性**: 收益率的时间序列相关性

## 实用计算函数

### 收益率计算器
```python
def calculate_returns(prices, method='simple'):
    """计算收益率序列"""
    if method == 'simple':
        returns = prices.pct_change()
    elif method == 'log':
        returns = np.log(prices / prices.shift(1))
    return returns.dropna()

def annualize_returns(returns, periods=252):
    """年化收益率"""
    cumulative_return = (1 + returns).prod() - 1
    n_periods = len(returns)
    return (1 + cumulative_return) ** (periods / n_periods) - 1
```

### 滚动收益率
```python
def rolling_returns(prices, window=20):
    """计算滚动收益率"""
    return prices.pct_change(window)

def rolling_annualized_returns(prices, window=252):
    """滚动年化收益率"""
    returns = prices.pct_change()
    rolling_ret = returns.rolling(window).apply(
        lambda x: (1 + x).prod() ** (252/len(x)) - 1
    )
    return rolling_ret
```

### 收益率统计
```python
def return_statistics(returns):
    """收益率统计特征"""
    stats = {
        '平均收益率': returns.mean(),
        '收益率标准差': returns.std(),
        '收益率偏度': returns.skew(),
        '收益率峰度': returns.kurt(),
        '最大收益率': returns.max(),
        '最小收益率': returns.min(),
        '正收益率占比': (returns > 0).mean(),
        '年化收益率': returns.mean() * 252,
        '年化波动率': returns.std() * np.sqrt(252)
    }
    return stats
```

## 收益率类型对比

### 简单收益率 vs 对数收益率

#### 简单收益率
- **优点**: 直观易懂，符合日常理解
- **缺点**: 不具备时间可加性
- **适用**: 单期收益分析

#### 对数收益率  
- **优点**: 具有时间可加性，便于复利计算
- **缺点**: 理解相对复杂
- **适用**: 多期收益分析、风险模型

### 算术平均 vs 几何平均

#### 算术平均收益率
- **计算**: 所有收益率的简单平均
- **特点**: 简单直接，但高估长期收益
- **适用**: 短期收益预期

#### 几何平均收益率
- **计算**: 复合收益率的几何平均
- **特点**: 反映真实的复合增长率
- **适用**: 长期投资收益评估

## 实际应用案例

### 投资组合分析
```python
def portfolio_analysis(etf_returns, benchmark_returns):
    """投资组合分析"""
    # 超额收益
    excess_returns = etf_returns - benchmark_returns
    
    # 信息比率
    information_ratio = excess_returns.mean() / excess_returns.std() * np.sqrt(252)
    
    # 最大回撤
    cumulative = (1 + etf_returns).cumprod()
    rolling_max = cumulative.expanding().max()
    drawdown = (cumulative - rolling_max) / rolling_max
    max_drawdown = drawdown.min()
    
    return {
        '年化收益率': etf_returns.mean() * 252,
        '年化波动率': etf_returns.std() * np.sqrt(252),
        '信息比率': information_ratio,
        '最大回撤': max_drawdown,
        '胜率': (excess_returns > 0).mean()
    }
```

### 风险调整收益
```python
def risk_adjusted_returns(returns, risk_free_rate=0.03):
    """风险调整收益指标"""
    annual_return = returns.mean() * 252
    annual_volatility = returns.std() * np.sqrt(252)
    
    # 夏普比率
    sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility
    
    # 索提诺比率 (只考虑下行风险)
    downside_returns = returns[returns < 0]
    downside_volatility = downside_returns.std() * np.sqrt(252)
    sortino_ratio = (annual_return - risk_free_rate) / downside_volatility
    
    return {
        '夏普比率': sharpe_ratio,
        '索提诺比率': sortino_ratio
    }
```

## 复权处理

### 复权对收益率的影响
- **前复权**: 适合技术分析的收益率计算
- **后复权**: 适合投资业绩的真实评估
- **除权**: 不考虑分红的价格收益率

### 总回报计算
```python
def total_return_analysis(forward_adj_prices, raw_prices):
    """总回报分析"""
    # 价格收益率 (基于除权价格)
    price_returns = raw_prices.pct_change()
    
    # 总收益率 (基于复权价格)  
    total_returns = forward_adj_prices.pct_change()
    
    # 分红收益率
    dividend_returns = total_returns - price_returns
    
    return {
        '总收益率': total_returns,
        '价格收益率': price_returns,
        '分红收益率': dividend_returns
    }
```

## 注意事项

### 1. 数据质量
- **价格连续性**: 确保价格数据连续无误
- **复权一致性**: 统一使用相同复权方式
- **异常值处理**: 识别和处理异常收益率

### 2. 计算精度
- **浮点数精度**: 注意累计计算的精度损失
- **时间对齐**: 确保不同频率数据的时间对齐
- **交易日计算**: 使用实际交易日数而非自然日

### 3. 解释局限
- **历史局限性**: 历史收益不代表未来表现
- **市场环境**: 不同市场环境下收益特征不同
- **样本偏差**: 避免幸存者偏差等统计陷阱

## 适用场景
- ✅ 投资业绩评估
- ✅ 风险收益分析
- ✅ 投资组合优化
- ✅ 基准比较分析

## 输出字段
- `Daily_Return`: 日收益率
- `Weekly_Return`: 周收益率
- `Monthly_Return`: 月收益率
- `Cumulative_Return`: 累计收益率
- `Annualized_Return`: 年化收益率
- `Rolling_Return_20`: 20日滚动收益率
- `Return_Volatility`: 收益率波动性
- `Excess_Return`: 超额收益率 