# 可计算的流动性指标 (Calculable Liquidity Indicators)

## 概述
基于现有的OHLCV数据，可以计算出多个反映ETF流动性状况的指标，这些指标帮助投资者评估ETF的交易活跃度和市场深度。

## 主要指标

### 1. 成交活跃度指标

#### 成交强度 (Trading Intensity)
- **公式**: TI = 当日成交额 / 近期平均成交额
- **含义**: 相对于历史水平的交易活跃程度
- **阈值**: 
  - TI > 2: 异常活跃
  - 1.5 < TI ≤ 2: 较为活跃
  - 0.5 < TI ≤ 1.5: 正常
  - TI ≤ 0.5: 交易清淡

#### 成交量比率 (Volume Ratio)
- **公式**: VR = 当日成交量 / N日平均成交量
- **周期**: 通常使用20日平均
- **应用**: 识别异常交易日
- **信号**: VR > 3时关注重大事件

#### 成交频率指标
- **计算**: 基于价格变化频率推算
- **估算**: 通过振幅和成交量关系估算
- **用途**: 评估市场参与度

### 2. 价量关系指标

#### 成交量加权平均价格 (VWAP)
- **公式**: VWAP = Σ(Price × Volume) / Σ(Volume)
- **计算**: 可以按日内或多日计算
- **用途**: 
  - 评估交易成本
  - 算法交易基准
  - 流动性质量评估

#### 价格冲击指标 (Price Impact)
- **估算**: 成交量变化与价格变化的关系
- **计算**: ΔPrice / ΔVolume
- **含义**: 单位成交量对价格的影响
- **应用**: 大额交易成本预估

#### 成交量加权收益率
- **公式**: VWR = Σ(Return × Volume) / Σ(Volume)
- **优势**: 考虑了成交量权重的收益率
- **用途**: 更准确的业绩评估

### 3. 市场深度代理指标

#### 买卖价差估算 (Estimated Spread)
- **Roll模型**: Spread = 2√(-Cov(Δp_t, Δp_{t-1}))
- **基于**: 价格序列的负自相关性
- **限制**: 需要高频数据效果更好
- **应用**: 流动性成本估算

#### 流动性噪声指标
- **计算**: 价格变化的非基本面部分
- **方法**: 残差分析或滤波技术
- **含义**: 市场微观结构摩擦
- **用途**: 流动性质量评估

### 4. 时间序列流动性指标

#### 成交量持续性
- **计算**: 成交量的自相关系数
- **含义**: 高成交量的持续性
- **应用**: 预测未来流动性

#### 价格效率指标
- **方法**: 方差比率测试
- **VR = Var(k-period returns) / k × Var(1-period returns)**
- **理想值**: VR = 1 (随机游走)
- **含义**: VR偏离1表示价格效率降低

#### 流动性波动率
- **计算**: 流动性指标的标准差
- **周期**: 通常使用20日或60日
- **用途**: 评估流动性稳定性

## 计算方法

### VWAP计算
```python
def calculate_vwap(high, low, close, volume, period=20):
    """计算成交量加权平均价格"""
    typical_price = (high + low + close) / 3
    vwap = (typical_price * volume).rolling(period).sum() / volume.rolling(period).sum()
    return vwap
```

### 成交强度计算
```python
def trading_intensity(amount, period=20):
    """计算成交强度"""
    avg_amount = amount.rolling(period).mean()
    intensity = amount / avg_amount
    return intensity
```

### 价格冲击估算
```python
def price_impact_estimate(returns, volume_change):
    """估算价格冲击"""
    # 简单线性回归方法
    from scipy import stats
    slope, intercept, r_value, p_value, std_err = stats.linregress(volume_change, returns)
    return slope  # 斜率即为价格冲击系数
```

### Buy-Sell Spread估算
```python
def estimate_spread_roll(price_changes):
    """使用Roll模型估算买卖价差"""
    # 计算价格变化的负自相关
    autocovariance = np.cov(price_changes[:-1], price_changes[1:])[0,1]
    if autocovariance < 0:
        spread = 2 * np.sqrt(-autocovariance)
    else:
        spread = 0  # 没有检测到买卖价差
    return spread
```

## 输出字段
- `Trading_Intensity`: 成交强度
- `Volume_Ratio`: 成交量比率
- `VWAP_20`: 20日成交量加权平均价
- `Price_Impact`: 价格冲击估算
- `VW_Return`: 成交量加权收益率
- `Liquidity_Score`: 综合流动性评分
- `Volume_Persistence`: 成交量持续性
- `Price_Efficiency`: 价格效率指标
- `Estimated_Spread`: 估算买卖价差

## 流动性评级体系

### 流动性评分计算
```python
def liquidity_score(trading_intensity, volume_ratio, price_impact, avg_amount):
    """计算综合流动性评分"""
    score = 0
    
    # 成交强度评分 (40%权重)
    if trading_intensity > 1.5:
        score += 40
    elif trading_intensity > 1.0:
        score += 30
    elif trading_intensity > 0.5:
        score += 20
    else:
        score += 10
    
    # 成交量稳定性评分 (30%权重)
    vol_cv = volume_ratio.rolling(20).std() / volume_ratio.rolling(20).mean()
    if vol_cv < 0.5:
        score += 30
    elif vol_cv < 1.0:
        score += 20
    else:
        score += 10
    
    # 成交金额评分 (30%权重)
    if avg_amount > 50000:  # 5万以上
        score += 30
    elif avg_amount > 10000:  # 1万以上
        score += 20
    else:
        score += 10
    
    return min(score, 100)  # 最高100分
```

### 流动性等级划分
```python
def liquidity_grade(score):
    """流动性等级划分"""
    if score >= 85:
        return "A级 - 优秀流动性"
    elif score >= 70:
        return "B级 - 良好流动性"
    elif score >= 55:
        return "C级 - 一般流动性"
    elif score >= 40:
        return "D级 - 较差流动性"
    else:
        return "E级 - 流动性不足"
```

## 实战应用

### 1. 交易时机选择
```python
def optimal_trading_time(trading_intensity, volume_ratio):
    """选择最佳交易时机"""
    if trading_intensity > 1.5 and volume_ratio > 1.2:
        return "适合大额交易"
    elif trading_intensity > 1.0:
        return "适合中等交易"
    elif trading_intensity < 0.5:
        return "谨慎交易，流动性不足"
    else:
        return "正常交易"
```

### 2. 交易成本预估
```python
def estimate_trading_cost(trade_size, avg_volume, price_impact):
    """预估交易成本"""
    volume_percentage = trade_size / avg_volume
    
    # 市场冲击成本
    impact_cost = price_impact * volume_percentage
    
    # 时间成本（基于流动性情况）
    if volume_percentage > 0.1:  # 超过10%日均成交量
        time_cost = 0.002  # 额外20bp
    elif volume_percentage > 0.05:
        time_cost = 0.001  # 额外10bp
    else:
        time_cost = 0.0005  # 额外5bp
    
    total_cost = impact_cost + time_cost
    return {
        'impact_cost': impact_cost,
        'time_cost': time_cost,
        'total_cost': total_cost
    }
```

### 3. 流动性风险监控
```python
def liquidity_risk_alert(current_indicators, historical_indicators):
    """流动性风险预警"""
    alerts = []
    
    # 成交量急剧萎缩
    if current_indicators['volume_ratio'] < 0.3:
        alerts.append("成交量严重萎缩")
    
    # 成交强度异常
    if current_indicators['trading_intensity'] < 0.2:
        alerts.append("交易活跃度极低")
    
    # 价格冲击增大
    if current_indicators['price_impact'] > historical_indicators['price_impact'] * 2:
        alerts.append("价格冲击成本上升")
    
    return alerts
```

## 市场环境适应性

### 不同市场状态下的流动性特征
- **牛市**: 流动性充裕，成交活跃
- **熊市**: 流动性紧张，价差扩大
- **震荡市**: 流动性波动较大
- **危机时**: 流动性急剧恶化

### 行业ETF vs 宽基ETF
- **宽基ETF**: 通常流动性更好
- **行业ETF**: 流动性差异较大
- **主题ETF**: 流动性波动较大

## 限制和注意事项
- 基于历史数据的估算可能不准确
- 某些指标需要高频数据才能精确计算
- 市场结构变化可能影响指标有效性
- 应结合实际交易体验验证指标准确性 