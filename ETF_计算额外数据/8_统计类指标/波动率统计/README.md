# 波动率统计 (Volatility Statistics)

## 概述
波动率统计是通过各种统计方法量化价格变动的不确定性和风险水平，为投资决策和风险管理提供重要的量化指标。

## 主要指标

### 1. 历史波动率 (Historical Volatility)

#### 简单历史波动率
- **公式**: σ = √(Σ(Ri - R̄)² / (n-1)) × √252
- **其中**: Ri为日收益率，R̄为平均收益率，n为样本数
- **年化**: 乘以√252调整为年化波动率
- **用途**: 衡量历史价格波动程度

#### EWMA波动率 (指数加权移动平均)
- **公式**: σt² = λ × σt-1² + (1-λ) × Rt²
- **参数**: λ通常取0.94-0.97
- **优势**: 对近期数据给予更高权重
- **应用**: 动态风险评估

#### GARCH波动率
- **模型**: σt² = ω + α × εt-1² + β × σt-1²
- **特点**: 考虑波动率聚集性
- **优势**: 更好地拟合金融数据特征
- **应用**: 专业风险管理

### 2. 实现波动率 (Realized Volatility)

#### 基于高频数据
- **公式**: RV = √(Σ ri²) × √(N/T)
- **其中**: ri为高频收益率，N为频率，T为时间单位
- **优势**: 更准确反映实际波动
- **要求**: 需要日内高频数据

#### 基于日内极值
- **Parkinson估计**: σ² = (1/4ln2) × Σ(ln(Hi/Li))²
- **Garman-Klass估计**: 结合开高低收四个价格
- **优势**: 仅需OHLC数据
- **应用**: 适合ETF日度数据

### 3. 条件波动率 (Conditional Volatility)

#### 时变波动率
- **计算**: 滚动窗口计算的动态波动率
- **窗口**: 20日、60日、120日
- **特点**: 反映波动率的时变特征
- **用途**: 动态风险调整

#### 分段波动率
- **上涨波动率**: 仅计算上涨日的波动率
- **下跌波动率**: 仅计算下跌日的波动率
- **比较**: 比值反映市场不对称性
- **应用**: 情绪分析和风险偏好

### 4. 相对波动率指标

#### 波动率比率
- **公式**: VR = 当前波动率 / 历史平均波动率
- **解释**: >1表示高波动，<1表示低波动
- **应用**: 波动率异常检测

#### 波动率分位数
- **计算**: 当前波动率在历史分布中的位置
- **范围**: 0-100分位数
- **用途**: 相对波动水平判断

## 计算方法

### 标准历史波动率
```python
def historical_volatility(returns, window=20):
    """计算历史波动率"""
    rolling_std = returns.rolling(window).std()
    annualized_vol = rolling_std * np.sqrt(252)
    return annualized_vol
```

### EWMA波动率
```python
def ewma_volatility(returns, lambda_param=0.94):
    """计算EWMA波动率"""
    var = returns.var()
    for i in range(1, len(returns)):
        var = lambda_param * var + (1 - lambda_param) * returns.iloc[i]**2
    return np.sqrt(var * 252)
```

### Parkinson波动率
```python
def parkinson_volatility(high, low, window=20):
    """基于高低价的波动率估计"""
    hl_ratio = np.log(high / low)
    parkinson_var = (1 / (4 * np.log(2))) * hl_ratio**2
    rolling_var = parkinson_var.rolling(window).mean()
    return np.sqrt(rolling_var * 252)
```

## 输出字段
- `HV_20`: 20日历史波动率
- `HV_60`: 60日历史波动率
- `EWMA_Vol`: EWMA波动率
- `Parkinson_Vol`: Parkinson波动率
- `Upside_Vol`: 上涨波动率
- `Downside_Vol`: 下跌波动率
- `Vol_Ratio`: 波动率比率
- `Vol_Percentile`: 波动率分位数
- `Vol_Trend`: 波动率趋势

## 实战应用

### 1. 风险管理
```python
# 动态风险预算
if current_volatility > percentile_90:
    position_size *= 0.5  # 高波动时减仓
elif current_volatility < percentile_10:
    position_size *= 1.5  # 低波动时增仓
```

### 2. 交易时机
- **低波动买入**: 波动率处于历史低位时买入
- **高波动卖出**: 波动率极高时可能接近顶部
- **波动率突破**: 波动率突然上升可能预示大行情

### 3. 策略选择
- **高波动环境**: 适合做空波动率、卖出期权
- **低波动环境**: 适合买入波动率、买入期权
- **波动率均值回归**: 利用波动率的均值回归特性

## 市场环境分析

### 1. 波动率状态判断
```python
def volatility_state(current_vol, historical_vol):
    percentile = stats.percentileofscore(historical_vol, current_vol)
    
    if percentile > 80:
        return "高波动状态"
    elif percentile < 20:
        return "低波动状态"
    else:
        return "正常波动状态"
```

### 2. 波动率趋势
- **上升趋势**: 连续上升，市场不确定性增加
- **下降趋势**: 连续下降，市场趋于稳定
- **波动率爆发**: 突然大幅上升，可能有重大事件

### 3. 相关性分析
- **与价格关系**: 通常负相关（恐慌指数效应）
- **与成交量关系**: 通常正相关
- **与宏观环境**: 与市场情绪和政策环境相关

## 参数设置

### 1. 时间窗口选择
- **短期**: 10-20日（适合短线交易）
- **中期**: 30-60日（适合中线投资）
- **长期**: 120-252日（适合长线配置）

### 2. EWMA参数
- **λ=0.94**: RiskMetrics标准参数
- **λ=0.97**: 更平滑，适合长期分析
- **λ=0.90**: 更敏感，适合短期分析

## 组合应用

### 与技术指标组合
- **波动率 + RSI**: 高波动率超卖区域是好的买点
- **波动率 + MACD**: 低波动率MACD金叉信号更可靠
- **波动率 + 均线**: 突破 + 波动率上升确认趋势

### 与基本面组合
- **业绩公布期**: 波动率通常上升
- **分红除权**: 波动率可能受到影响
- **重大事件**: 关注波动率异常变化

## 风险提示
- 波动率预测具有不确定性
- 历史波动率不等于未来波动率
- 需要考虑市场结构性变化
- 极端事件可能导致波动率模型失效
- 建议结合多种方法综合判断 