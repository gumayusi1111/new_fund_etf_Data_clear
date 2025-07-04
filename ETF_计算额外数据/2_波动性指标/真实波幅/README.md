# 真实波幅 (Average True Range - ATR)

## 指标概述
ATR由韦尔斯·怀尔德开发，用于衡量价格的波动性。不同于简单的价格范围，ATR考虑了跳空缺口对波动性的影响，提供更准确的波动率测量。

## 包含指标

### 核心ATR指标
- **TR**: 真实波幅 - 单日波动幅度
- **ATR14**: 14日平均真实波幅 - 标准周期
- **ATR21**: 21日平均真实波幅 - 月度周期

### 衍生指标
- **ATR_Percent**: ATR百分比 (ATR/收盘价×100)
- **ATR_Signal**: ATR信号 (高波动/低波动标识)
- **ATR_Bands**: 基于ATR的价格通道

## 计算公式

### 真实波幅计算
```
TR = MAX(
    最高价 - 最低价,
    |最高价 - 昨日收盘价|,
    |最低价 - 昨日收盘价|
)
```

### ATR计算
```
ATR = TR的N日简单移动平均
或
ATR = ((N-1) × 前日ATR + 当日TR) / N  (指数平滑)
```

### ATR百分比
```
ATR% = (ATR / 收盘价) × 100
用于不同价格水平的ETF比较
```

## 应用策略

### 1. 止损设置
- **倍数止损**: 止损 = 入场价 ± (N × ATR)
- **动态止损**: 根据ATR变化调整止损位
- **ATR追踪止损**: 止损位跟随价格移动

### 2. 仓位管理
- **风险均等**: 根据ATR调整仓位大小
- **波动性仓位**: ATR高时减仓，ATR低时加仓

### 3. 趋势强度判断
- **ATR上升**: 趋势强度增强
- **ATR下降**: 趋势强度减弱
- **ATR突破**: 波动性突变的信号

### 4. 市场状态识别
- **高ATR**: 市场活跃，波动剧烈
- **低ATR**: 市场平静，波动收敛
- **ATR扩张**: 可能有重要事件

## 实战应用

### 止损位计算示例
```python
# 基于ATR的止损设置
entry_price = 10.00
atr_14 = 0.15
multiplier = 2.0

# 多头止损
long_stop = entry_price - (multiplier * atr_14)  # 9.70

# 空头止损  
short_stop = entry_price + (multiplier * atr_14)  # 10.30
```

### 仓位大小计算
```python
# 风险均等仓位计算
account_risk = 0.02  # 单笔风险2%
account_capital = 100000
atr_14 = 0.15
multiplier = 2.0

risk_per_share = multiplier * atr_14  # 每股风险
max_risk = account_capital * account_risk  # 最大风险
position_size = max_risk / risk_per_share  # 仓位大小
```

## 参数设置建议

### 短期ATR (ATR7)
- **特点**: 反应快，适合短线
- **应用**: 日内交易止损

### 标准ATR (ATR14)
- **特点**: 平衡性好，最常用
- **应用**: 一般交易策略

### 长期ATR (ATR21/ATR30)
- **特点**: 平滑性好，噪音少
- **应用**: 中长线投资

## 市场环境分析

### ATR水平判断
- **极低ATR**: < 历史均值的50% - 市场极度平静
- **低ATR**: 50%-80%历史均值 - 市场相对平静
- **正常ATR**: 80%-120%历史均值 - 正常波动
- **高ATR**: 120%-200%历史均值 - 市场活跃
- **极高ATR**: > 200%历史均值 - 市场剧烈波动

### 波动性周期
- **收缩期**: ATR持续下降，酝酿突破
- **扩张期**: ATR快速上升，趋势加速
- **稳定期**: ATR水平平稳，趋势持续

## 实用工具函数

### ATR通道
```python
def atr_bands(high, low, close, period=14, multiplier=2):
    tr = calculate_tr(high, low, close)
    atr = tr.rolling(period).mean()
    
    upper_band = close + (multiplier * atr)
    lower_band = close - (multiplier * atr)
    
    return upper_band, lower_band
```

### 波动性比较
```python
def volatility_rank(atr, period=252):
    """计算ATR的历史分位数"""
    return (atr.rolling(period).rank() - 1) / (period - 1)
```

## 注意事项

### 1. 数据质量
- **价格跳空**: ATR能正确处理跳空缺口
- **数据异常**: 异常价格会影响ATR计算
- **复权处理**: 建议使用前复权数据

### 2. 使用限制
- **绝对数值**: ATR是绝对数值，需要相对化比较
- **滞后性**: ATR是历史数据的平均，有一定滞后
- **参数依赖**: 不同参数设置影响结果

### 3. 最佳实践
- **动态调整**: 根据市场状况调整ATR倍数
- **组合使用**: 与其他指标组合验证
- **定期评估**: 定期评估ATR策略有效性

## 适用场景
- ✅ 止损位设置
- ✅ 仓位管理
- ✅ 市场波动性分析
- ✅ 趋势强度判断

## 输出字段
- `TR`: 真实波幅
- `ATR_14`: 14日平均真实波幅
- `ATR_21`: 21日平均真实波幅
- `ATR_Percent`: ATR百分比
- `ATR_Rank`: ATR历史分位数
- `Volatility_State`: 波动性状态标识 