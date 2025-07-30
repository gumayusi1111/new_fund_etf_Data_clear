# 📊 动量振荡器系统 (Momentum Oscillators)

## 🎯 指标概述
动量振荡器基于物理学动量概念，由技术分析先驱马丁·普林格(Martin Pring)等专家发展完善。该指标通过计算价格变化的速率和强度来量化市场动能，基于"动量先于价格"的核心理念，是识别趋势加速、减速和转折点的重要工具。

## 📈 理论基础

### 🔬 核心原理
**动量先于价格理论**: 价格变化的速度往往在趋势转变前发生变化，动量指标能够提前捕捉到这种变化。通过测量当前价格与历史价格的差异或比率，构建反映市场动能的连续性指标，能够提前识别趋势强弱和反转信号。

### 📊 数学基础
```python
# 基础动量计算公式
Momentum = Close[t] - Close[t-n]    # 绝对动量
ROC = ((Close[t] - Close[t-n]) / Close[t-n]) * 100    # 相对变化率
Williams_R = ((HN - Close) / (HN - LN)) * (-100)     # 威廉指标
PMO = EMA(EMA(ROC * 1000, 35), 20)                  # 价格动量振荡器
```

## 🎯 核心指标设计 (客观数据字段)

### 📊 **十三大核心字段** (纯客观数据)

#### 1. 📦 基础动量指标组 (2个)
- **momentum_10**: 10日动量指标值 *(短期价格变化强度)*
- **momentum_20**: 20日动量指标值 *(中期价格变化强度)*

#### ⚙️ **计算逻辑**
```python
# 基础动量计算 (绝对差值法)
momentum_10 = close - close.shift(10)
momentum_20 = close - close.shift(20)

# 数值含义: 正值表示上涨动量，负值表示下跌动量
# 绝对值大小反映动量强度
```

#### 2. 📈 变动率指标组 (3个)
- **roc_5**: 5日变动率 *(超短期动量敏感度)*
- **roc_12**: 12日变动率 *(短期动量变化率)*
- **roc_25**: 25日变动率 *(中期动量趋势)*

#### ⚙️ **计算逻辑**
```python
# ROC变动率计算 (百分比形式)
roc_5 = ((close - close.shift(5)) / close.shift(5)) * 100
roc_12 = ((close - close.shift(12)) / close.shift(12)) * 100
roc_25 = ((close - close.shift(25)) / close.shift(25)) * 100

# 数值范围: -∞ ~ +∞ (不进行人为标准化)
# 正值表示涨幅，负值表示跌幅
```

#### 3. 📊 价格动量振荡器组 (3个)
- **pmo**: 价格动量振荡器主线 *(双重平滑动量信号)*
- **pmo_signal**: PMO信号线 *(九日EMA平滑)*
- **williams_r**: 威廉指标 *(超买超卖识别)*

#### ⚙️ **计算逻辑**
```python
# PMO双重指数平滑 (DecisionPoint方法)
roc_10 = ((close - close.shift(10)) / close.shift(10)) * 1000
ema1 = roc_10.ewm(span=35).mean()    # 第一次平滑
pmo = ema1.ewm(span=20).mean()       # 第二次平滑
pmo_signal = pmo.ewm(span=9).mean()  # 信号线

# Williams %R计算 (超买超卖边界)
highest_14 = high.rolling(14).max()
lowest_14 = low.rolling(14).min()
williams_r = ((highest_14 - close) / (highest_14 - lowest_14)) * (-100)
# 数值范围: -100 ~ 0，-20以上超买，-80以下超卖
```

#### 4. 🎯 综合动量指标组 (5个)
- **momentum_strength**: 动量强度 *(绝对动量大小)*
- **momentum_acceleration**: 动量加速度 *(动量变化率)*
- **momentum_trend**: 动量趋势状态 *(方向分类)*
- **momentum_divergence**: 动量背离信号 *(价格动量不一致)*
- **momentum_volatility**: 动量波动率 *(动量稳定性)*

#### ⚙️ **计算逻辑**
```python
# 动量强度和加速度
roc_20 = ((close - close.shift(20)) / close.shift(20)) * 100
momentum_strength = abs(roc_20)
momentum_acceleration = momentum_10 - momentum_20

# 动量趋势分类
def get_momentum_trend(momentum_20, roc_25):
    if momentum_20 > 0 and roc_25 > 2:
        return 1    # 上升趋势
    elif momentum_20 < 0 and roc_25 < -2:
        return -1   # 下降趋势
    else:
        return 0    # 震荡趋势

# 动量背离检测
price_direction = (close > close.shift(5)).astype(int) * 2 - 1
momentum_direction = (roc_5 > 0).astype(int) * 2 - 1
momentum_divergence = (price_direction != momentum_direction).astype(int)

# 动量波动率 (10日标准差)
momentum_volatility = roc_20.rolling(10).std()
```

## 📊 客观数据使用指南

### 🔬 **数据解读原则**
动量振荡器提供纯客观的数据输出，不进行主观评分。用户可根据具体需求自行分析判断：

### 📈 **基础数据解读**

#### 基础动量数值含义
- **正值**: 当前价格高于N日前价格，显示上涨动量
- **负值**: 当前价格低于N日前价格，显示下跌动量
- **绝对值大小**: 反映动量变化的强度

#### ROC变动率含义
- **roc_5 > 5%**: 超短期强势动量
- **roc_12 在±10%**: 正常波动范围
- **roc_25 > 15%**: 中期强势趋势确立

#### PMO振荡器作用
- **pmo > pmo_signal**: 动量上升趋势
- **pmo < 0**: 整体动量偏弱
- **pmo极值**: 动量达到极端水平

#### 威廉指标含义
- **williams_r > -20**: 超买区域，注意回调风险
- **williams_r < -80**: 超卖区域，关注反弹机会
- **williams_r = -50**: 中性区域分界线

### 🚨 **使用注意事项**

#### 数据局限性
- **滞后特性**: 动量指标基于历史数据，存在一定滞后
- **震荡市失效**: 横盘整理中容易产生假信号
- **参数敏感**: 不同周期参数对结果影响较大

#### 建议分析方法
- **多周期分析**: 结合短中长期动量判断
- **背离分析**: 重点关注momentum_divergence信号
- **波动率分析**: 利用momentum_volatility评估稳定性
- **组合验证**: 与成交量、趋势指标交叉验证

## 📋 中国ETF市场应用策略

### 🎯 **1. 宽基指数ETF应用**
- **沪深300ETF**: 动量信号相对稳定，momentum_strength > 8%时关注
- **中证500ETF**: 波动较大，建议使用momentum_volatility过滤
- **建议阈值**: roc_25 > 10%作为强势筛选标准

### 🏭 **2. 行业主题ETF应用**
- **科技类ETF**: momentum_acceleration变化敏感，适合短期动量策略
- **消费类ETF**: 趋势相对稳定，重点关注momentum_trend状态
- **周期类ETF**: 需结合宏观环境，单独使用效果一般

### 💰 **3. 小盘成长ETF应用**
- **流动性考量**: momentum_volatility > 15的ETF需谨慎
- **背离重视**: momentum_divergence信号在小盘股中更重要
- **建议**: 优先选择pmo > pmo_signal的ETF

## 📊 系统输出格式

### 🎯 **13个核心字段CSV输出**
```csv
code,date,momentum_10,momentum_20,roc_5,roc_12,roc_25,pmo,pmo_signal,williams_r,momentum_strength,momentum_acceleration,momentum_trend,momentum_divergence,momentum_volatility,calc_time
```

### 📝 **字段详细说明**
- `code`: ETF代码 (不含交易所后缀)
- `date`: 计算日期 (YYYY-MM-DD格式)
- `momentum_10`: 10日价格动量 (8位小数精度)
- `momentum_20`: 20日价格动量 (8位小数精度)
- `roc_5`: 5日变动率 (%)
- `roc_12`: 12日变动率 (%)
- `roc_25`: 25日变动率 (%)
- `pmo`: 价格动量振荡器
- `pmo_signal`: PMO信号线
- `williams_r`: 威廉指标 (-100~0)
- `momentum_strength`: 动量强度 (%)
- `momentum_acceleration`: 动量加速度
- `momentum_trend`: 趋势状态 (1/0/-1)
- `momentum_divergence`: 背离信号 (1/0)
- `momentum_volatility`: 动量波动率
- `calc_time`: 计算时间戳

### 📈 **数据格式示例**
```csv
159001,2025-07-30,2.45,1.87,3.25,8.76,-2.14,15.23,12.45,-35.67,8.76,0.58,1,0,4.52,2025-07-30 18:31:39
```

### 📊 **数据示例解读**
- `momentum_10`: 2.45 - 10日内价格上涨2.45元
- `momentum_20`: 1.87 - 20日内价格上涨1.87元
- `roc_5`: 3.25% - 5日内涨幅3.25%
- `roc_25`: -2.14% - 25日内跌幅2.14%
- `pmo > pmo_signal`: 动量上升趋势
- `momentum_trend`: 1 - 当前为上升趋势
- `momentum_divergence`: 0 - 无背离现象

## 🎯 客观数据筛选参考

### 📊 **数据筛选思路** (仅供参考)
基于客观数据的一些常用筛选思路，用户可自行调整：

```python
# 强势动量筛选
def strong_momentum_filter(data):
    return (
        data['roc_25'] > 10 and              # 25日涨幅超过10%
        data['momentum_strength'] > 8 and    # 动量强度大于8%
        data['pmo'] > data['pmo_signal'] and # PMO上升趋势
        data['williams_r'] > -80             # 非超卖状态
    )

# 动量加速筛选
def momentum_acceleration_filter(data):
    return (
        data['momentum_acceleration'] > 0 and    # 动量正加速
        data['roc_5'] > data['roc_12'] and      # 短期动量强于中期
        data['momentum_volatility'] < 10        # 动量相对稳定
    )

# 背离预警筛选
def divergence_warning_filter(data):
    return (
        data['momentum_divergence'] == 1 and    # 出现背离信号
        data['momentum_strength'] > 5 and       # 动量强度足够
        abs(data['williams_r'] + 50) > 30       # 处于极值区域
    )
```

### 🔄 **组合分析建议**
- **动量 + 趋势**: 与MACD配合双重确认
- **动量 + 成交量**: 量价配合验证真实性
- **动量 + 相对强度**: 多ETF动量强弱对比
- **时间周期**: 多周期动量一致性验证

## ⚠️ 使用注意事项

### 🚨 **中国市场特殊性**
1. **T+1交易制度**: 动量信号确认需要延后1个交易日
2. **涨跌停限制**: 连续涨跌停会影响动量指标连续性
3. **ETF套利机制**: 申购赎回对价格动量产生扰动
4. **节假日效应**: 长假前后动量指标可能出现异常

### 📊 **数据质量要求**
- **最少数据量**: 至少30个交易日数据用于计算移动平均
- **异常值处理**: 涨跌停、停牌等特殊情况需要标记
- **复权处理**: 必须使用前复权数据确保连续性

### 🔧 **参数优化建议**
- **动量周期**: 可根据ETF特性调整为5/10/15/20日
- **PMO参数**: (35,20,9)为A股优化参数，可微调
- **背离检测**: 建议5-10个交易日作为判断周期

## 🏆 系统优势

### 📈 **技术特色**
- **13维度覆盖**: 全面覆盖动量分析各个维度
- **双重验证**: 绝对动量+相对变化率双重验证
- **背离检测**: 自动识别价格动量背离现象
- **波动率控制**: 内置动量稳定性评估

### ⚡ **性能优化**
- **向量化计算**: 批量处理500+ ETF，提高计算效率
- **滚动窗口**: 优化内存使用，支持长时间序列
- **并行处理**: 多进程计算，提升系统性能

## 📊 数据依赖最小化

### 🎯 **核心依赖** (仅4个字段)
- **收盘价**: 动量计算的基础数据，必须连续
- **最高价**: 威廉指标计算需要，14日最高价
- **最低价**: 威廉指标计算需要，14日最低价
- **历史数据**: 计算各周期动量需要足够历史数据

### 📋 **数据质量要求**
- **最少数据量**: 至少30个交易日(计算25日ROC+平滑)
- **数据连续性**: 停牌或缺失数据会影响动量计算准确性
- **价格有效性**: 异常价格波动需要特殊处理

## 🔥 计算优先级
⭐ **第二优先级**: 动量指标作为技术分析的核心组件，是识别趋势强弱和转折点的重要工具，建议在RSI系统之后优先实现。

---

**📊 系统状态**: 🔧 设计完成 | **🚀 实现状态**: ⏳ 待开发 | **📅 最后更新**: 2025-07-30