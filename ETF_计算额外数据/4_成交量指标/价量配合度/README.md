# 价量配合度 (Price-Volume Coordination)

## 概述
价量配合度是分析价格变动与成交量变化关系的重要指标，揭示市场参与者的真实意图和趋势的可靠性。

## 核心理论
**量在价先**: 成交量往往先于价格发生变化
**量价同向**: 健康的趋势需要成交量配合
**量价背离**: 可能预示趋势反转

## 主要指标

### 1. 价量相关性 (Price-Volume Correlation)
- **计算**: 价格变化与成交量变化的相关系数
- **周期**: 10日、20日、30日相关性
- **意义**: 正相关表示量价配合良好

### 2. 量价趋势指标 (Volume Price Trend)
- **公式**: VPT = 前日VPT + 成交量 × (收盘价变化率)
- **特点**: 累积量价信息
- **用途**: 确认价格趋势

### 3. 价量强度比 (Price-Volume Strength Ratio)
- **计算**: 价格变化幅度 / 成交量变化幅度
- **应用**: 判断价格变动的质量
- **标准**: 比值越高，价格效率越好

## 价量配合模式

### 1. 理想配合模式
- **价涨量增**: 多头力量强劲，趋势可靠
- **价跌量增**: 空头力量占优，下跌趋势确立
- **价涨量平**: 温和上涨，可持续性较强
- **价跌量缩**: 下跌动能减弱，可能止跌

### 2. 背离警示模式
- **价涨量缩**: 上涨动能不足，警惕回调
- **价跌量缩**: 卖压减轻，可能探底回升
- **价平量增**: 多空分歧加大，变盘在即
- **价涨量巨**: 可能是顶部放量，注意风险

## 输出字段
- `Price_Volume_Correlation`: 价量相关系数
- `VPT`: 量价趋势指标
- `PV_Strength_Ratio`: 价量强度比
- `Volume_Quality`: 成交量质量评分
- `Trend_Reliability`: 趋势可靠性评分

## 实战应用

### 1. 趋势确认
```
强势上涨: 价格 ↑ + 成交量 ↑ + 相关性 > 0.6
弱势上涨: 价格 ↑ + 成交量 ↓ + 相关性 < 0.3
```

### 2. 买卖时机
- **买入信号**: 价格突破 + 成交量放大 + VPT向上
- **卖出信号**: 价格新高 + 成交量萎缩 + VPT背离

### 3. 风险控制
- 当价量背离超过3个交易日，降低仓位
- VPT与价格背离时，谨慎追高

## 组合策略
- **与MACD配合**: VPT确认MACD金叉/死叉
- **与布林带配合**: 价量配合的突破更可靠
- **与RSI配合**: 避免在RSI超买时追高

## 市场环境适应性
- **强势市场**: 注重量价齐升
- **弱势市场**: 关注量缩止跌信号
- **震荡市场**: 利用价量背离把握拐点

## 注意事项
- 大盘指数ETF的价量关系更稳定
- 主题ETF可能出现非理性价量关系
- 需要结合基本面分析，避免单纯技术分析
- 量价分析在突发事件时可能失效 