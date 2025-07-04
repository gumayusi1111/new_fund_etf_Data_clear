# 📈 趋势类指标

## 指标分类概述
趋势类指标主要用于识别价格趋势的方向和强度，是技术分析中最基础和重要的指标类别。

## 包含子分类

### 1. 移动平均线 (Moving Average)
- **简单移动平均线 (SMA)**: MA5、MA10、MA20、MA60、MA120、MA250
- **功能**: 平滑价格波动，识别趋势方向
- **应用**: 金叉死叉信号、支撑阻力判断

### 2. 指数移动平均线 (Exponential Moving Average)  
- **指数移动平均线 (EMA)**: EMA12、EMA26、EMA9
- **功能**: 对近期价格赋予更高权重，反应更灵敏
- **应用**: MACD指标计算基础、短期趋势判断

### 3. 加权移动平均线 (Weighted Moving Average)
- **加权移动平均线 (WMA)**: WMA5、WMA10、WMA20
- **功能**: 线性加权平均，平衡敏感性和平滑性
- **应用**: 趋势确认、价格预测

### 4. MACD指标组合
- **DIF线**: 快线与慢线差值 (EMA12 - EMA26)
- **DEA线**: 信号线 (DIF的9日EMA)
- **MACD柱**: 差值柱状图 (DIF - DEA) × 2
- **功能**: 趋势转折点识别、买卖信号生成
- **应用**: 金叉死叉、背离分析

## 数据依赖
- **必需字段**: 收盘价、历史价格数据
- **计算周期**: 建议至少250个交易日数据
- **更新频率**: 每日更新

## 应用场景
1. **趋势确认**: 判断当前市场趋势方向
2. **入场时机**: 识别趋势开始和加速阶段  
3. **出场信号**: 捕捉趋势衰竭和转折点
4. **支撑阻力**: 移动平均线作为动态支撑阻力位

## 计算优先级
🔥 **第一优先级**: 所有趋势指标都是核心基础指标，建议优先实现 