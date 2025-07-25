# 🎯 支撑阻力指标

## 指标分类概述
支撑阻力指标用于识别关键价格位，帮助判断价格反转点和突破点，是技术分析中预测价格行为的重要工具。

## 包含子分类

### 1. 枢轴点 (Pivot Point)
- **基准枢轴点**: PP = (最高价 + 最低价 + 收盘价) / 3
- **支撑位1**: S1 = 2 × PP - 最高价
- **支撑位2**: S2 = PP - (最高价 - 最低价)
- **阻力位1**: R1 = 2 × PP - 最低价
- **阻力位2**: R2 = PP + (最高价 - 最低价)
- **功能**: 提供日内交易的关键价格位

### 2. 价格通道
- **高点通道**: 基于N日最高价构建的上轨道
- **低点通道**: 基于N日最低价构建的下轨道
- **中位通道**: 高低点通道的中线
- **通道宽度**: 反映价格波动范围
- **功能**: 确定价格运行区间

### 3. 动态支撑阻力
- **移动平均支撑**: 各周期均线作为动态支撑阻力
- **趋势线**: 基于价格高低点的趋势线
- **成本均线**: 基于成交量加权的价格均线
- **功能**: 提供动态的支撑阻力参考

### 4. 关键价格位
- **前期高低点**: 历史重要高低点位置
- **整数关口**: 重要的整数价格位
- **斐波那契回调**: 基于前期波段的回调位
- **功能**: 心理价位和技术价位识别

## 数据依赖
- **必需字段**: 最高价、最低价、收盘价
- **历史数据**: 至少20-50个交易日数据
- **更新频率**: 每日更新

## 计算方法

### 标准枢轴点
```
PP = (H + L + C) / 3
R1 = 2 × PP - L
R2 = PP + (H - L)  
S1 = 2 × PP - H
S2 = PP - (H - L)
```

### 价格通道
```
上轨 = MAX(高价, N天)
下轨 = MIN(低价, N天)
中轨 = (上轨 + 下轨) / 2
通道宽度 = (上轨 - 下轨) / 中轨 × 100%
```

## 应用策略

### 1. 枢轴点交易
- **突破策略**: 价格突破R1/S1的方向性交易
- **反转策略**: 价格触及R2/S2的反向交易
- **区间交易**: 在PP附近的震荡交易

### 2. 通道交易
- **通道突破**: 价格突破上下轨的趋势跟随
- **通道反弹**: 价格在轨道边缘的反转交易
- **通道收缩**: 低波动率后的突破准备

### 3. 动态支撑阻力
- **均线支撑**: 价格回调至重要均线的买入机会
- **均线阻力**: 价格上升至均线的卖出压力
- **多均线排列**: 均线系统的整体支撑阻力效应

## 信号强度

### 强信号
- **多重确认**: 多个支撑阻力位重合
- **大幅突破**: 放量突破关键位置
- **历史验证**: 该位置多次起到支撑阻力作用

### 中等信号
- **单一位置**: 仅一个指标显示的关键位
- **普通突破**: 小幅度突破支撑阻力位
- **短期验证**: 近期形成的支撑阻力位

### 弱信号
- **模糊位置**: 支撑阻力位不够明确
- **无量突破**: 成交量未配合的突破
- **首次测试**: 未经验证的新形成位置

## 时间周期选择

### 日内交易
- **枢轴点**: 使用前一日数据计算
- **更新频率**: 每日更新一次

### 短线交易  
- **价格通道**: 5-10日通道
- **动态支撑**: 短期均线系统

### 中长线投资
- **价格通道**: 20-60日通道  
- **动态支撑**: 中长期均线系统

## 适用场景
- ✅ 区间震荡市场
- ✅ 趋势市场的回调买入
- ✅ 突破交易策略
- ✅ 止损止盈位设置

## 计算优先级
⭐ **第二优先级**: 支撑阻力指标是实用的交易工具

## 实战应用技巧
- **多重验证**: 结合多个支撑阻力指标
- **量价配合**: 突破需要成交量确认
- **风险控制**: 基于支撑阻力位设置止损
- **灵活调整**: 根据市场环境调整参数 