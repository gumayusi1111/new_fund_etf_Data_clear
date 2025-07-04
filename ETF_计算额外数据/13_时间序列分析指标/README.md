# 13. 时间序列分析指标

## 概述
时间序列分析指标利用时间维度的信息，揭示ETF价格在时间轴上的规律性、周期性和异常性。这类指标是现代量化投资和算法交易的重要工具。

## 数据需求
- **时间序列价格**: 连续的日期和收盘价数据
- **交易日历**: 识别交易日和非交易日
- **历史数据**: 至少1年以上的历史数据
- **基准数据**: 市场指数数据(用于相对分析)

## 主要指标分类

### 📅 季节性分析指标
- **月度效应**: 不同月份的收益率特征
- **星期效应**: 一周内不同交易日的表现
- **节假日效应**: 节假日前后的价格行为
- **年末效应**: 年初年末的特殊表现

### 🔄 周期性指标
- **傅里叶分析**: 识别价格中的周期性成分
- **自相关分析**: 价格序列的自我相关性
- **周期强度指标**: 周期性的显著程度
- **相位分析**: 周期性波动的相位特征
- **周期性检测**: 综合方法检测市场周期

### 📈 趋势分解指标
- **HP滤波趋势**: Hodrick-Prescott滤波提取的趋势
- **线性趋势**: 最小二乘法拟合的趋势线
- **趋势强度**: 趋势组件的解释能力
- **趋势变化点**: 趋势方向改变的时点
- **趋势分解**: 多方法趋势成分分离

### 🚨 异常检测指标
- **Z-Score异常**: 基于标准差的异常检测
- **IQR异常**: 基于四分位数范围的异常检测
- **移动中位数异常**: 基于移动中位数的偏离
- **极值比例**: 异常值在总样本中的比例

## 详细计算方法

### 月度效应分析
```
月度收益率 = 每月最后一天收盘价 / 每月第一天收盘价 - 1
月度平均收益 = 各年同月收益率的平均值
月度标准差 = 各年同月收益率的标准差
月度夏普比率 = 月度平均收益 / 月度标准差
```

### 星期效应分析
```
周一到周五收益率 = (当日收盘价 / 前一交易日收盘价) - 1
各交易日平均收益 = 该交易日所有收益率的平均值
显著性检验 = t检验或方差分析
```

### 自相关分析
```
滞后k期自相关系数 = Corr(Rt, Rt-k)
其中 Rt 为第t期的收益率
Ljung-Box检验 = 检验自相关的显著性
```

### HP滤波趋势提取
```
最小化: Σ(yt - τt)² + λΣ[(τt+1 - τt) - (τt - τt-1)]²
其中 yt为观测值，τt为趋势值，λ为平滑参数
```

### Z-Score异常检测
```
Z-Score = (Xt - μ) / σ
其中 Xt为当期观测值，μ为均值，σ为标准差
异常阈值 = |Z-Score| > 2 或 3
```

## 实用性评级

### 🔥 极高实用性
- **月度效应**: 制定投资日历的重要依据
- **异常检测**: 识别重要事件和机会
- **趋势分解**: 理解价格的基本走势

### ⭐ 高实用性
- **自相关分析**: 验证市场有效性假设
- **周期性分析**: 中长期投资策略制定
- **星期效应**: 短期交易策略优化

### 📈 中等实用性
- **相位分析**: 高频交易和套利策略
- **HP滤波**: 学术研究和模型构建
- **傅里叶分析**: 复杂量化策略的组件

## 应用场景

### 📊 投资策略制定
- **季节性配置**: 根据月度效应调整仓位
- **择时策略**: 利用周期性进行买卖时机选择
- **风险管理**: 基于异常检测设置风控阈值

### 🎯 量化交易
- **日内策略**: 利用星期效应优化开平仓时机
- **套利策略**: 利用周期性差异进行跨期套利
- **高频交易**: 基于短期自相关进行快速交易

### 📈 投资研究
- **市场有效性研究**: 通过自相关分析验证
- **行为金融研究**: 季节性效应的心理学解释
- **风险建模**: 基于时间序列特征的风险模型

## 参数设置建议

### 📅 时间窗口设置
- **短期分析**: 3-6个月数据
- **中期分析**: 1-3年数据
- **长期分析**: 5年以上数据

### ⚙️ 技术参数
- **HP滤波λ值**: 日数据使用1600，月数据使用100
- **异常检测阈值**: Z-Score > 2.5或3
- **自相关最大滞后**: 数据长度的1/4

## 注意事项

### ⚠️ 数据要求
- **数据完整性**: 避免数据缺失影响分析结果
- **数据质量**: 确保价格数据的准确性
- **样本大小**: 确保足够的样本量支撑统计分析

### 🔍 分析限制
- **结构性变化**: 市场制度变化可能使历史规律失效
- **过度拟合**: 避免在样本内过度优化参数
- **统计显著性**: 确保发现的规律具有统计学意义

### 💡 最佳实践
- **滚动分析**: 使用滚动窗口验证规律的稳定性
- **样本外验证**: 在样本外数据上验证模型效果
- **多维度确认**: 结合其他分析方法确认结论 