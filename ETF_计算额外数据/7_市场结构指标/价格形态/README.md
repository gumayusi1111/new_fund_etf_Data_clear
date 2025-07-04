# 价格形态 (Price Patterns)

## 概述
价格形态是基于价格序列的高低点关系来识别市场结构变化的技术分析方法，帮助判断趋势的延续或反转。

## 基础概念

### 高低点定义
- **高点 (Peak)**: 当前价格 > 前后N日价格
- **低点 (Trough)**: 当前价格 < 前后N日价格
- **波段高点**: 相对高点中的最高点
- **波段低点**: 相对低点中的最低点

## 主要形态识别

### 1. 趋势结构形态

#### 上升趋势结构
- **更高高点 (Higher High, HH)**: 新高点 > 前一个高点
- **更高低点 (Higher Low, HL)**: 新低点 > 前一个低点
- **特征**: HH + HL = 健康上升趋势
- **应用**: 趋势延续确认

#### 下降趋势结构
- **更低高点 (Lower High, LH)**: 新高点 < 前一个高点
- **更低低点 (Lower Low, LL)**: 新低点 < 前一个低点
- **特征**: LH + LL = 健康下降趋势
- **应用**: 趋势延续确认

#### 趋势反转结构
- **上升趋势反转**: HH后出现LL
- **下降趋势反转**: LL后出现HH
- **双重结构**: 同时出现LH和HL

### 2. 整理形态

#### 三角形整理
- **上升三角形**: 水平阻力线 + 上升支撑线
- **下降三角形**: 水平支撑线 + 下降阻力线
- **对称三角形**: 上升支撑线 + 下降阻力线
- **突破方向**: 通常延续原趋势

#### 矩形整理
- **特征**: 水平支撑线 + 水平阻力线
- **应用**: 横盘震荡，等待突破
- **交易**: 高抛低吸或突破跟进

#### 楔形整理
- **上升楔形**: 两条向上收敛的趋势线
- **下降楔形**: 两条向下收敛的趋势线
- **特点**: 通常是反转形态

### 3. 反转形态

#### 双顶双底 (Double Top/Bottom)
- **双顶**: 两个相近的高点，中间有一个低点
- **双底**: 两个相近的低点，中间有一个高点
- **确认**: 突破颈线位
- **目标位**: 形态高度的投影

#### 头肩形态 (Head and Shoulders)
- **头肩顶**: 左肩 + 头部 + 右肩 (头部最高)
- **头肩底**: 左肩 + 头部 + 右肩 (头部最低)
- **颈线**: 连接两个肩部的线
- **目标位**: 头部到颈线距离的投影

#### 三重顶底 (Triple Top/Bottom)
- **特征**: 三个相近的高点或低点
- **确认**: 突破颈线
- **可靠性**: 比双顶双底更可靠

## 计算方法

### 高低点识别算法
```python
def identify_peaks_troughs(data, window=5):
    peaks = []
    troughs = []
    
    for i in range(window, len(data) - window):
        # 高点判断
        if all(data[i] >= data[j] for j in range(i-window, i+window+1) if j != i):
            peaks.append((i, data[i]))
        
        # 低点判断  
        if all(data[i] <= data[j] for j in range(i-window, i+window+1) if j != i):
            troughs.append((i, data[i]))
    
    return peaks, troughs
```

### 趋势结构判断
```python
def analyze_trend_structure(peaks, troughs):
    higher_highs = 0
    higher_lows = 0
    lower_highs = 0
    lower_lows = 0
    
    # 分析高点序列
    for i in range(1, len(peaks)):
        if peaks[i][1] > peaks[i-1][1]:
            higher_highs += 1
        else:
            lower_highs += 1
    
    # 分析低点序列
    for i in range(1, len(troughs)):
        if troughs[i][1] > troughs[i-1][1]:
            higher_lows += 1
        else:
            lower_lows += 1
    
    return {
        'HH': higher_highs,
        'HL': higher_lows, 
        'LH': lower_highs,
        'LL': lower_lows
    }
```

## 输出字段
- `Higher_High`: 更高高点标识
- `Higher_Low`: 更高低点标识
- `Lower_High`: 更低高点标识
- `Lower_Low`: 更低低点标识
- `Peak_Points`: 波段高点位置
- `Trough_Points`: 波段低点位置
- `Trend_Structure`: 趋势结构评分
- `Pattern_Type`: 识别的形态类型
- `Pattern_Completion`: 形态完成度

## 应用策略

### 1. 趋势跟踪策略
```python
# 上升趋势跟踪
if trend_structure['HH'] > 0 and trend_structure['HL'] > 0:
    signal = "UPTREND_CONFIRMED"
    
# 下降趋势跟踪
if trend_structure['LH'] > 0 and trend_structure['LL'] > 0:
    signal = "DOWNTREND_CONFIRMED"
```

### 2. 反转交易策略
```python
# 趋势反转信号
if last_structure == "HH" and current_structure == "LL":
    signal = "TREND_REVERSAL_DOWN"
elif last_structure == "LL" and current_structure == "HH":
    signal = "TREND_REVERSAL_UP"
```

### 3. 形态突破策略
- **三角形突破**: 等待方向选择
- **头肩形态**: 颈线突破确认
- **双顶双底**: 关键位置突破

## 形态可靠性评估

### 高可靠性指标
1. **成交量配合**: 突破伴随放量
2. **时间充分**: 形态酝酿时间足够
3. **幅度合理**: 回撤和反弹幅度适中
4. **多重确认**: 多个时间框架共振

### 低可靠性警示
1. **成交量萎缩**: 突破无量或缩量
2. **时间过短**: 形态形成时间不足
3. **幅度异常**: 过度反弹或回撤
4. **基本面冲突**: 与基本面趋势相反

## 实战技巧

### 1. 多时间框架分析
- **大级别**: 确定主趋势方向
- **中级别**: 寻找交易机会
- **小级别**: 精确入场点位

### 2. 关键位置判断
- **支撑阻力**: 前期高低点
- **整数关口**: 心理价位
- **均线位置**: 重要均线支撑

### 3. 风险控制
- **止损设置**: 形态关键点位破位
- **仓位管理**: 根据形态可靠性调整
- **时间止损**: 形态失效的时间限制

## 组合分析
- **与成交量分析**: 量价结构确认
- **与技术指标**: RSI背离 + 价格形态
- **与趋势线**: 趋势线 + 价格形态共振
- **与周期分析**: 时间窗口 + 价格形态

## 注意事项
- 价格形态具有一定的主观性
- 需要充分的历史数据支持
- 市场环境变化可能影响形态有效性
- 建议结合其他分析方法使用 