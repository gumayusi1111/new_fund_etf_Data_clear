# 📈 第一大类：趋势类指标系统

## 🎯 系统概述

趋势类指标是技术分析中最基础和重要的指标类别，用于识别价格趋势的方向和强度。本系统包含四个核心子指标，采用统一的模块化架构，支持智能缓存和增量计算，基于ETF初筛结果进行批量计算。

## 🚀 系统特性

### 📊 **核心优势**
- ✅ **智能缓存系统**: 96.5%+ 缓存命中率，增量计算避免重复
- ✅ **双门槛处理**: 3000万/5000万门槛ETF分别处理优化
- ✅ **自动初始化**: 目录结构和元数据自动创建管理
- ✅ **高性能计算**: 向量化计算，批量处理大规模ETF
- ✅ **统一架构**: 六层模块化设计，高内聚低耦合

### 📁 **数据来源**
- **ETF初筛结果**: 基于 `ETF_初筛/data/门槛/通过筛选ETF.txt`
- **3000万门槛**: 290个通过筛选的ETF
- **5000万门槛**: 235个通过筛选的ETF  
- **数据精度**: 统一8位小数精度，ISO日期格式

## 📋 四个子指标系统

### 1. 📊 移动平均线 (SMA) 系统

#### 🎯 **输出字段**
```csv
code,date,SMA_5,SMA_10,SMA_20,SMA_60,SMA_DIFF_5_20,SMA_DIFF_5_20_PCT,SMA_DIFF_5_10
```

#### 📈 **核心指标**
- **SMA_5**: 5日简单移动平均线 (超短期趋势)
- **SMA_10**: 10日简单移动平均线 (短期趋势)  
- **SMA_20**: 20日简单移动平均线 (月线趋势)
- **SMA_60**: 60日简单移动平均线 (季线趋势)

#### 📊 **差值指标**
- **SMA_DIFF_5_20**: SMA_5 - SMA_20 (核心趋势强度)
- **SMA_DIFF_5_20_PCT**: 相对差值百分比 ((SMA_5 - SMA_20) / SMA_20 × 100)
- **SMA_DIFF_5_10**: SMA_5 - SMA_10 (短期动量)

#### ⚙️ **计算公式**
```python
SMA_n = Σ(收盘价[i]) / n  # 最近n天收盘价的算术平均值
差值 = SMA_短期 - SMA_长期
相对差值百分比 = (差值 / SMA_长期) × 100
```

#### 🚀 **启动方式**
```bash
cd 移动平均线/
python sma_main.py                     # 默认：超高性能向量化计算
python sma_main.py --status            # 系统状态检查
python sma_main.py --etf 159001        # 单个ETF计算
python sma_main.py --threshold "3000万门槛"  # 指定门槛
python sma_main.py --historical-batch  # 批量历史计算
```

### 2. 📈 指数移动平均线 (EMA) 系统

#### 🎯 **输出字段**
```csv
code,date,EMA_12,EMA_26,EMA_DIFF_12_26,EMA_DIFF_12_26_PCT,EMA12_MOMENTUM
```

#### 📊 **核心指标**
- **EMA_12**: 12日指数移动平均线 (快线)
- **EMA_26**: 26日指数移动平均线 (慢线)
- **EMA_DIFF_12_26**: 12日与26日EMA差值
- **EMA_DIFF_12_26_PCT**: EMA差值百分比
- **EMA12_MOMENTUM**: EMA12动量指标

#### ⚙️ **计算公式**
```python
α = 2 / (周期 + 1)  # 平滑因子
EMA_today = α × 价格_today + (1-α) × EMA_yesterday
EMA差值 = EMA_12 - EMA_26
相对差值百分比 = (EMA差值 / EMA_26) × 100
动量 = EMA12_today - EMA12_yesterday
```

#### 🚀 **启动方式**
```bash
cd 指数移动平均线/
python ema_main.py                     # 默认：批量计算所有门槛
python ema_main.py --etf 510050.SH     # 计算单个ETF
python ema_main.py --screening --threshold 3000万门槛  # 指定门槛
python ema_main.py --quick 510050.SH   # 快速分析
python ema_main.py --validate 510050.SH # 验证计算正确性
```

### 3. ⚖️ 加权移动平均线 (WMA) 系统

#### 🎯 **输出字段**
```csv
code,date,WMA_3,WMA_5,WMA_10,WMA_20,WMA_DIFF_5_20,WMA_DIFF_5_20_PCT,WMA_DIFF_3_5
```

#### 📊 **核心指标**
- **WMA_3**: 3日加权移动平均线 (超短期)
- **WMA_5**: 5日加权移动平均线 (短期)
- **WMA_10**: 10日加权移动平均线 (中期)
- **WMA_20**: 20日加权移动平均线 (长期)

#### 📈 **差值指标**
- **WMA_DIFF_5_20**: 5日与20日WMA差值
- **WMA_DIFF_5_20_PCT**: WMA差值百分比
- **WMA_DIFF_3_5**: 3日与5日WMA差值

#### ⚙️ **计算公式**
```python
WMA = Σ(价格_i × 权重_i) / Σ(权重_i)
其中 权重_i = i (i = 1, 2, ..., n)，最新价格权重最大
权重 = [1, 2, 3, ..., n]
差值 = WMA_短期 - WMA_长期
相对差值百分比 = (差值 / WMA_长期) × 100
```

#### 🚀 **启动方式**
```bash
cd 加权移动平均线/
python wma_main.py                     # 默认：完整批量计算
python wma_main.py --etf 159201        # 单个ETF计算
python wma_main.py --threshold 3000万门槛  # 指定门槛
python wma_main.py --quick 159201      # 快速分析
python wma_main.py --status            # 系统状态
```

### 4. 📊 MACD指标组合系统

#### 🎯 **输出字段**
```csv
date,code,ema_fast,ema_slow,dif,dea,macd_bar,calc_time
```

#### 📈 **三参数配置**

##### 🔸 **标准参数** (Standard)
- **快线周期**: 12日 | **慢线周期**: 26日 | **信号线周期**: 9日
- **适用**: 通用场景，平衡灵敏度和稳定性

##### 🔸 **敏感参数** (Sensitive)  
- **快线周期**: 8日 | **慢线周期**: 17日 | **信号线周期**: 9日
- **适用**: 短期交易，更快响应价格变化

##### 🔸 **平滑参数** (Smooth)
- **快线周期**: 19日 | **慢线周期**: 39日 | **信号线周期**: 9日
- **适用**: 长期分析，减少噪声干扰

#### 📊 **核心指标**
- **ema_fast**: 快速指数移动平均
- **ema_slow**: 慢速指数移动平均  
- **dif**: DIF差离值 (ema_fast - ema_slow)
- **dea**: DEA信号线 (DIF的9日EMA)
- **macd_bar**: MACD柱状图 ((dif - dea) × 2)

#### ⚙️ **计算公式**
```python
EMA_fast = EMA(收盘价, 快线周期)
EMA_slow = EMA(收盘价, 慢线周期)  
DIF = EMA_fast - EMA_slow
DEA = EMA(DIF, 9)
MACD = (DIF - DEA) × 2
```

#### 🚀 **启动方式**
```bash
cd MACD指标组合/
python macd_main.py                    # 默认：增量更新所有参数MACD
python macd_main.py --etf 510050.SH    # 计算单个ETF
python macd_main.py --parameter-set sensitive  # 使用敏感参数
python macd_main.py --quick 510050.SH  # 快速分析
python macd_main.py --vectorized       # 向量化历史计算
```

## 🗂️ 文件组织结构

### 📁 **统一目录架构**
```
1_趋势类指标/
├── 📊 移动平均线/
│   ├── cache/门槛/ETF代码.csv      # 缓存文件
│   ├── data/门槛/ETF代码.csv       # 最终输出
│   ├── sma_calculator/            # 六层模块化架构
│   └── sma_main.py               # 主程序入口
│
├── 📈 指数移动平均线/
│   ├── cache/门槛/ETF代码.csv
│   ├── data/门槛/ETF代码.csv
│   ├── ema_calculator/
│   └── ema_main.py
│
├── ⚖️ 加权移动平均线/
│   ├── cache/门槛/ETF代码.csv
│   ├── data/门槛/ETF代码.csv
│   ├── wma_calculator/
│   └── wma_main.py
│
└── 📊 MACD指标组合/
    ├── cache/门槛/参数类型/ETF代码.csv
    ├── data/门槛/参数类型/ETF代码.csv
    ├── macd_calculator/
    └── macd_main.py
```

### 🗄️ **Meta文件管理**
```
cache/meta/
├── 3000万门槛_meta.json           # 3000万门槛元数据
├── 5000万门槛_meta.json           # 5000万门槛元数据
└── cache_global_meta.json        # 全局缓存元数据
```

## ⚡ 性能指标

### 📊 **处理效率**
- **SMA系统**: 7.44秒处理519个ETF (70+ ETF/秒)
- **缓存命中率**: 96.5%+ 智能增量更新
- **成功率**: 98.6% 稳定可靠
- **内存使用**: < 100MB 单次批量处理

### 🔄 **更新机制**
- **数据依赖**: ETF日更/周更数据自动更新
- **缓存策略**: 基于文件修改时间的智能失效
- **增量计算**: 只处理新增或变更数据
- **更新频率**: 建议每日更新（第一优先级）

## 🎯 应用场景

### 📈 **投资分析**
1. **趋势确认**: 判断当前市场趋势方向和强度
2. **入场时机**: 识别趋势开始和加速阶段  
3. **出场信号**: 捕捉趋势衰竭和转折点
4. **支撑阻力**: 移动平均线作为动态支撑阻力位

### 🔍 **技术分析**
- **金叉死叉**: 短期均线与长期均线交叉信号
- **MACD信号**: DIF与DEA线交叉，柱状图变化
- **趋势强度**: 通过差值和百分比量化趋势力度
- **动量分析**: EMA动量指标识别加速减速

## 🚀 快速开始

### 🎮 **批量运行（推荐）**
```bash
# 在1_趋势类指标目录下运行所有四个系统
python 移动平均线/sma_main.py          # SMA系统
python 指数移动平均线/ema_main.py        # EMA系统  
python 加权移动平均线/wma_main.py        # WMA系统
python MACD指标组合/macd_main.py       # MACD系统
```

### 🎯 **单系统运行**
```bash
# 进入具体系统目录
cd 移动平均线/
python sma_main.py --status            # 查看系统状态
python sma_main.py                     # 执行计算
```

## 🔧 系统要求

### 📦 **依赖包**
```bash
pip install pandas numpy argparse
```

### 💾 **硬件要求**
- **内存**: 建议4GB+
- **磁盘**: 预留2GB+缓存空间  
- **CPU**: 支持多核并行计算

## 📚 更新日志

- **v2.1**: 新增大类控制器架构，统一管理四个子系统
- **v2.0**: 完成四个子系统重构，模块化架构，智能缓存
- **v1.5**: 增加MACD三参数配置，提升计算精度
- **v1.0**: 基础版本，四个子指标分别实现

## 🎯 计算优先级
🔥 **第一优先级**: 趋势类指标是技术分析的核心基础，建议每日更新 