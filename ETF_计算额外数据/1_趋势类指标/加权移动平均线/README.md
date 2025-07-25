# 加权移动平均线 (Weighted Moving Average - WMA) - 重构版

## 系统概述
高性能的加权移动平均线计算系统，采用模块化架构，支持智能缓存和增量计算。WMA通过线性递减权重的方式计算平均值，最近的价格获得最高权重，较早的价格获得较低权重，在敏感性和平滑性之间取得平衡。

## 核心特性
- 📊 **双门槛处理**: 3000万/5000万门槛ETF分别处理  
- 🗂️ **智能缓存**: 增量计算，避免重复计算
- 📁 **目录组织**: 按门槛自动组织输出文件
- 🔧 **自动初始化**: 自动创建目录结构和meta文件，即使删除后也能重新创建
- ⚡ **高性能**: 批量处理，支持大规模ETF计算

## 包含指标

### 短期WMA
- **WMA5**: 5日加权移动平均线 - 超短期趋势
- **WMA10**: 10日加权移动平均线 - 短期趋势

### 中期WMA
- **WMA20**: 20日加权移动平均线 - 中期趋势

## 计算公式
```
WMA(n) = (n×P1 + (n-1)×P2 + ... + 1×Pn) / (n + (n-1) + ... + 1)

其中：
P1 = 最新价格，权重 = n
P2 = 第二新价格，权重 = n-1
...
Pn = 第n个价格，权重 = 1
分母 = n×(n+1)/2
```

## 权重分配示例

### WMA5权重分配
- 今日: 权重 5 (83.3%)
- 昨日: 权重 4 (66.7%) 
- 前2日: 权重 3 (50.0%)
- 前3日: 权重 2 (33.3%)
- 前4日: 权重 1 (16.7%)

### WMA10权重分配  
- 最新5日: 权重10-6 (平均65%)
- 较新5日: 权重5-1 (平均27%)

## 特性对比

### 与SMA对比
- **优势**: 更重视近期价格变化
- **劣势**: 计算相对复杂

### 与EMA对比  
- **优势**: 权重变化更线性、可预测
- **劣势**: 对历史数据依赖更明显

## 应用策略

### 1. 短期趋势判断
- **WMA5**: 捕捉短期价格动量
- **信号**: WMA向上为看涨，向下为看跌

### 2. 趋势确认
- **多重确认**: WMA5 > WMA10 > WMA20 确认上升趋势
- **反转信号**: WMA序列反转预示趋势转换

### 3. 动态支撑阻力
- **上升趋势**: WMA作为动态支撑位
- **下降趋势**: WMA作为动态阻力位

## 适用场景
- ✅ 需要对近期价格变化敏感的策略
- ✅ 短期交易信号生成
- ✅ 趋势加速阶段的确认
- ❌ 需要极度平滑信号的长期投资

## 参数建议
- **日内交易**: WMA3, WMA5
- **短线交易**: WMA5, WMA10
- **中线交易**: WMA10, WMA20

## 目录结构

系统自动生成有序的目录结构：

```
data/
├── 3000万门槛/              # 3000万门槛ETF计算结果
└── 5000万门槛/              # 5000万门槛ETF计算结果

cache/
├── 3000万门槛/              # 缓存文件
├── 5000万门槛/              # 缓存文件
└── meta/
    ├── cache_global_meta.json      # 全局缓存元数据
    ├── 3000万门槛_meta.json        # 3000万门槛元数据
    └── 5000万门槛_meta.json        # 5000万门槛元数据
```

**🔧 自动初始化特性**：
- 系统首次运行时自动创建完整的目录结构
- 自动生成meta文件记录缓存状态和处理历史
- 即使手动删除`data/`、`cache/`或`meta/`目录，系统也能自动重新创建

## 使用方法

### 快速开始
```bash
python wma_main.py                          # 批量处理所有门槛ETF
```

### 单个ETF计算
```bash
python wma_main.py --etf 159303.SZ         # 计算指定ETF
```

### 快速分析
```bash
python wma_main.py --status                # 查看系统状态
python wma_main.py --list                  # 列出可用ETF
```

## 输出格式

每个CSV文件包含以下字段：
- `date`: 日期
- `code`: ETF代码
- `WMA_3`: 3日加权移动平均线
- `WMA_5`: 5日加权移动平均线
- `WMA_10`: 10日加权移动平均线  
- `WMA_20`: 20日加权移动平均线
- `WMA_DIFF_5_20`: WMA5-WMA20差值
- `WMA_DIFF_3_5`: WMA3-WMA5差值
- `WMA_DIFF_5_20_PCT`: WMA5-WMA20相对差值百分比
- `calc_time`: 计算时间

## 性能特性

### 智能缓存
- 自动检测数据更新，只计算新增数据
- 缓存命中率提升处理速度80%+

### 增量计算
- 基于最后计算日期的增量更新
- 避免全量重复计算历史数据
- 显著提升大规模ETF处理效率

### 自动管理
- 系统启动时自动创建必要目录和文件
- 智能检测并恢复缺失的meta文件
- 零配置运行，即开即用

### 批量处理
- 支持双门槛的批量计算
- 并行友好的架构设计
- 进度追踪和错误处理

## 技术架构

```
wma_calculator/
├── controllers/        # 控制层
├── engines/           # 计算引擎
├── infrastructure/    # 基础设施
├── outputs/          # 输出处理
└── interfaces/       # 接口定义
```

## 系统要求
- Python 3.7+
- pandas, numpy
- 充足的磁盘空间用于缓存

## 更新日志
- v2.0.1: 新增自动目录创建和meta文件管理功能
- v2.0.0: 重构版发布，模块化架构，智能缓存
- v1.0.0: 基础版本，WMA计算

## 故障排除

### 常见问题
1. **目录或文件缺失**：系统会自动创建，无需手动操作
2. **缓存不一致**：删除`cache/`目录后重新运行即可
3. **meta文件损坏**：系统会自动检测并重新创建

### 完全重置
如需完全重置系统状态：
```bash
rm -rf data/ cache/
python wma_main.py --status  # 系统会自动重新创建所有目录
``` 