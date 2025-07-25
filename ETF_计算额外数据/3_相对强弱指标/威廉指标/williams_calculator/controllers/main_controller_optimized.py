"""
威廉指标优化主控制器
集成所有性能优化和bug修复

优化内容：
1. 集成优化版本的计算引擎和数据读取器
2. 增量更新支持
3. 向量化计算提升
4. 修复计算逻辑bug
5. 优化内存使用和错误处理
"""

import os
import sys
from datetime import datetime
import traceback

# 常量定义
PROJECT_ROOT_LEVELS = 6  # 向上查找项目根目录的层级数
PROGRESS_DISPLAY_INTERVAL = 10  # 批量处理时显示进度的间隔
MAX_FAILED_ETF_DISPLAY = 10  # 显示失败ETF的最大数量

# 添加项目根目录到Python路径
current_file_path = os.path.abspath(__file__)
project_root = current_file_path
for _ in range(PROJECT_ROOT_LEVELS):  # 向上查找到达data_clear目录
    project_root = os.path.dirname(project_root)
    if os.path.basename(project_root) == "data_clear":
        break

if project_root not in sys.path:
    sys.path.insert(0, project_root)

# 导入优化版本的模块
try:
    # 添加当前包路径
    current_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(current_dir)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)

    from infrastructure.config import WilliamsConfig
    from infrastructure.data_reader_optimized import WilliamsDataReaderOptimized
    from infrastructure.cache_manager import WilliamsCacheManager
    from engines.williams_engine_optimized import WilliamsEngineOptimized
    from outputs.csv_handler import WilliamsCSVHandler

except ImportError as e:
    print(f"❌ 导入威廉指标优化模块失败: {str(e)}")
    raise


class WilliamsMainControllerOptimized:
    """威廉指标优化主业务控制器"""

    def __init__(self, adj_type="前复权", use_optimized_components=True):
        """
        初始化优化主控制器

        Args:
            adj_type: 复权类型，默认为"前复权"
            use_optimized_components: 是否使用优化组件
        """
        try:
            # 初始化配置
            self.config = WilliamsConfig(adj_type=adj_type)
            self.use_optimized_components = use_optimized_components

            # 验证配置
            validation = self.config.validate_config()
            if not validation["is_valid"]:
                for error in validation["errors"]:
                    print(f"❌ 配置错误: {error}")
                raise ValueError("配置验证失败")

            # 初始化各模块（使用优化版本）
            self._initialize_optimized_modules()

            # 初始化统计信息
            self._initialize_statistics()

            print("✅ 威廉指标优化主控制器初始化成功")

        except Exception as e:
            print(f"❌ 威廉指标优化主控制器初始化失败: {str(e)}")
            raise

    def _initialize_optimized_modules(self):
        """初始化优化版本的功能模块"""
        try:
            if self.use_optimized_components:
                # 使用优化版本的组件
                self.data_reader = WilliamsDataReaderOptimized(self.config)
                self.williams_engine = WilliamsEngineOptimized(self.config)
                print("🚀 使用优化版本组件")
            else:
                # 回退到原版本组件
                from infrastructure.data_reader import WilliamsDataReader
                from engines.williams_engine import WilliamsEngine

                self.data_reader = WilliamsDataReader(self.config)
                self.williams_engine = WilliamsEngine(self.config)
                print("📊 使用原版本组件")

            # 缓存和输出组件保持不变
            self.cache_manager = WilliamsCacheManager(self.config)
            self.csv_handler = WilliamsCSVHandler(self.config)

            print("🔧 威廉指标优化功能模块初始化完成")

        except Exception as e:
            print(f"❌ 优化功能模块初始化失败: {str(e)}")
            raise

    def _initialize_statistics(self):
        """初始化统计信息"""
        self.statistics = {
            "processed_etfs": 0,
            "successful_calculations": 0,
            "failed_calculations": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "incremental_updates": 0,
            "full_calculations": 0,
            "start_time": None,
            "end_time": None,
            "failed_etf_list": [],
            "optimization_metrics": {
                "total_calculation_time_ms": 0,
                "average_calculation_time_ms": 0,
                "vectorization_benefits": 0,
            },
        }

    def calculate_single_etf_optimized(
        self, etf_code, threshold, save_result=True, use_incremental=True
    ):
        """
        优化的单个ETF威廉指标计算

        优化内容：
        1. 支持增量更新
        2. 性能监控
        3. 智能缓存策略

        Args:
            etf_code: ETF代码
            threshold: 门槛值('3000万门槛' or '5000万门槛')
            save_result: 是否保存结果，默认True
            use_incremental: 是否使用增量更新，默认True

        Returns:
            dict: 计算结果
        """
        try:
            calculation_start_time = datetime.now()
            print(f"🚀 开始优化计算威廉指标: {etf_code} ({threshold})")

            self.statistics["processed_etfs"] += 1

            # 1. 读取ETF数据
            etf_data = self.data_reader.read_etf_data(etf_code)
            if etf_data is None or etf_data.empty:
                error_msg = f"无法读取ETF数据: {etf_code}"
                print(f"⚠️ {error_msg}")
                self.statistics["failed_calculations"] += 1
                self.statistics["failed_etf_list"].append(etf_code)
                return {"success": False, "error": error_msg, "etf_code": etf_code}

            # 2. 智能缓存检查
            source_file_path = self.data_reader._find_etf_data_file(
                self.data_reader._clean_etf_code(etf_code)
            )

            cached_data = None

            if source_file_path and self.cache_manager.is_cache_valid_optimized(
                etf_code, threshold, source_file_path
            ):
                # 缓存有效，考虑增量更新
                cached_data = self.cache_manager.load_etf_cache(etf_code, threshold)
                if not cached_data.empty:
                    if use_incremental:
                        # 尝试增量更新
                        incremental_result = self._try_incremental_update(
                            etf_code, threshold, cached_data, etf_data
                        )
                        if incremental_result is not None:
                            print(f"⚡ 使用增量更新: {etf_code}")
                            self.statistics["cache_hits"] += 1
                            self.statistics["incremental_updates"] += 1
                            self.statistics["successful_calculations"] += 1

                            if save_result:
                                self.csv_handler.save_etf_williams_data(
                                    etf_code, incremental_result, threshold
                                )

                            calculation_time = (
                                datetime.now() - calculation_start_time
                            ).total_seconds() * 1000
                            self._update_performance_metrics(calculation_time)

                            return {
                                "success": True,
                                "etf_code": etf_code,
                                "data_source": "incremental_update",
                                "record_count": len(incremental_result),
                                "data": incremental_result,
                                "calculation_time_ms": calculation_time,
                            }
                    else:
                        # 使用完整缓存
                        print(f"💾 使用缓存数据: {etf_code}")
                        self.statistics["cache_hits"] += 1
                        self.statistics["successful_calculations"] += 1

                        if save_result:
                            self.csv_handler.save_etf_williams_data(
                                etf_code, cached_data, threshold
                            )

                        calculation_time = (
                            datetime.now() - calculation_start_time
                        ).total_seconds() * 1000
                        self._update_performance_metrics(calculation_time)

                        return {
                            "success": True,
                            "etf_code": etf_code,
                            "data_source": "cache",
                            "record_count": len(cached_data),
                            "data": cached_data,
                            "calculation_time_ms": calculation_time,
                        }

            # 3. 缓存未命中或无效，进行全量计算
            self.statistics["cache_misses"] += 1
            self.statistics["full_calculations"] += 1
            print(f"🔄 缓存未命中，开始优化计算: {etf_code}")

            # 使用优化引擎计算威廉指标
            williams_result = self.williams_engine.calculate_williams_indicators_batch(
                etf_data
            )

            if williams_result.empty:
                error_msg = f"威廉指标计算失败: {etf_code}"
                print(f"❌ {error_msg}")
                self.statistics["failed_calculations"] += 1
                self.statistics["failed_etf_list"].append(etf_code)
                return {"success": False, "error": error_msg, "etf_code": etf_code}

            # 4. 格式化输出数据
            formatted_data = self.williams_engine.format_output_data(
                williams_result, etf_code
            )

            if formatted_data.empty:
                error_msg = f"数据格式化失败: {etf_code}"
                print(f"❌ {error_msg}")
                self.statistics["failed_calculations"] += 1
                self.statistics["failed_etf_list"].append(etf_code)
                return {"success": False, "error": error_msg, "etf_code": etf_code}

            # 5. 保存缓存和结果
            if save_result:
                try:
                    # 保存到缓存
                    self.cache_manager.save_etf_cache(
                        etf_code, formatted_data, threshold
                    )

                    # 保存到最终输出目录
                    self.csv_handler.save_etf_williams_data(
                        etf_code, formatted_data, threshold
                    )

                except Exception as save_error:
                    print(f"⚠️ 保存结果时发生错误: {etf_code} - {str(save_error)}")

            # 6. 计算性能指标
            calculation_time = (
                datetime.now() - calculation_start_time
            ).total_seconds() * 1000
            self._update_performance_metrics(calculation_time)

            # 7. 返回成功结果
            self.statistics["successful_calculations"] += 1
            print(
                f"✅ 威廉指标优化计算完成: {etf_code} (耗时: {calculation_time:.2f}ms)"
            )

            return {
                "success": True,
                "etf_code": etf_code,
                "data_source": "optimized_calculation",
                "record_count": len(formatted_data),
                "data": formatted_data,
                "calculation_time_ms": calculation_time,
                "statistics": self.williams_engine.calculate_williams_statistics(
                    williams_result
                )
                if hasattr(self.williams_engine, "calculate_williams_statistics")
                else {},
            }

        except Exception as e:
            error_msg = f"优化计算过程发生异常: {etf_code} - {str(e)}"
            print(f"❌ {error_msg}")
            print(f"🔍 异常详情: {traceback.format_exc()}")

            self.statistics["failed_calculations"] += 1
            self.statistics["failed_etf_list"].append(etf_code)

            return {
                "success": False,
                "error": error_msg,
                "etf_code": etf_code,
                "exception_type": type(e).__name__,
            }

    def _try_incremental_update(self, etf_code, threshold, cached_data, new_etf_data):
        """
        尝试增量更新

        Args:
            etf_code: ETF代码
            threshold: 门槛值
            cached_data: 缓存数据
            new_etf_data: 新ETF数据

        Returns:
            DataFrame: 增量更新结果，失败返回None
        """
        try:
            # 检查是否有新数据
            if "日期" not in new_etf_data.columns or "date" not in cached_data.columns:
                return None

            # 获取缓存数据的最新日期
            cached_latest_date = cached_data["date"].max()

            # 转换新数据的日期格式进行比较
            new_etf_data_copy = new_etf_data.copy()
            new_etf_data_copy["date"] = new_etf_data_copy["日期"].dt.strftime(
                "%Y-%m-%d"
            )

            # 筛选出比缓存更新的数据
            new_data_mask = new_etf_data_copy["date"] > cached_latest_date
            truly_new_data = new_etf_data[new_data_mask]

            if truly_new_data.empty:
                # 没有新数据，返回缓存数据
                print(f"📊 无新数据，使用缓存: {etf_code}")
                return cached_data

            # 使用优化引擎的增量更新功能
            if hasattr(self.williams_engine, "calculate_incremental_update"):
                # 准备现有数据格式
                existing_data_for_calc = (
                    new_etf_data[~new_data_mask]
                    if (~new_data_mask).any()
                    else new_etf_data.head(0)
                )

                incremental_result = self.williams_engine.calculate_incremental_update(
                    existing_data_for_calc, truly_new_data
                )

                if not incremental_result.empty:
                    # 格式化增量结果
                    formatted_incremental = self.williams_engine.format_output_data(
                        incremental_result, etf_code
                    )

                    # 合并缓存数据和增量数据
                    combined_result = self.csv_handler.merge_incremental_data(
                        etf_code, threshold, formatted_incremental
                    )

                    return combined_result

            return None

        except Exception as e:
            print(f"⚠️ 增量更新尝试失败: {etf_code} - {str(e)}")
            return None

    def _update_performance_metrics(self, calculation_time_ms):
        """更新性能指标"""
        self.statistics["optimization_metrics"]["total_calculation_time_ms"] += (
            calculation_time_ms
        )

        if self.statistics["successful_calculations"] > 0:
            self.statistics["optimization_metrics"]["average_calculation_time_ms"] = (
                self.statistics["optimization_metrics"]["total_calculation_time_ms"]
                / self.statistics["successful_calculations"]
            )

    def _calculate_hit_rate(self):
        """计算缓存命中率"""
        total_cache_operations = (
            self.statistics["cache_hits"] + self.statistics["cache_misses"]
        )
        if total_cache_operations > 0:
            return (self.statistics["cache_hits"] / total_cache_operations) * 100
        return 0.0

    def _calculate_incremental_update_rate(self):
        """计算增量更新率"""
        if self.statistics["processed_etfs"] > 0:
            return (
                self.statistics["incremental_updates"]
                / self.statistics["processed_etfs"]
            ) * 100
        return 0.0

    def get_optimization_summary(self):
        """获取优化效果摘要"""
        try:
            summary = {
                "optimization_status": "enabled"
                if self.use_optimized_components
                else "disabled",
                "performance_metrics": self.statistics["optimization_metrics"],
                "cache_performance": {
                    "hit_rate": self._calculate_hit_rate(),
                    "incremental_update_rate": self._calculate_incremental_update_rate(),
                },
                "calculation_breakdown": {
                    "full_calculations": self.statistics["full_calculations"],
                    "incremental_updates": self.statistics["incremental_updates"],
                    "cache_hits": self.statistics["cache_hits"],
                },
            }

            return summary

        except Exception as e:
            print(f"⚠️ 获取优化摘要失败: {str(e)}")
            return {}

    def print_optimization_summary(self):
        """打印优化效果摘要"""
        summary = self.get_optimization_summary()

        if summary:
            print(f"\n{'=' * 60}")
            print("🚀 威廉指标系统优化效果摘要")
            print(f"{'=' * 60}")
            print(
                f"🔧 优化状态: {'已启用' if summary['optimization_status'] == 'enabled' else '未启用'}"
            )
            print(
                f"⏱️ 平均计算时间: {summary['performance_metrics']['average_calculation_time_ms']:.2f}ms"
            )
            print(f"💾 缓存命中率: {summary['cache_performance']['hit_rate']:.2f}%")
            print(
                f"⚡ 增量更新率: {summary['cache_performance']['incremental_update_rate']:.2f}%"
            )
            print("📊 计算分布:")
            print(
                f"   • 全量计算: {summary['calculation_breakdown']['full_calculations']}次"
            )
            print(
                f"   • 增量更新: {summary['calculation_breakdown']['incremental_updates']}次"
            )
            print(f"   • 缓存命中: {summary['calculation_breakdown']['cache_hits']}次")
            print(f"{'=' * 60}")

    # 向后兼容接口
    def calculate_single_etf(self, etf_code, threshold, save_result=True):
        """向后兼容接口"""
        return self.calculate_single_etf_optimized(etf_code, threshold, save_result)

    def calculate_batch_etfs(self, etf_codes, threshold):
        """批量计算ETF威廉指标（继承原有逻辑，使用优化计算）"""
        try:
            print(f"🚀 开始优化批量计算威廉指标: {threshold}")
            print(f"📊 待处理ETF数量: {len(etf_codes)}")

            # 重置统计信息
            self._initialize_statistics()
            self.statistics["start_time"] = datetime.now()

            batch_results = {
                "threshold": threshold,
                "total_count": len(etf_codes),
                "success_count": 0,
                "fail_count": 0,
                "results": {},
                "failed_etfs": [],
                "start_time": self.statistics["start_time"],
                "end_time": None,
                "duration_seconds": 0,
                "optimization_used": self.use_optimized_components,
            }

            # 逐个处理ETF
            for i, etf_code in enumerate(etf_codes, 1):
                try:
                    print(f"📈 优化处理进度: {i}/{len(etf_codes)} - {etf_code}")

                    result = self.calculate_single_etf_optimized(
                        etf_code, threshold, save_result=True
                    )
                    batch_results["results"][etf_code] = result

                    if result["success"]:
                        batch_results["success_count"] += 1
                    else:
                        batch_results["fail_count"] += 1
                        batch_results["failed_etfs"].append(etf_code)

                    # 每处理指定数量ETF显示一次进度和优化效果
                    if i % PROGRESS_DISPLAY_INTERVAL == 0:
                        success_rate = (batch_results["success_count"] / i) * 100
                        avg_time = self.statistics["optimization_metrics"][
                            "average_calculation_time_ms"
                        ]
                        print(
                            f"📊 中间统计: 成功率 {success_rate:.1f}%, 平均耗时 {avg_time:.2f}ms"
                        )

                except Exception as e:
                    print(f"❌ 处理ETF时发生异常: {etf_code} - {str(e)}")
                    batch_results["fail_count"] += 1
                    batch_results["failed_etfs"].append(etf_code)
                    batch_results["results"][etf_code] = {
                        "success": False,
                        "error": f"处理异常: {str(e)}",
                        "etf_code": etf_code,
                    }

            # 完成统计
            self.statistics["end_time"] = datetime.now()
            batch_results["end_time"] = self.statistics["end_time"]
            batch_results["duration_seconds"] = (
                batch_results["end_time"] - batch_results["start_time"]
            ).total_seconds()

            # 更新缓存统计
            self.cache_manager.update_global_cache_stats(threshold)

            # 打印批量处理结果和优化效果
            self._print_batch_results_optimized(batch_results)

            return batch_results

        except Exception as e:
            print(f"❌ 优化批量计算过程发生异常: {str(e)}")
            raise

    def _print_batch_results_optimized(self, batch_results):
        """打印优化的批量处理结果"""
        print(f"\n{'=' * 60}")
        print(f"📊 威廉指标优化批量计算结果 - {batch_results['threshold']}")
        print(f"{'=' * 60}")

        success_rate = (
            (batch_results["success_count"] / batch_results["total_count"] * 100)
            if batch_results["total_count"] > 0
            else 0
        )

        print("📈 总体统计:")
        print(f"   • 总ETF数量: {batch_results['total_count']}")
        print(f"   • 成功数量: {batch_results['success_count']}")
        print(f"   • 失败数量: {batch_results['fail_count']}")
        print(f"   • 成功率: {success_rate:.2f}%")
        print(f"   • 处理时长: {batch_results['duration_seconds']:.1f}秒")
        print(
            f"   • 优化状态: {'✅ 已启用' if batch_results['optimization_used'] else '❌ 未启用'}"
        )

        # 优化效果统计
        avg_time = self.statistics["optimization_metrics"][
            "average_calculation_time_ms"
        ]
        print("🚀 优化效果:")
        print(f"   • 平均计算时间: {avg_time:.2f}ms")
        print(f"   • 全量计算: {self.statistics['full_calculations']}次")
        print(f"   • 增量更新: {self.statistics['incremental_updates']}次")
        print(f"   • 缓存命中: {self.statistics['cache_hits']}次")

        # 缓存统计
        cache_stats = self.cache_manager.get_cache_summary()
        if cache_stats:
            print("💾 缓存统计:")
            print(
                f"   • 缓存命中率: {cache_stats['performance']['hit_rate_percent']:.2f}%"
            )

        # 失败ETF列表
        if batch_results["failed_etfs"]:
            print("❌ 失败ETF列表:")
            failed_display = batch_results["failed_etfs"][:MAX_FAILED_ETF_DISPLAY]
            print(f"   • {', '.join(failed_display)}")
            if len(batch_results["failed_etfs"]) > MAX_FAILED_ETF_DISPLAY:
                print(
                    f"   • ... 以及其他{len(batch_results['failed_etfs']) - MAX_FAILED_ETF_DISPLAY}个"
                )

        print(f"{'=' * 60}")


if __name__ == "__main__":
    # 优化主控制器测试
    try:
        print("🧪 威廉指标优化主控制器测试")

        # 初始化优化控制器
        controller = WilliamsMainControllerOptimized(use_optimized_components=True)

        # 打印优化摘要
        controller.print_optimization_summary()

        print("✅ 优化主控制器测试完成")

    except Exception as e:
        print(f"❌ 优化主控制器测试失败: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")
