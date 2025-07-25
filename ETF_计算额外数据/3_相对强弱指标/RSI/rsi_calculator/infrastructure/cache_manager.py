"""
RSI指标缓存管理器
基于威廉指标的缓存管理架构

功能特性：
1. 智能缓存验证和管理
2. 支持增量更新的缓存策略
3. 多门槛缓存组织结构
4. 自动缓存清理和优化
"""

import os
import json
import pandas as pd
from datetime import datetime, timedelta
import traceback


class RSICacheManager:
    """RSI指标缓存管理器"""

    def __init__(self, config):
        """
        初始化RSI缓存管理器
        
        Args:
            config: RSI配置对象
        """
        self.config = config
        self.cache_base_path = config.cache_base_path
        self.meta_path = config.get_meta_path()
        
        # 缓存统计
        self.cache_stats = {
            'cache_hits': 0,
            'cache_misses': 0,
            'cache_saves': 0,
            'cache_validation_errors': 0
        }
        
        # 确保缓存目录存在
        self._ensure_cache_directories()
        
        print("✅ RSI缓存管理器初始化完成")
        print(f"💾 缓存路径: {self.cache_base_path}")

    def _ensure_cache_directories(self):
        """确保缓存目录结构存在"""
        try:
            directories = [
                self.cache_base_path,
                self.meta_path,
                self.config.get_cache_path("3000万门槛"),
                self.config.get_cache_path("5000万门槛")
            ]
            
            for directory in directories:
                if not os.path.exists(directory):
                    os.makedirs(directory, exist_ok=True)
                    print(f"📁 创建缓存目录: {directory}")
                    
        except Exception as e:
            print(f"❌ 创建缓存目录失败: {str(e)}")

    def is_cache_valid_optimized(self, etf_code, threshold, source_file_path):
        """
        优化的缓存有效性检查
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            source_file_path: 源数据文件路径
            
        Returns:
            bool: 缓存是否有效
        """
        try:
            # 获取缓存文件路径
            cache_file_path = self._get_cache_file_path(etf_code, threshold)
            
            if not os.path.exists(cache_file_path):
                self.cache_stats['cache_misses'] += 1
                return False
            
            # 检查源文件是否存在
            if not os.path.exists(source_file_path):
                print(f"⚠️ 源文件不存在: {source_file_path}")
                self.cache_stats['cache_misses'] += 1
                return False
            
            # 比较文件修改时间
            cache_mtime = os.path.getmtime(cache_file_path)
            source_mtime = os.path.getmtime(source_file_path)
            
            # 如果源文件更新，缓存无效
            if source_mtime > cache_mtime:
                print(f"📝 源文件已更新，缓存无效: {etf_code}")
                self.cache_stats['cache_misses'] += 1
                return False
            
            # 检查缓存文件完整性
            try:
                cache_df = pd.read_csv(cache_file_path)
                if cache_df.empty:
                    print(f"⚠️ 缓存文件为空: {etf_code}")
                    self.cache_stats['cache_misses'] += 1
                    return False
                
                # 检查必需字段
                required_fields = ['code', 'date', 'rsi_6', 'rsi_12', 'rsi_24']
                missing_fields = [field for field in required_fields if field not in cache_df.columns]
                
                if missing_fields:
                    print(f"⚠️ 缓存文件缺少字段: {etf_code} - {missing_fields}")
                    self.cache_stats['cache_misses'] += 1
                    return False
                
            except Exception as e:
                print(f"⚠️ 缓存文件读取失败: {etf_code} - {str(e)}")
                self.cache_stats['cache_misses'] += 1
                return False
            
            self.cache_stats['cache_hits'] += 1
            return True
            
        except Exception as e:
            print(f"❌ 缓存验证失败: {etf_code} - {str(e)}")
            self.cache_stats['cache_validation_errors'] += 1
            return False

    def load_etf_cache(self, etf_code, threshold):
        """
        加载ETF缓存数据
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            
        Returns:
            DataFrame: 缓存的RSI数据
        """
        try:
            cache_file_path = self._get_cache_file_path(etf_code, threshold)
            
            if not os.path.exists(cache_file_path):
                print(f"📁 缓存文件不存在: {etf_code}")
                return pd.DataFrame()
            
            # 读取缓存文件
            cache_df = pd.read_csv(cache_file_path)
            
            if cache_df.empty:
                print(f"⚠️ 缓存文件为空: {etf_code}")
                return pd.DataFrame()
            
            print(f"💾 成功加载缓存: {etf_code} ({len(cache_df)}行数据)")
            return cache_df
            
        except Exception as e:
            print(f"❌ 加载缓存失败: {etf_code} - {str(e)}")
            return pd.DataFrame()

    def save_etf_cache(self, etf_code, rsi_data, threshold):
        """
        保存ETF缓存数据
        
        Args:
            etf_code: ETF代码
            rsi_data: RSI计算结果
            threshold: 门槛值
            
        Returns:
            bool: 保存是否成功
        """
        try:
            if rsi_data is None or rsi_data.empty:
                print(f"⚠️ RSI数据为空，跳过缓存保存: {etf_code}")
                return False
            
            # 获取缓存文件路径
            cache_file_path = self._get_cache_file_path(etf_code, threshold)
            
            # 确保缓存目录存在
            cache_dir = os.path.dirname(cache_file_path)
            if not os.path.exists(cache_dir):
                os.makedirs(cache_dir, exist_ok=True)
            
            # 保存缓存文件
            rsi_data.to_csv(cache_file_path, index=False, encoding='utf-8')
            
            # 更新缓存元数据
            self._update_cache_metadata(etf_code, threshold, len(rsi_data))
            
            self.cache_stats['cache_saves'] += 1
            print(f"💾 缓存保存成功: {etf_code} ({len(rsi_data)}行数据)")
            
            return True
            
        except Exception as e:
            print(f"❌ 缓存保存失败: {etf_code} - {str(e)}")
            print(f"🔍 异常详情: {traceback.format_exc()}")
            return False

    def _get_cache_file_path(self, etf_code, threshold):
        """获取缓存文件路径"""
        cache_dir = self.config.get_cache_path(threshold)
        return os.path.join(cache_dir, f"{etf_code}.csv")

    def _update_cache_metadata(self, etf_code, threshold, data_count):
        """
        更新缓存元数据
        
        Args:
            etf_code: ETF代码
            threshold: 门槛值
            data_count: 数据行数
        """
        try:
            meta_file = os.path.join(self.meta_path, f"{threshold}_meta.json")
            
            # 读取现有元数据
            if os.path.exists(meta_file):
                with open(meta_file, 'r', encoding='utf-8') as f:
                    meta_data = json.load(f)
            else:
                meta_data = {}
            
            # 更新ETF元数据
            meta_data[etf_code] = {
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'data_count': data_count,
                'threshold': threshold,
                'cache_file': f"{etf_code}.csv"
            }
            
            # 保存元数据
            with open(meta_file, 'w', encoding='utf-8') as f:
                json.dump(meta_data, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"⚠️ 更新缓存元数据失败: {etf_code} - {str(e)}")

    def cleanup_invalid_cache(self, days_old=30):
        """
        清理无效的缓存文件
        
        Args:
            days_old: 清理多少天前的缓存
            
        Returns:
            dict: 清理统计信息
        """
        try:
            cleanup_stats = {
                'total_checked': 0,
                'deleted_files': 0,
                'deleted_size_mb': 0,
                'errors': 0
            }
            
            cutoff_time = datetime.now() - timedelta(days=days_old)
            cutoff_timestamp = cutoff_time.timestamp()
            
            # 检查所有门槛的缓存目录
            for threshold in ["3000万门槛", "5000万门槛"]:
                cache_dir = self.config.get_cache_path(threshold)
                
                if not os.path.exists(cache_dir):
                    continue
                
                # 遍历缓存文件
                for filename in os.listdir(cache_dir):
                    if not filename.endswith('.csv'):
                        continue
                    
                    file_path = os.path.join(cache_dir, filename)
                    cleanup_stats['total_checked'] += 1
                    
                    try:
                        # 检查文件修改时间
                        file_mtime = os.path.getmtime(file_path)
                        
                        if file_mtime < cutoff_timestamp:
                            # 获取文件大小
                            file_size = os.path.getsize(file_path) / (1024 * 1024)  # MB
                            
                            # 删除文件
                            os.remove(file_path)
                            
                            cleanup_stats['deleted_files'] += 1
                            cleanup_stats['deleted_size_mb'] += file_size
                            
                            print(f"🗑️ 清理过期缓存: {filename}")
                            
                    except Exception as e:
                        print(f"⚠️ 清理缓存文件失败: {filename} - {str(e)}")
                        cleanup_stats['errors'] += 1
            
            # 清理元数据中已删除文件的记录
            self._cleanup_metadata()
            
            print(f"🧹 缓存清理完成:")
            print(f"   检查文件: {cleanup_stats['total_checked']}")
            print(f"   删除文件: {cleanup_stats['deleted_files']}")
            print(f"   释放空间: {cleanup_stats['deleted_size_mb']:.2f}MB")
            print(f"   错误数量: {cleanup_stats['errors']}")
            
            return cleanup_stats
            
        except Exception as e:
            print(f"❌ 缓存清理失败: {str(e)}")
            return {'error': str(e)}

    def _cleanup_metadata(self):
        """清理元数据中无效的记录"""
        try:
            for threshold in ["3000万门槛", "5000万门槛"]:
                meta_file = os.path.join(self.meta_path, f"{threshold}_meta.json")
                
                if not os.path.exists(meta_file):
                    continue
                
                # 读取元数据
                with open(meta_file, 'r', encoding='utf-8') as f:
                    meta_data = json.load(f)
                
                # 检查每个ETF的缓存文件是否存在
                cache_dir = self.config.get_cache_path(threshold)
                etfs_to_remove = []
                
                for etf_code, etf_meta in meta_data.items():
                    cache_file = os.path.join(cache_dir, etf_meta.get('cache_file', f"{etf_code}.csv"))
                    if not os.path.exists(cache_file):
                        etfs_to_remove.append(etf_code)
                
                # 移除无效记录
                for etf_code in etfs_to_remove:
                    del meta_data[etf_code]
                
                # 保存更新后的元数据
                if etfs_to_remove:
                    with open(meta_file, 'w', encoding='utf-8') as f:
                        json.dump(meta_data, f, ensure_ascii=False, indent=2)
                    print(f"🧹 清理元数据记录: {len(etfs_to_remove)}个无效记录")
                    
        except Exception as e:
            print(f"⚠️ 清理元数据失败: {str(e)}")

    def update_global_cache_stats(self, threshold):
        """
        更新全局缓存统计
        
        Args:
            threshold: 门槛值
        """
        try:
            global_meta_file = os.path.join(self.meta_path, "cache_global_meta.json")
            
            # 读取现有全局元数据
            if os.path.exists(global_meta_file):
                with open(global_meta_file, 'r', encoding='utf-8') as f:
                    global_meta = json.load(f)
            else:
                global_meta = {}
            
            # 更新统计信息
            global_meta[threshold] = {
                'last_updated': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'cache_stats': self.cache_stats.copy(),
                'total_cache_files': self._count_cache_files(threshold)
            }
            
            # 保存全局元数据
            with open(global_meta_file, 'w', encoding='utf-8') as f:
                json.dump(global_meta, f, ensure_ascii=False, indent=2)
                
        except Exception as e:
            print(f"⚠️ 更新全局缓存统计失败: {str(e)}")

    def _count_cache_files(self, threshold):
        """统计缓存文件数量"""
        try:
            cache_dir = self.config.get_cache_path(threshold)
            if not os.path.exists(cache_dir):
                return 0
            
            csv_files = [f for f in os.listdir(cache_dir) if f.endswith('.csv')]
            return len(csv_files)
            
        except Exception as e:
            print(f"⚠️ 统计缓存文件失败: {str(e)}")
            return 0

    def get_cache_summary(self):
        """
        获取缓存摘要信息
        
        Returns:
            dict: 缓存摘要
        """
        try:
            summary = {
                'cache_directories': {},
                'performance': {},
                'metadata': {}
            }
            
            # 统计各门槛的缓存文件
            for threshold in ["3000万门槛", "5000万门槛"]:
                cache_dir = self.config.get_cache_path(threshold)
                file_count = self._count_cache_files(threshold)
                
                summary['cache_directories'][threshold] = {
                    'file_count': file_count,
                    'directory': cache_dir,
                    'exists': os.path.exists(cache_dir)
                }
            
            # 性能统计
            total_operations = self.cache_stats['cache_hits'] + self.cache_stats['cache_misses']
            if total_operations > 0:
                hit_rate = self.cache_stats['cache_hits'] / total_operations * 100
            else:
                hit_rate = 0
            
            summary['performance'] = {
                'hit_rate_percent': round(hit_rate, 2),
                'cache_hits': self.cache_stats['cache_hits'],
                'cache_misses': self.cache_stats['cache_misses'],
                'cache_saves': self.cache_stats['cache_saves'],
                'validation_errors': self.cache_stats['cache_validation_errors']
            }
            
            # 元数据统计
            summary['metadata'] = {
                'meta_path': self.meta_path,
                'global_meta_exists': os.path.exists(os.path.join(self.meta_path, "cache_global_meta.json"))
            }
            
            return summary
            
        except Exception as e:
            print(f"❌ 获取缓存摘要失败: {str(e)}")
            return {}

    def print_cache_summary(self):
        """打印缓存摘要"""
        summary = self.get_cache_summary()
        
        if not summary:
            print("❌ 无法获取缓存摘要")
            return
        
        print(f"\n{'=' * 60}")
        print("💾 RSI缓存管理器摘要")
        print(f"{'=' * 60}")
        
        # 缓存目录统计
        print("📁 缓存目录:")
        for threshold, info in summary['cache_directories'].items():
            status = "✅" if info['exists'] else "❌"
            print(f"   {status} {threshold}: {info['file_count']}个文件")
        
        # 性能统计
        perf = summary['performance']
        print("📊 性能统计:")
        print(f"   缓存命中率: {perf['hit_rate_percent']:.2f}%")
        print(f"   缓存命中: {perf['cache_hits']}次")
        print(f"   缓存未命中: {perf['cache_misses']}次")
        print(f"   缓存保存: {perf['cache_saves']}次")
        print(f"   验证错误: {perf['validation_errors']}次")
        
        print(f"{'=' * 60}")


if __name__ == "__main__":
    # 缓存管理器测试
    try:
        from config import RSIConfig
        
        print("🧪 RSI缓存管理器测试")
        config = RSIConfig()
        cache_manager = RSICacheManager(config)
        
        # 打印缓存摘要
        cache_manager.print_cache_summary()
        
        print("✅ RSI缓存管理器测试完成")
        
    except Exception as e:
        print(f"❌ RSI缓存管理器测试失败: {str(e)}")
        print(f"🔍 异常详情: {traceback.format_exc()}")