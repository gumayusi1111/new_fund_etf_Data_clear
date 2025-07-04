#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MACD结果处理器 - 客观数据专版
=========================

🚫 已简化：只保留客观数据处理，移除主观判断
负责MACD计算结果的格式化、输出和管理
📊 功能: CSV生成、目录管理、结果验证、统计报告
🎯 输出: 标准化的MACD指标数据文件（纯客观数据）
🚫 已移除: 交易建议、信号分析、金叉死叉等主观判断

"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from .config import MACDConfig


class MACDResultProcessor:
    """MACD结果处理器 - 客观数据专版"""
    
    def __init__(self, config: MACDConfig):
        """
        初始化结果处理器
        
        Args:
            config: MACD配置对象
        """
        self.config = config
        self.base_output_dir = config.get_output_base_dir()
        
        # 创建输出目录结构
        self._ensure_output_directories()
        
        print("📁 MACD结果处理器初始化完成 (客观数据专版)")
        print(f"📂 输出目录: {self.base_output_dir}")
        print("🚫 已移除: 交易建议、信号分析等主观判断")
    
    def _ensure_output_directories(self):
        """确保输出目录结构存在"""
        try:
            # 创建主输出目录
            os.makedirs(self.base_output_dir, exist_ok=True)
            
            # 创建门槛类型目录，每个门槛下按参数类型分子目录
            thresholds = ["3000万门槛", "5000万门槛"]
            parameter_types = ["标准", "敏感", "平滑"]
            
            for threshold in thresholds:
                for param_type in parameter_types:
                    threshold_param_dir = os.path.join(self.base_output_dir, threshold, param_type)
                    os.makedirs(threshold_param_dir, exist_ok=True)
                    print(f"📁 确保目录存在: {threshold_param_dir}")
                    
        except Exception as e:
            print(f"❌ 创建输出目录失败: {e}")
    
    def format_macd_results(self, df: pd.DataFrame, etf_code: str) -> pd.DataFrame:
        """
        格式化MACD计算结果 - 客观数据专版
        
        Args:
            df: 包含MACD计算结果的DataFrame
            etf_code: ETF代码
            
        Returns:
            格式化后的DataFrame
        """
        try:
            if df.empty:
                print(f"⚠️ {etf_code}: 空的MACD结果")
                return pd.DataFrame()
            
            # 创建格式化后的DataFrame，按用户要求的字段顺序
            formatted_df = pd.DataFrame({
                'date': df['Date'].dt.strftime('%Y-%m-%d'),  # 日期格式化
                'code': etf_code,  # ETF代码
                'ema_fast': df['EMA_Fast'].round(6),  # 快线EMA
                'ema_slow': df['EMA_Slow'].round(6),  # 慢线EMA
                'dif': df['DIF'].round(6),  # ema_fast - ema_slow
                'dea': df['DEA'].round(6),  # DIF的信号线EMA
                'macd_bar': df['MACD'].round(6),  # (dif - dea) * 2
                'calc_time': pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')  # 脚本更新时间
            })
            
            # 按日期排序（最新日期在前）
            formatted_df = formatted_df.sort_values('date', ascending=False).reset_index(drop=True)
            
            print(f"✅ {etf_code} MACD结果格式化完成，{len(formatted_df)} 行数据")
            return formatted_df
            
        except Exception as e:
            print(f"❌ {etf_code} MACD结果格式化失败: {e}")
            return pd.DataFrame()
    

    
    def save_single_etf_result(self, result_df: pd.DataFrame, etf_code: str, 
                              threshold_type: str = "3000万门槛") -> bool:
        """
        保存单个ETF的MACD结果到对应参数类型的子目录
        
        Args:
            result_df: 格式化的结果DataFrame
            etf_code: ETF代码
            threshold_type: 门槛类型
            
        Returns:
            是否保存成功
        """
        try:
            # 根据配置参数类型确定子目录
            parameter_mapping = {
                'standard': '标准',
                'sensitive': '敏感', 
                'smooth': '平滑'
            }
            
            param_type = parameter_mapping.get(self.config.parameter_set, '标准')
            
            # 构建保存路径：门槛类型/参数类型/ETF文件
            output_subdir = os.path.join(self.base_output_dir, threshold_type, param_type)
            output_file = os.path.join(output_subdir, f"{etf_code}.csv")
            
            # 确保目录存在
            os.makedirs(output_subdir, exist_ok=True)
            
            # 保存文件
            result_df.to_csv(output_file, index=False, encoding='utf-8-sig')
            
            print(f"💾 {etf_code} 结果已保存: {output_file}")
            return True
            
        except Exception as e:
            print(f"❌ {etf_code} 保存失败: {e}")
            return False
    
    def batch_save_results(self, results_dict: Dict[str, pd.DataFrame], 
                          threshold_type: str = "3000万门槛") -> Dict[str, str]:
        """
        批量保存ETF结果
        
        Args:
            results_dict: ETF代码到结果DataFrame的映射
            threshold_type: 门槛类型
            
        Returns:
            保存状态字典
        """
        save_status = {}
        success_count = 0
        
        print(f"💾 开始批量保存 {len(results_dict)} 个ETF的MACD结果...")
        
        for i, (etf_code, df) in enumerate(results_dict.items(), 1):
            print(f"💾 [{i}/{len(results_dict)}] 保存 {etf_code}...", end=" ")
            
            if self.save_single_etf_result(df, etf_code, threshold_type):
                save_status[etf_code] = "成功"
                success_count += 1
                print("✅")
            else:
                save_status[etf_code] = "失败"
                print("❌")
        
        print(f"🎯 批量保存完成: {success_count}/{len(results_dict)} 个文件保存成功")
        return save_status
    
    def generate_summary_report(self, results_dict: Dict[str, pd.DataFrame], 
                               save_status: Dict[str, str],
                               threshold_type: str = "3000万门槛") -> Dict:
        """
        生成汇总报告 - 客观数据专版
        
        Args:
            results_dict: 结果数据字典
            save_status: 保存状态字典
            threshold_type: 门槛类型
            
        Returns:
            汇总报告字典（仅客观统计）
        """
        report = {
            'processing_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'threshold_type': threshold_type,
            'parameter_set': self.config.parameter_set,
            'macd_periods': self.config.get_macd_periods(),
            'total_etfs': len(results_dict),
            'successful_saves': sum(1 for status in save_status.values() if status == "成功"),
            'failed_saves': sum(1 for status in save_status.values() if status == "失败"),
            'average_data_length': 0,
            # 🚫 已移除主观判断统计
            # 'signal_distribution': {},
            # 'latest_signals': {}
        }
        
        # 计算客观统计信息
        if results_dict:
            data_lengths = [len(df) for df in results_dict.values()]
            report['average_data_length'] = int(np.mean(data_lengths))
            
            # 🚫 已移除信号分布统计和最新信号统计
            # all_signals = []
            # latest_signals = {}
            # 
            # for etf_code, df in results_dict.items():
            #     if len(df) > 0:
            #         signals = df['MACD信号描述'].tolist()
            #         all_signals.extend(signals)
            #         
            #         # 记录最新信号
            #         latest_signals[etf_code] = {
            #             'signal': df['MACD信号描述'].iloc[-1],
            #             'score': df['MACD信号评分'].iloc[-1],
            #             'date': df['日期'].iloc[-1]
            #         }
            # 
            # # 信号分布统计
            # from collections import Counter
            # signal_counts = Counter(all_signals)
            # report['signal_distribution'] = dict(signal_counts)
            # report['latest_signals'] = latest_signals
        
        return report
    
    def save_summary_report(self, report: Dict, threshold_type: str = "3000万门槛") -> bool:
        """
        保存汇总报告 - 客观数据专版
        
        Args:
            report: 汇总报告字典
            threshold_type: 门槛类型
            
        Returns:
            是否保存成功
        """
        try:
            output_dir = os.path.join(self.base_output_dir, threshold_type)
            report_file = os.path.join(output_dir, "MACD_汇总报告.txt")
            
            with open(report_file, 'w', encoding='utf-8') as f:
                f.write("=" * 60 + "\n")
                f.write("MACD指标计算汇总报告 - 客观数据专版\n")
                f.write("=" * 60 + "\n\n")
                
                f.write(f"计算时间: {report['processing_time']}\n")
                f.write(f"门槛类型: {report['threshold_type']}\n")
                f.write(f"参数配置: {report['parameter_set']}\n")
                f.write(f"MACD参数: EMA({report['macd_periods'][0]}, {report['macd_periods'][1]}, {report['macd_periods'][2]})\n\n")
                
                f.write(f"处理统计:\n")
                f.write(f"- 总ETF数量: {report['total_etfs']}\n")
                f.write(f"- 成功保存: {report['successful_saves']}\n")
                f.write(f"- 失败保存: {report['failed_saves']}\n")
                f.write(f"- 平均数据长度: {report['average_data_length']} 天\n\n")
                
                # 🚫 已移除主观判断统计
                # f.write("信号分布统计:\n")
                # for signal, count in report['signal_distribution'].items():
                #     f.write(f"- {signal}: {count} 次\n")
                # 
                # f.write("\n最新信号前10个ETF:\n")
                # sorted_signals = sorted(report['latest_signals'].items(), 
                #                       key=lambda x: x[1]['score'], reverse=True)
                # for i, (etf_code, signal_info) in enumerate(sorted_signals[:10], 1):
                #     f.write(f"{i:2d}. {etf_code}: {signal_info['signal']} "
                #            f"(评分: {signal_info['score']:.3f}, 日期: {signal_info['date']})\n")
                
                f.write("说明:\n")
                f.write("🚫 已移除主观判断内容：信号分析、交易建议、金叉死叉等\n")
                f.write("📊 只保留客观数据：DIF、DEA、MACD等技术指标数值\n")
            
            print(f"📊 汇总报告已保存: {report_file}")
            return True
            
        except Exception as e:
            print(f"❌ 汇总报告保存失败: {e}")
            return False
    
    def validate_output_files(self, threshold_type: str = "3000万门槛") -> Dict[str, bool]:
        """
        验证输出文件 - 客观数据专版
        
        Args:
            threshold_type: 门槛类型
            
        Returns:
            文件验证结果字典
        """
        output_dir = os.path.join(self.base_output_dir, threshold_type)
        validation_results = {}
        
        if not os.path.exists(output_dir):
            print(f"❌ 输出目录不存在: {output_dir}")
            return validation_results
        
        csv_files = [f for f in os.listdir(output_dir) if f.endswith('.csv')]
        
        for csv_file in csv_files:
            file_path = os.path.join(output_dir, csv_file)
            etf_code = csv_file.replace('.csv', '')
            
            try:
                # 检查文件是否可读
                df = pd.read_csv(file_path)
                
                # 检查必要列（客观数据）
                required_columns = ['date', 'code', 'dif', 'dea', 'macd_bar']
                has_required_columns = all(col in df.columns for col in required_columns)
                
                # 检查数据质量
                has_valid_data = len(df) > 0 and not df['dif'].isna().all()
                
                validation_results[etf_code] = has_required_columns and has_valid_data
                
            except Exception as e:
                print(f"❌ 验证文件失败 {csv_file}: {e}")
                validation_results[etf_code] = False
        
        valid_files = sum(validation_results.values())
        total_files = len(validation_results)
        print(f"📊 文件验证完成: {valid_files}/{total_files} 个文件验证通过")
        
        return validation_results 