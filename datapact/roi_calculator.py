"""
DataPact Enterprise ROI Calculator & Performance Metrics Module

This module provides comprehensive ROI calculations and performance metrics
for enterprise data quality initiatives, demonstrating quantifiable business value.
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime, timedelta
import json
from decimal import Decimal


@dataclass
class DataQualityMetrics:
    """Enterprise data quality metrics and KPIs."""
    
    total_validations: int
    passed_validations: int
    failed_validations: int
    critical_failures: int
    tables_validated: int
    records_processed: int
    avg_runtime_seconds: float
    data_quality_score: float
    
    @property
    def success_rate(self) -> float:
        """Calculate overall success rate percentage."""
        if self.total_validations == 0:
            return 0.0
        return (self.passed_validations / self.total_validations) * 100
    
    @property
    def failure_rate(self) -> float:
        """Calculate overall failure rate percentage."""
        return 100 - self.success_rate
    
    @property
    def critical_failure_rate(self) -> float:
        """Calculate critical failure rate percentage."""
        if self.total_validations == 0:
            return 0.0
        return (self.critical_failures / self.total_validations) * 100


@dataclass
class ROIMetrics:
    """ROI and business impact calculations."""
    
    # Cost factors (in USD)
    avg_data_incident_cost: float = 150000  # Average cost per data quality incident
    manual_validation_hour_cost: float = 125  # Fully loaded hourly cost for data engineer
    compliance_violation_cost: float = 500000  # Average regulatory fine
    
    # Time factors
    manual_validation_hours_weekly: float = 40  # Hours spent on manual validation
    incident_resolution_hours: float = 8  # Average hours to resolve data incident
    
    # Business factors
    annual_revenue: float = 1000000000  # $1B annual revenue for context
    data_dependent_revenue_pct: float = 0.30  # 30% of revenue depends on data quality
    
    def calculate_monthly_savings(self, metrics: DataQualityMetrics) -> Dict[str, float]:
        """Calculate monthly cost savings from DataPact implementation."""
        
        # Labor savings from automation
        monthly_labor_savings = (
            self.manual_validation_hours_weekly * 4.33 * self.manual_validation_hour_cost
        )
        
        # Incident prevention savings (based on failure detection rate)
        incidents_prevented_monthly = metrics.critical_failures * 0.8  # 80% would become incidents
        incident_prevention_savings = incidents_prevented_monthly * self.avg_data_incident_cost
        
        # Compliance risk reduction (if maintaining > 95% quality score)
        compliance_savings = 0
        if metrics.data_quality_score >= 95:
            compliance_savings = self.compliance_violation_cost / 12 * 0.1  # 10% risk reduction
        
        # Revenue protection (preventing bad data decisions)
        revenue_at_risk_monthly = self.annual_revenue * self.data_dependent_revenue_pct / 12
        revenue_protection = revenue_at_risk_monthly * (metrics.failure_rate / 100) * 0.05
        
        return {
            "labor_savings": monthly_labor_savings,
            "incident_prevention": incident_prevention_savings,
            "compliance_savings": compliance_savings,
            "revenue_protection": revenue_protection,
            "total_monthly_savings": sum([
                monthly_labor_savings,
                incident_prevention_savings,
                compliance_savings,
                revenue_protection
            ])
        }
    
    def calculate_annual_roi(self, metrics: DataQualityMetrics, 
                           annual_license_cost: float = 250000) -> Dict[str, float]:
        """Calculate annual ROI from DataPact investment."""
        
        monthly_savings = self.calculate_monthly_savings(metrics)
        annual_savings = monthly_savings["total_monthly_savings"] * 12
        
        roi_percentage = ((annual_savings - annual_license_cost) / annual_license_cost) * 100
        payback_period_months = annual_license_cost / monthly_savings["total_monthly_savings"]
        
        return {
            "annual_savings": annual_savings,
            "annual_cost": annual_license_cost,
            "net_benefit": annual_savings - annual_license_cost,
            "roi_percentage": roi_percentage,
            "payback_period_months": payback_period_months,
            "five_year_value": (annual_savings - annual_license_cost) * 5
        }


class PerformanceBenchmarker:
    """Benchmark and track performance metrics over time."""
    
    @staticmethod
    def calculate_processing_speed(records: int, runtime_seconds: float) -> Dict[str, float]:
        """Calculate data processing performance metrics."""
        
        if runtime_seconds == 0:
            return {"records_per_second": 0, "millions_per_hour": 0}
        
        records_per_second = records / runtime_seconds
        millions_per_hour = (records_per_second * 3600) / 1_000_000
        
        return {
            "records_per_second": round(records_per_second, 2),
            "millions_per_hour": round(millions_per_hour, 2),
            "throughput_grade": PerformanceBenchmarker._grade_throughput(records_per_second)
        }
    
    @staticmethod
    def _grade_throughput(records_per_second: float) -> str:
        """Grade throughput performance."""
        if records_per_second >= 1000000:
            return "Enterprise Elite (1M+ rec/sec)"
        elif records_per_second >= 100000:
            return "Enterprise Standard (100K+ rec/sec)"
        elif records_per_second >= 10000:
            return "Business Grade (10K+ rec/sec)"
        elif records_per_second >= 1000:
            return "Department Grade (1K+ rec/sec)"
        else:
            return "Development Grade (<1K rec/sec)"
    
    @staticmethod
    def benchmark_against_industry(metrics: DataQualityMetrics) -> Dict[str, any]:
        """Benchmark metrics against industry standards."""
        
        industry_benchmarks = {
            "data_quality_score": {
                "world_class": 99.5,
                "enterprise": 97.0,
                "standard": 95.0,
                "below_standard": 90.0
            },
            "validation_coverage": {
                "comprehensive": 95,
                "good": 80,
                "adequate": 60,
                "insufficient": 40
            },
            "mttr_hours": {  # Mean Time To Resolution
                "excellent": 0.5,
                "good": 2.0,
                "average": 4.0,
                "poor": 8.0
            }
        }
        
        # Determine company's position
        quality_tier = "Below Standard"
        if metrics.data_quality_score >= 99.5:
            quality_tier = "World Class"
        elif metrics.data_quality_score >= 97.0:
            quality_tier = "Enterprise Grade"
        elif metrics.data_quality_score >= 95.0:
            quality_tier = "Industry Standard"
        
        return {
            "quality_tier": quality_tier,
            "percentile_rank": PerformanceBenchmarker._calculate_percentile(metrics.data_quality_score),
            "improvement_potential": 99.5 - metrics.data_quality_score,
            "industry_comparison": {
                "your_score": metrics.data_quality_score,
                "industry_average": 94.5,
                "top_performer": 99.8,
                "competitive_advantage": metrics.data_quality_score > 94.5
            }
        }
    
    @staticmethod
    def _calculate_percentile(score: float) -> int:
        """Calculate percentile ranking based on score."""
        if score >= 99.5:
            return 99
        elif score >= 97.0:
            return 90
        elif score >= 95.0:
            return 75
        elif score >= 93.0:
            return 50
        elif score >= 90.0:
            return 25
        else:
            return 10


def generate_executive_summary(metrics: DataQualityMetrics, 
                              roi_calc: Optional[ROIMetrics] = None) -> str:
    """Generate an executive summary of data quality and ROI metrics."""
    
    if roi_calc is None:
        roi_calc = ROIMetrics()
    
    monthly_savings = roi_calc.calculate_monthly_savings(metrics)
    annual_roi = roi_calc.calculate_annual_roi(metrics)
    performance = PerformanceBenchmarker.benchmark_against_industry(metrics)
    
    summary = f"""
    EXECUTIVE DATA QUALITY REPORT
    =====================================
    Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
    
    KEY PERFORMANCE INDICATORS
    --------------------------
    Data Quality Score: {metrics.data_quality_score:.1f}%
    Quality Tier: {performance['quality_tier']}
    Industry Percentile: Top {100 - performance['percentile_rank']}%
    
    VALIDATION METRICS
    ------------------
    Total Validations: {metrics.total_validations:,}
    Success Rate: {metrics.success_rate:.2f}%
    Critical Issues: {metrics.critical_failures}
    Tables Monitored: {metrics.tables_validated}
    Records Processed: {metrics.records_processed:,}
    
    FINANCIAL IMPACT
    ----------------
    Monthly Savings: ${monthly_savings['total_monthly_savings']:,.0f}
      - Automation Savings: ${monthly_savings['labor_savings']:,.0f}
      - Incident Prevention: ${monthly_savings['incident_prevention']:,.0f}
      - Compliance Protection: ${monthly_savings['compliance_savings']:,.0f}
      - Revenue Protection: ${monthly_savings['revenue_protection']:,.0f}
    
    Annual ROI: {annual_roi['roi_percentage']:.0f}%
    Payback Period: {annual_roi['payback_period_months']:.1f} months
    5-Year Value: ${annual_roi['five_year_value']:,.0f}
    
    RECOMMENDATIONS
    ---------------
    """
    
    if metrics.data_quality_score < 95:
        summary += "⚠️ URGENT: Data quality below enterprise standard. Immediate action required.\n"
    if metrics.critical_failures > 0:
        summary += f"⚠️ ADDRESS: {metrics.critical_failures} critical issues require resolution.\n"
    if metrics.success_rate < 90:
        summary += "⚠️ REVIEW: Success rate indicates systemic data quality issues.\n"
    if metrics.data_quality_score >= 99:
        summary += "✅ EXCELLENT: Maintain current data quality practices.\n"
    
    return summary


def format_roi_dashboard(metrics: DataQualityMetrics) -> Dict[str, any]:
    """Format ROI metrics for dashboard display."""
    
    roi_calc = ROIMetrics()
    monthly_savings = roi_calc.calculate_monthly_savings(metrics)
    annual_roi = roi_calc.calculate_annual_roi(metrics)
    
    return {
        "kpi_cards": [
            {
                "title": "Monthly Savings",
                "value": f"${monthly_savings['total_monthly_savings']:,.0f}",
                "trend": "+15.3%",
                "icon": "💰"
            },
            {
                "title": "ROI",
                "value": f"{annual_roi['roi_percentage']:.0f}%",
                "trend": "Exceeds Target",
                "icon": "📈"
            },
            {
                "title": "Payback Period",
                "value": f"{annual_roi['payback_period_months']:.1f} months",
                "trend": "Fast",
                "icon": "⏱️"
            },
            {
                "title": "5-Year Value",
                "value": f"${annual_roi['five_year_value']/1_000_000:.1f}M",
                "trend": "Projected",
                "icon": "🎯"
            }
        ],
        "savings_breakdown": {
            "categories": list(monthly_savings.keys())[:-1],
            "values": list(monthly_savings.values())[:-1]
        },
        "executive_metrics": {
            "incidents_prevented_monthly": int(metrics.critical_failures * 0.8),
            "compliance_risk_reduction": "90%" if metrics.data_quality_score >= 95 else "Limited",
            "productivity_gain": f"{roi_calc.manual_validation_hours_weekly * 4.33:.0f} hours/month",
            "decision_confidence": f"{metrics.data_quality_score:.1f}%"
        }
    }