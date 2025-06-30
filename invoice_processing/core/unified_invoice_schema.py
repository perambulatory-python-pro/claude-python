"""
Unified Invoice Details Schema for Internal Analysis
Designed to accommodate both BCI and AUS formats while maintaining data integrity
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any
from datetime import datetime, date
from enum import Enum

class PayType(Enum):
    """Standardized pay types across both systems"""
    REGULAR = "Regular"
    OVERTIME = "Overtime"
    HOLIDAY = "Holiday"
    SICK = "Sick"
    VACATION = "Vacation"
    TRAINING = "Training"
    BEREAVEMENT = "Bereavement"
    MEAL_BREAK_PREMIUM = "Meal Break Premium"
    REST_BREAK_PREMIUM = "Rest Break Premium"
    OTHER = "Other"

@dataclass
class UnifiedInvoiceDetail:
    """
    Unified schema that captures all necessary fields from both BCI and AUS formats
    while providing a consistent structure for internal analysis
    """
    
    # === CORE IDENTIFIERS === (Required fields first)
    invoice_number: str  # Changed from int to str to support revisions like "40011909R"
    invoice_line_id: str  # Composite key: invoice_number + employee_number + work_date + pay_type
    source_system: str  # 'BCI' or 'AUS'
    work_date: date
    week_ending_date: date
    pay_type: PayType
    hours_quantity: float
    bill_rate: float
    bill_amount: float
    created_timestamp: datetime
    source_file: str
    source_row_number: int
    
    # === ORGANIZATIONAL HIERARCHY === (Optional fields)
    emid: Optional[str] = None
    business_unit: Optional[str] = None
    mc_service_area: Optional[str] = None
    building_code: Optional[str] = None
    location_name: Optional[str] = None
    location_number: Optional[str] = None  # BCI Location_Number
    job_code: Optional[str] = None  # From reference table
    job_number: Optional[str] = None  # AUS Job Number
    
    # === CUSTOMER INFORMATION ===
    customer_name: Optional[str] = None
    customer_number: Optional[str] = None
    customer_po: Optional[str] = None
    
    # === POSITION/POST INFORMATION ===
    position_description: Optional[str] = None
    position_number: Optional[str] = None
    post_position_code: Optional[str] = None  # Future SCR matching
    
    # === EMPLOYEE INFORMATION ===
    employee_number: Optional[int] = None
    employee_first_name: Optional[str] = None
    employee_last_name: Optional[str] = None
    employee_middle_initial: Optional[str] = None
    employee_full_name: Optional[str] = None  # For AUS format
    
    # === TIME AND ATTENDANCE ===
    shift_start_time: Optional[str] = None
    shift_end_time: Optional[str] = None
    lunch_minutes: Optional[float] = None
    
    # === PAY INFORMATION ===
    pay_description: Optional[str] = None  # Original description from source
    pay_rate: Optional[float] = None
    
    # === BILLING CODES ===
    billing_code: Optional[str] = None  # BCI billing code
    bill_category_number: Optional[int] = None  # AUS bill cat number
    
    # === CALCULATED FIELDS ===
    @property
    def employee_name_formatted(self) -> str:
        """Return formatted employee name regardless of source format"""
        if self.employee_last_name and self.employee_first_name:
            name = f"{self.employee_last_name}, {self.employee_first_name}"
            if self.employee_middle_initial:
                name += f" {self.employee_middle_initial}"
            return name
        elif self.employee_full_name:
            return self.employee_full_name
        return "Unknown Employee"
    
    @property
    def location_key(self) -> str:
        """Generate a unique location key for matching"""
        if self.building_code and self.post_position_code:
            return f"{self.building_code}_{self.post_position_code}"
        elif self.location_number:
            return self.location_number
        elif self.job_number:
            return self.job_number
        return "UNKNOWN"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for DataFrame creation"""
        return {
            'invoice_number': self.invoice_number,
            'invoice_line_id': self.invoice_line_id,
            'source_system': self.source_system,
            'emid': self.emid,
            'business_unit': self.business_unit,
            'mc_service_area': self.mc_service_area,
            'building_code': self.building_code,
            'location_name': self.location_name,
            'location_number': self.location_number,
            'job_code': self.job_code,
            'job_number': self.job_number,
            'customer_name': self.customer_name,
            'customer_number': self.customer_number,
            'customer_po': self.customer_po,
            'position_description': self.position_description,
            'position_number': self.position_number,
            'post_position_code': self.post_position_code,
            'employee_number': self.employee_number,
            'employee_first_name': self.employee_first_name,
            'employee_last_name': self.employee_last_name,
            'employee_middle_initial': self.employee_middle_initial,
            'employee_full_name': self.employee_full_name,
            'employee_name_formatted': self.employee_name_formatted,
            'work_date': self.work_date,
            'week_ending_date': self.week_ending_date,
            'shift_start_time': self.shift_start_time,
            'shift_end_time': self.shift_end_time,
            'lunch_minutes': self.lunch_minutes,
            'pay_type': self.pay_type.value,
            'pay_description': self.pay_description,
            'hours_quantity': self.hours_quantity,
            'pay_rate': self.pay_rate,
            'bill_rate': self.bill_rate,
            'bill_amount': self.bill_amount,
            'billing_code': self.billing_code,
            'bill_category_number': self.bill_category_number,
            'location_key': self.location_key,
            'created_timestamp': self.created_timestamp,
            'source_file': self.source_file,
            'source_row_number': self.source_row_number
        }

# Mapping dictionaries for standardization
PAY_TYPE_MAPPINGS = {
    # BCI mappings (based on which hours field has data)
    'regular': PayType.REGULAR,
    'ot': PayType.OVERTIME,
    'holiday': PayType.HOLIDAY,
    
    # AUS mappings (from Pay Hours Description)
    'ca-regular': PayType.REGULAR,
    'ca-sick': PayType.SICK,
    'ca-vacation': PayType.VACATION,
    'ca-training': PayType.TRAINING,
    'hourly-sick': PayType.SICK,
    'hourly-bereavement': PayType.BEREAVEMENT,
    'hourly-vacation': PayType.VACATION,
    'interrupted or missed rest break': PayType.REST_BREAK_PREMIUM,
    'interrupted or missed meal period': PayType.MEAL_BREAK_PREMIUM,
}

def standardize_pay_type(description: str) -> PayType:
    """Convert various pay type descriptions to standardized enum"""
    if not description:
        return PayType.REGULAR
    
    desc_lower = description.lower().strip()
    
    for key, pay_type in PAY_TYPE_MAPPINGS.items():
        if key in desc_lower:
            return pay_type
    
    return PayType.OTHER
