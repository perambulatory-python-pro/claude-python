"""
Database Models - SQLAlchemy ORM Definitions
This file defines the structure of our database tables as Python classes.

Key Python Concepts:
- Classes: Python blueprints for creating objects
- Inheritance: Our models inherit from SQLAlchemy's Base class
- Decorators: @property creates computed fields
- Type hints: Helps with code completion and error detection
"""

from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, Boolean, Text, Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func
from datetime import datetime, date
from typing import Optional

# Create the base class that all our models will inherit from
Base = declarative_base()

class Invoice(Base):
    """
    Invoice model - represents the main invoice tracking table
    This maps to your 'invoices' table in NeonDB
    
    Python Concept: Each attribute here becomes a column in the database
    """
    __tablename__ = 'invoices'
    
    # Primary key - unique identifier for each invoice
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Required fields
    invoice_no = Column(String(50), nullable=False, unique=True)
    
    # Optional fields - can be None/NULL
    emid = Column(String(20), nullable=True)
    nuid = Column(String(20), nullable=True)
    service_reqd_by = Column(String(100), nullable=True)
    service_area = Column(String(200), nullable=True)
    post_name = Column(String(200), nullable=True)
    chartfield = Column(String(50), nullable=True)
    
    # Date fields
    invoice_from = Column(Date, nullable=True)
    invoice_to = Column(Date, nullable=True)
    invoice_date = Column(Date, nullable=True)
    edi_date = Column(Date, nullable=True)
    release_date = Column(Date, nullable=True)
    add_on_date = Column(Date, nullable=True)
    
    # Original date tracking (for your date preservation logic)
    original_edi_date = Column(Date, nullable=True)
    original_add_on_date = Column(Date, nullable=True)
    original_release_date = Column(Date, nullable=True)
    
    # Financial fields
    invoice_total = Column(Numeric(10, 2), nullable=True, default=0.00)
    
    # Status fields
    not_transmitted = Column(Boolean, nullable=True, default=False)
    
    # History tracking
    invoice_no_history = Column(String(50), nullable=True)
    notes = Column(Text, nullable=True)
    
    # Automatic timestamps
    created_at = Column(DateTime, nullable=False, default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    def __repr__(self):
        """
        Python Concept: __repr__ defines how the object appears when printed
        Useful for debugging and logging
        """
        return f"<Invoice(invoice_no='{self.invoice_no}', emid='{self.emid}', total={self.invoice_total})>"

class InvoiceDetail(Base):
    """
    Invoice Detail model - represents individual line items/employee records
    This handles your massive BCI/AUS data (50K+ rows per cycle)
    """
    __tablename__ = 'invoice_details'
    
    # Primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Required fields
    invoice_no = Column(String(50), nullable=False)
    source_system = Column(String(10), nullable=False)  # 'BCI' or 'AUS'
    
    # Employee information
    employee_id = Column(String(50), nullable=True)
    employee_name = Column(String(200), nullable=True)
    
    # Work details
    work_date = Column(Date, nullable=True)
    
    # Hours tracking
    hours_regular = Column(Numeric(8, 2), nullable=True, default=0)
    hours_overtime = Column(Numeric(8, 2), nullable=True, default=0)
    hours_holiday = Column(Numeric(8, 2), nullable=True, default=0)
    hours_total = Column(Numeric(8, 2), nullable=True, default=0)
    
    # Rate tracking
    rate_regular = Column(Numeric(8, 2), nullable=True, default=0)
    rate_overtime = Column(Numeric(8, 2), nullable=True, default=0)
    rate_holiday = Column(Numeric(8, 2), nullable=True, default=0)
    pay_rate = Column(Numeric(8, 2), nullable=True)
    
    # Amount tracking
    amount_regular = Column(Numeric(10, 2), nullable=True, default=0)
    amount_overtime = Column(Numeric(10, 2), nullable=True, default=0)
    amount_holiday = Column(Numeric(10, 2), nullable=True, default=0)
    amount_total = Column(Numeric(10, 2), nullable=True, default=0)
    
    # Time tracking
    shift_in = Column(Time, nullable=True)
    shift_out = Column(Time, nullable=True)
    in_time = Column(Time, nullable=True)
    out_time = Column(Time, nullable=True)
    lunch_hours = Column(Numeric(4, 2), nullable=True)
    
    # Location and position
    location_code = Column(String(50), nullable=True)
    location_name = Column(String(200), nullable=True)
    building_code = Column(String(50), nullable=True)
    emid = Column(String(20), nullable=True)
    position_code = Column(String(50), nullable=True)
    position_description = Column(String(200), nullable=True)
    
    # Business information
    bill_category = Column(String(50), nullable=True)
    job_number = Column(String(50), nullable=True)
    po = Column(String(50), nullable=True)
    customer_number = Column(String(50), nullable=True)
    customer_name = Column(String(200), nullable=True)
    business_unit = Column(String(50), nullable=True)
    
    # Automatic timestamps
    created_at = Column(DateTime, nullable=False, default=func.current_timestamp())
    updated_at = Column(DateTime, nullable=False, default=func.current_timestamp(), onupdate=func.current_timestamp())
    
    def __repr__(self):
        return f"<InvoiceDetail(invoice_no='{self.invoice_no}', employee_id='{self.employee_id}', hours={self.hours_total})>"

class BuildingDimension(Base):
    """
    Building reference data - from your Kaiser SCR master file
    """
    __tablename__ = 'building_dimension'
    
    # Primary key
    building_code = Column(String(20), primary_key=True)
    
    # Building information
    building_name = Column(String(100), nullable=True)
    emid = Column(String(20), nullable=True)
    mc_service_area = Column(String(100), nullable=True)
    region = Column(String(50), nullable=True)
    address = Column(Text, nullable=True)
    business_unit = Column(String(50), nullable=True)
    
    # Timestamp
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    def __repr__(self):
        return f"<BuildingDimension(building_code='{self.building_code}', name='{self.building_name}')>"

class EMIDReference(Base):
    """
    EMID reference data
    """
    __tablename__ = 'emid_reference'
    
    # Primary key
    emid = Column(String(20), primary_key=True)
    
    # Reference information
    description = Column(String(200), nullable=True)
    job_code = Column(String(20), nullable=True)
    region = Column(String(50), nullable=True)
    
    # Timestamp
    created_at = Column(DateTime, nullable=False, default=func.now())
    
    def __repr__(self):
        return f"<EMIDReference(emid='{self.emid}', description='{self.description}')>"

# Additional reference tables (for future use)
class PostAssignment(Base):
    __tablename__ = 'post_assignment'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_assignment_id = Column(String(50), nullable=False)
    post_assignment = Column(String(255), nullable=False)

class PostDescription(Base):
    __tablename__ = 'post_description'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_description_id = Column(String(50), nullable=False)
    post_description = Column(String(50), nullable=False)

class PostPosition(Base):
    __tablename__ = 'post_position'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    post_position_id = Column(String(50), nullable=False)
    post_position = Column(String(255), nullable=False)