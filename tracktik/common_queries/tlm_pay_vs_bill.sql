SELECT position.account.region.customId as `Division No`, position.account.region.name as `Division Name`,position.account.parentAccount.customId as `Customer No`,
position.account.parentAccount.name as `Customer Name` , 
position.account.customId as `Location No`, position.account.name as `Location Name`, 
position.customId as `Post No`, position.name as `Post Name`,
startDateTime as `Tour In`, endDateTime as `Tour Out`,
employee.customId as `Employee No`,
employee.name as `Employee Name`, 
billableHours as `Bill Reg Hrs`,
billRate as `Bill Reg Rate`,
billableHours*billRate as `Total Bill Reg Amount`,
CASE
	WHEN overtimeHours is not null and billable = 1 THEN overtimeHours
    ELSE "0"
    END as `Total Bill OT1 Hrs`,
CASE
	WHEN billable = 1 and overtimeHours is not null THEN billRate * 1.5
    ELSE "0"
    END as `Total Bill OT1 Rate`,
    (CASE
	WHEN overtimeHours is not null and billable = 1 THEN overtimeHours
    ELSE "0"
    END)
    *
ROUND((CASE
	WHEN billable = 1 and overtimeHours is not null THEN billRate * 1.5
    ELSE "0"
    END),2) as `Total Bill OT1 Amt`,
CASE
	WHEN billable = 1 and position.billingSetting.holidayGroup.id and holidayHours is not null THEN holidayHours
    ELSE "0"
    END as `Total Holiday Hrs`,
CASE
	WHEN billable = 1 and position.billingSetting.holidayGroup.id and holidayHours is not null THEN billRate * 1.5
    ELSE "0"
    END as `Total Holiday Rate`,
  (CASE
	WHEN billable = 1 and position.billingSetting.holidayGroup.id and holidayHours is not null THEN holidayHours
    ELSE "0"
    END)
    *
(CASE
	WHEN billable = 1 and position.billingSetting.holidayGroup.id and holidayHours is not null THEN billRate * 1.5
    ELSE "0"
    END) as `Total Bill Holiday Amount`,
    summary.payHolidayHours as `Holiday Hours`,
CASE
    WHEN position.payrollSetting.holiday = 'MULTIPLIER' THEN position.payrollSetting.holidayMultiplier
    WHEN position.payrollSetting.holiday = 'RATE' THEN position.payrollSetting.holidayRate
END AS `Holiday Multiplier Or Rate`,
CASE
    WHEN position.payrollSetting.holiday = 'MULTIPLIER' THEN payRate*position.payrollSetting.holidayMultiplier
    WHEN position.payrollSetting.holiday = 'RATE' THEN position.payrollSetting.holidayRate
END AS `Holiday Rate`,
CASE
    WHEN position.payrollSetting.holiday = 'MULTIPLIER' THEN summary.payHolidayHours*payRate*position.payrollSetting.holidayMultiplier
    WHEN position.payrollSetting.holiday = 'RATE' THEN summary.payHolidayHours*position.payrollSetting.holidayRate
END AS `Holiday Amount`,
summary.payHours as `Total Pay Hours`,
(summary.payHours - summary.payOvertimeHours - summary.payHolidayHours) as `Pay Reg`,payRate as `Pay Rate`,
(summary.payHours - summary.payOvertimeHours - summary.payHolidayHours)*payRate as `Reg Amount`,
summary.payOvertimeHours as `Overtime Hours`, 1.50 as `Overtime Multiplier`, (payRate*1.50) as `Overtime Rate`,
(summary.payOvertimeHours*payRate*1.50) as `Overtime Amount`
from shifts
