SELECT employee.region.name as `Business Unit`,
position.account.parentAccount.customId as `Customer Number`,
position.account.parentAccount.name  as `Customer Name`,
position.account.name as `Location`,
position.account.customId as `Location Number`,
position.customId as `Position Number`,
position.name as `Position`,
employee.lastName as `Employee Last Name`,
employee.firstName as `Employee First Name`,
employee.customId as `Employee ID`,
" " as `Employee MI`,
sum(summary.billHoursActual - summary.billOvertimeHoursActual - summary.billHolidayHoursActual) as `Billed RegHours`,
summary.billBaseRateActual as `Billed Regular Rate`,
ROUND((summary.billTotalActual - (summary.billBaseRateActual*summary.billOvertimeHours + summary.billOvertimeImpactActual) - (summary.billBaseRateActual*summary.billHolidayHours + summary.billHolidayImpactActual)),2) as `Bill Regular Wages`,
sum(summary.billOvertimeHoursActual) as `Billed OT Hours`,
(summary.billOvertimeImpactActual/summary.billOvertimeHoursActual) + summary.billBaseRateActual as `Billed OT Rate`,
sum(summary.billHolidayHoursActual) as `Billed Holiday Hours`,
(summary.billHolidayImpactActual/summary.billHolidayHoursActual) + summary.billBaseRateActual  as `Holiday Rate`,
sum(summary.billHoursActual) as `Billed Total Hours`,
sum(summary.billTotalActual) as `Billed Total Wages`,
DATE_FORMAT(ADDHOURS(summary.periodDate,144) , "%m/%d/%Y") as `Week Ending Date`
from shifts
where summary.billHoursActual > 0
group by employee.customId, position.customId, summary.periodDate
order by employee.lastName, position.account.parentAccount.customId

limit 100000