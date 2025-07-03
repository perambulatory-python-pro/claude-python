SELECT
  employee.region.name as Market,
  account.name as Location,
  account.parentAccount.name as ParentName,
  account.parentAccount.customId as ParentID,
  employee.lastName as LastName,
  employee.firstName as FirstName,
  employee.customId as EmployeeID,
  sum(shift.summary.payHoursActual) as HoursPaid,
  sum(shift.summary.billHoursActual) as HoursBilled,
  date_format(
    date(
      adddays(shift.summary.periodDate, 6)
    ),
    '%m-%d-%Y'
  ) as WeekEnding
from
  shift_period_summaries
where
  account.parentAccount.customId <> 'RADY_RADY'
group by
  employee.customId,
  WeekEnding
order by
  employee.lastName,
  WeekEnding DESC
limit
  100000