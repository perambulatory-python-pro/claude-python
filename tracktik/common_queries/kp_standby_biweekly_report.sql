SELECT
  id as ShiftId,
  summary.region.parentRegion.name as Contract,
  position.account.region.name as Market,
  position.account.name as Location,
  position.account.customId AS ContractUID,
  position.customId AS PositionUID,
  position.name AS PositionName,
  employee.firstName AS FirstName,
  employee.lastName AS LastName,
  DATE_FORMAT(
    timezonify(
      startDateTime, position.account.timeZone
    ),
    "%m-%d-%Y"
  ) AS DateClockIn,
  DATE_FORMAT(
    timezonify(
      startDateTime, position.account.timeZone
    ),
    "%H:%i"
  ) AS TimeClockIn,
  DATE_FORMAT(
    timezonify(
      endDateTime, position.account.timeZone
    ),
    "%m-%d-%Y"
  ) AS DateClockOut,
  DATE_FORMAT(
    timezonify(
      endDateTime, position.account.timeZone
    ),
    "%H:%i"
  ) AS TimeClockOut,
  Round(clockedHours, 2) AS HoursClocked,
  plannedDurationHours AS HoursApproved,
  payableHours AS HoursPaid,
  billableHours AS HoursBilled,
  summary.billBaseRateActual AS BillRateReg,
  summary.billEffectiveRateActual as BillRateEff,
  summary.billOvertimeHours AS BillOTHours,
  summary.billOvertimeImpactActual AS BillOTImpact,
  summary.billTotal as BillTotal
FROM
  shifts
WHERE
  position.name LIKE '%STAND%'
  OR position.name LIKE '%WATCH'
order by
  startDateTime desc,
  `Market`,
  `Location`,
  employee.lastName
limit
  25000