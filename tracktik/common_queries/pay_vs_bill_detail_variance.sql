SELECT
  account.region.parentRegion.name as `Contract`,
  position.account.region.name as `Market`,
  position.account.name AS `Location`,
  position.customId AS `Position UID`,
  position.name AS `Position Name`,
  employee.firstName AS FirstName,
  employee.lastName AS LastName,
  DATE_FORMAT(
    timezonify(startedOn, account.timeZone),
    "%m-%d-%Y"
  ) AS DateClockIn,
  DATE_FORMAT(
    timezonify(startedOn, account.timeZone),
    "%H:%i"
  ) AS TimeClockIn,
  DATE_FORMAT(
    timezonify(endedOn, account.timeZone),
    "%m-%d-%Y"
  ) AS DateClockOut,
  DATE_FORMAT(
    timezonify(endedOn, account.timeZone),
    "%H:%i"
  ) AS TimeClockOut,
  Round(shift.clockedHours, 2) AS HoursClocked,
  shift.plannedDurationHours AS HoursApproved,
  shift.payableHours AS HoursPaid,
  shift.billableHours AS HoursBilled,
  Round(
    (
      shift.payableHours - shift.billableHours
    ),
    2
  ) as HoursNonBilled,
  ROUND(
    (
      shift.payRate * (
        shift.payableHours - shift.billableHours
      )
    ),
    2
  ) as Loss
From
  work_sessions
group by
  employee.name,
  position.customId,
  shift.startDateTime
order by
  `Market`,
  `Location`,
  startedOn,
  employee.lastName
limit
  10000