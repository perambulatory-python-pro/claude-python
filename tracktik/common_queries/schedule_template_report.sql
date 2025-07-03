SELECT
  position.account.name AS LocationName,
  position.customId AS PositionID,
  position.name AS PositionName,
  startDayLabel AS StartsOn,
  startTime AS StartTime,
  endTime AS endTime,
  hoursbetween(startTime, endTime) AS ShiftHours,
  endDayLabel AS EndsOn,
  employee.firstName AS FirstName,
  employee.lastName AS LastName,
  startDayIndex,
  endDayIndex
FROM
  shift_templates
ORDER BY
  PositionID,
  startDayIndex
limit
  100000