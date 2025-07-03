SELECT
  region.parentRegion.name as Contract,
  region.name as Market,
  account.parentAccount.name as `Parent Name`,
  account.parentAccount.customId as `Parent ID`,
  account.name as Location,
  position.customId as `Position UID`,
  position.name as `Position Name`,
  DATE_FORMAT(
    DATE(
      Adddays(periodDate, 6)
    ),
    '%m-%d-%Y'
  ) as `Week Ending`,
  DAYNAME(shift.startDateTime) as `Start Day`,
  DATE_FORMAT(shift.startDateTime, '%m-%d-%Y') as `Start Date`,
  FORMAT(
    sum(shift.approvedHours),
    2
  ) as `Hours Approved`,
  FORMAT(
    sum(payHours),
    2
  ) as `Hours Paid`,
  FORMAT(
    sum(billHoursActual),
    2
  ) as `Hours Billed`,
  FORMAT(
    sum(payHours) - sum(billHours),
    2
  ) as `Hours NonBilled`,
  FORMAT(
    sum(payOvertimeHours),
    2
  ) as `Hours OT Paid`,
  FORMAT(
    sum(billOvertimeHoursActual),
    2
  ) as `Hours Billed OT`,
  FORMAT(
    sum(payOvertimeHours) - sum(billOvertimeHours),
    2
  ) as `NBOT Hours`,
  FORMAT(
    (
      sum(payOvertimeImpact) / sum(payOvertimeHours) * (
        sum(payOvertimeHours) - sum(billOvertimeHours)
      )
    ),
    2
  ) as `NBOT $`,
  FORMAT(
    (
      sum(payOvertimeHours) - sum(billOvertimeHours)
    ) / payHours,
    4
  ) as `NBOT %`,
  FORMAT(payBaseRatePlanned, 2) as `Pay Base Rate Planned`,
  FORMAT(payBaseRateActual, 2) as `Pay Base Rate Actual`,
  FORMAT(payEffectiveRateActual, 2) as `Pay Effective Rate Actual`,
  FORMAT(
    sum(payTotalPlanned),
    2
  ) as `Pay Total Planned`,
  FORMAT(
    sum(payTotalActual),
    2
  ) as `Pay Total Actual`,
  FORMAT(billBaseRatePlanned, 2) as `Bill Base Rate Planned`,
  FORMAT(billBaseRateActual, 2) as `Bill Base Rate Actual`,
  FORMAT(billEffectiveRateActual, 2) as `Bill Effective Rate Actual`,
  FORMAT(
    sum(billTotalPlanned),
    2
  ) as `Bill Total Planned`,
  FORMAT(
    sum(billTotalActual),
    2
  ) as `Bill Total Actual`
FROM
  shift_period_summaries
GROUP BY
  DATE_FORMAT(
    DATE(
      Adddays(periodDate, 6)
    ),
    '%m-%d-%Y'
  ),
  position.customId,
  DAYNAME(shift.startDateTime)
ORDER BY
  DATE_FORMAT(
    DATE(
      Adddays(periodDate, 6)
    ),
    '%m-%d-%Y'
  ),
  DATE_FORMAT(shift.startDateTime, '%m-%d-%Y'),
  account.name,
  CASE WHEN `Start Day` = 'Friday' THEN
    1
  WHEN `Start Day` = 'Saturday' THEN
    2
  WHEN `Start Day` = 'Sunday' THEN
    3
  WHEN `Start Day` = 'Monday' THEN
    4
  WHEN `Start Day` = 'Tuesday' THEN
    5
  WHEN `Start Day` = 'Wednesday' THEN
    6
  WHEN `Start Day` = 'Thursday' THEN
    7
  ELSE
    0
  END
limit
  100000