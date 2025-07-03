SELECT
  region.name as region,
  account.parentAccount.name as customer_job,
  account.parentAccount.customId as parent_ID,
  account.name as location,
  position.customId as position_id,
  position.name as position_name,
  DATE_FORMAT(
    DATE(
      Adddays(periodDate, 6)
    ),
    '%Y-%m-%d'
  ) as week_ending,
  DAYNAME(shift.startDateTime) as start_day,
  DATE_FORMAT(shift.startDateTime, '%m-%d-%Y') as start_date,
  FORMAT(
    sum(payHours),
    2
  ) as hours_paid,
  FORMAT(
    sum(billHoursActual),
    2
  ) as hours_billed,
  FORMAT(
    sum(payHours) - sum(billHours),
    2
  ) as hours_non_billed,
  FORMAT(
    sum(payOvertimeHours),
    2
  ) as ot_hours_paid,
  FORMAT(
    sum(billOvertimeHoursActual),
    2
  ) as ot_hours_billed,
  FORMAT(
    sum(payOvertimeHours) - sum(billOvertimeHours),
    2
  ) as nbot_hours,
  FORMAT(payBaseRateActual, 2) as pay_rate_base,
  FORMAT(
    sum(payTotalActual),
    2
  ) as pay_total,
  FORMAT(billBaseRateActual, 2) as bill_rate_base,
  FORMAT(
    sum(billTotalActual),
    2
  ) as bill_total
FROM
  shift_period_summaries
GROUP BY
  DATE_FORMAT(
    DATE(
      Adddays(periodDate, 6)
    ),
    '%Y-%m-%d'
  ),
  position.customId,
  DAYNAME(shift.startDateTime)
ORDER BY
  DATE_FORMAT(
    DATE(
      Adddays(periodDate, 6)
    ),
    '%Y-%m-%d'
  ),
  DATE_FORMAT(shift.startDateTime, '%Y-%m-%d'),
  account.name,
  CASE WHEN start_day = 'Friday' THEN
    1
  WHEN start_day = 'Saturday' THEN
    2
  WHEN start_day = 'Sunday' THEN
    3
  WHEN start_day = 'Monday' THEN
    4
  WHEN start_day = 'Tuesday' THEN
    5
  WHEN start_day = 'Wednesday' THEN
    6
  WHEN start_day = 'Thursday' THEN
    7
  ELSE
    0
  END
limit
  100000