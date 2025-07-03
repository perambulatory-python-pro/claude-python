SELECT
  DATE_FORMAT(
    ADDDAYS(periodDate, 6),
    '%m-%d-%Y'
  ) as `Week Ending`,
  account.parentAccount.name as `Parent Name`,
  account.parentAccount.customId as `Parent ID`,
  account.name as `Account Name`,
  sum(billHoursActual) as `Total Bill Hours`
FROM
  shift_period_summaries
GROUP BY
  `Parent ID`,
  `Parent Name`
ORDER BY
  `Account Name`
LIMIT
  10000