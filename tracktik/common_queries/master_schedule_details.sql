SELECT 
position.account.region.name as `Market`,
position.account.name AS `Location`,
position.customId AS `Position UID`,
position.name AS `Position Name`,
CASE
	WHEN startDayIndex = 0 THEN 'Friday'
    WHEN startDayIndex = 1 THEN 'Saturday'
    WHEN startDayIndex = 2 THEN 'Sunday'
    WHEN startDayIndex = 3 THEN 'Monday'
    WHEN startDayIndex = 4 THEN 'Tuesday'
    WHEN startDayIndex = 5 THEN 'Wednesday'
    WHEN startDayIndex = 6 THEN 'Thursday'
    ELSE 'N/a'
END as `Start Day`,
startTime AS `Start Time`, 
endTime AS `End Time`,
CASE
WHEN hoursbetween(startTime, endTime) < 0 THEN hoursbetween(startTime,endTime) + 24
ELSE hoursbetween(startTime, endTime) 
END as `MSA Hours`,
CASE
	WHEN endDayIndex = 0 THEN 'Friday'
    WHEN endDayIndex = 1 THEN 'Saturday'
    WHEN endDayIndex = 2 THEN 'Sunday'
    WHEN endDayIndex = 3 THEN 'Monday'
    WHEN endDayIndex = 4 THEN 'Tuesday'
    WHEN endDayIndex = 5 THEN 'Wednesday'
    WHEN endDayIndex = 6 THEN 'Thursday'
    ELSE 'N/a'
END as `End Day`,    
employee.firstName AS `First Name`, 
employee.lastName AS `Last Name`

FROM 
shift_templates

ORDER BY 
`Position UID`, startDayIndex

limit 100000