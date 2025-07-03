SELECT account as Account,
account.customId as Account_UID,
serviceDate as Service_Date,
quantity as Quantity,
price as Price,
billItem as Bill_Item,
description as Description,
status as Status
FROM billing_adhoc_items