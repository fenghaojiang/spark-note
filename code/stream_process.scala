

var purchaseByCustomerPerHour =  streamingDataFrame
    .selectExpr(
        "CustomerId",
        "(UnitPrice * Quantity as total_cost",
        "InvoiceDate")
    .groupBy(
        $"CustomerId", window($"InvoiceDate", "1 day"))
    .sum("total_cost")


purchaseByCustomerPerHour.writeStream
    .format("memory")
    .queryName("customer_purchases")
    .outputMode("complete")
    .start()
