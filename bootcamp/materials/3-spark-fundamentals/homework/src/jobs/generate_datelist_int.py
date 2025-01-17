"""
This Spark job converts 4_generate_datelist_int.sql from PostgreSQL to SparkSQL.
It converts datelist to a binary sequence to simplify analysis.
"""
from datetime import datetime
import calendar

from pyspark.sql import SparkSession


def convert_datelist_to_binary(spark_session, user_devices_cumulated_df, today, datetime_format="%Y-%m-%d"):
    """
    This function aims to convert the date array to a binary sequence (bit 32). Since a month has less than 32 days,
    only takes into account the first x bits of the sequence in which x is the number of days for that month.
    """
    today_date = datetime.strptime(today, datetime_format)
    first_of_month = today_date.replace(day=1).strftime(datetime_format)
    last_of_month = today_date.replace(day=calendar.monthrange(today_date.year, today_date.month)[1]).strftime(datetime_format)
    user_devices_cumulated_df.createOrReplaceTempView("user_devices_cumulated")

    query = f"""
        WITH series_date AS (
            SELECT
                /*
                    SparkSQL does not have generate_series as in PostgreSQL, hence, we need to 
                    combine sequence to generate an array of the series, then explode the array to have one date per row.
                */
                explode(sequence(to_date('{first_of_month}'), to_date('{last_of_month}'), interval 1 day)) as valid_date
            ),
            starter AS (
            SELECT
                /* 
                    While in PostgreSQL we need to compare if an array is a subset of another array,
                    SparkSQL determines if an array contains a specific element or not, hence, array_contains,
                    and we dont need to put valid_date in an array.
                
                */
                array_contains(udc.device_activity_datelist, sd.valid_date) AS is_active,
                EXTRACT(DAY FROM DATE('{last_of_month}') - sd.valid_date) AS days_since,
                udc.user_id,
                udc.browser_type
            FROM user_devices_cumulated udc
            CROSS JOIN series_date sd
            WHERE date = DATE('{today}')
        ),
            bits AS (
                SELECT
                    user_id,
                    browser_type,
                    /*
                        SparkSQL does not have Bit(32) type, so use binary type instead.
                    */
                    CAST(CAST(SUM(CASE
                            WHEN is_active THEN POW(2, 32 - days_since)
                            ELSE 0 END) AS bigint) AS binary) AS datelist_int,
                    DATE('{today}') as date
                FROM starter
                GROUP BY user_id, browser_type
        )

        SELECT * FROM bits
    """
    return spark_session.sql(query)


if __name__ == "__main__":
    spark = SparkSession.builder.master("local").appName("hw-testing-2").getOrCreate()

    ds = "2023-01-01"
    user_devices_cumulated_bits = convert_datelist_to_binary(
        spark,
        spark.table("testing.user_devices_cumulated"),
        ds,
    )
    try:
        user_devices_cumulated_bits.write.mode("append").insertInto("testing.user_devices_cumulated_bits")
    except:
        user_devices_cumulated_bits.write.mode("overwrite").saveAsTable("testing.user_devices_cumulated_bits")
