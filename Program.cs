using CsvHelper;
using ETLCSVProject;
using Microsoft.Data.SqlClient;
using System;
using System.Data;
using System.Globalization;

class Program
{
    static void Main(string[] args)
    {
        string filePath = @"C:\Users\bogac\Downloads\sample-cab-data.csv";
        List<CabInfo> records = new List<CabInfo>();
        try
        {
            using (var reader = new StreamReader(filePath))
            using (var csv = new CsvReader(reader, CultureInfo.InvariantCulture))
            {
                csv.Read();
                csv.ReadHeader();
                while (csv.Read())
                {
                    var record = new CabInfo()
                    {
                        // using ternary operator to ensure accurate work
                        // replaced empty spaces with minimum value
                        tpep_pickup_datetime = IsDateTime(csv.GetField<string>("tpep_pickup_datetime"))
                        ? DateTime.ParseExact(csv.GetField<string>("tpep_pickup_datetime"), "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture) 
                        : DateTime.MinValue,
                        tpep_dropoff_datetime = IsDateTime(csv.GetField<string>("tpep_dropoff_datetime"))
                        ? DateTime.ParseExact(csv.GetField<string>("tpep_dropoff_datetime"), "MM/dd/yyyy hh:mm:ss tt", CultureInfo.InvariantCulture) 
                        : DateTime.MinValue,
                        passenger_count = string.IsNullOrWhiteSpace(csv.GetField("passenger_count")) 
                        ? 0 
                        : csv.GetField<int>("passenger_count"),
                        trip_distance = string.IsNullOrWhiteSpace(csv.GetField("trip_distance"))  
                        ? 0.0f 
                        : csv.GetField<float>("trip_distance"),
                        store_and_fwd_flag = string.IsNullOrWhiteSpace(csv.GetField("store_and_fwd_flag")) 
                        ? "No" 
                        : ConvertFlag(csv.GetField<string>("store_and_fwd_flag")),
                        PULocationID = string.IsNullOrWhiteSpace(csv.GetField("passenger_count")) 
                        ? 0 
                        : csv.GetField<int>("PULocationID"),
                        DOLocationID = string.IsNullOrWhiteSpace(csv.GetField("passenger_count")) 
                        ? 0 
                        : csv.GetField<int>("DOLocationID"),
                        fare_amount = string.IsNullOrWhiteSpace(csv.GetField("trip_distance")) 
                        ? 0.0m 
                        : csv.GetField<decimal>("fare_amount"),
                        tip_amount = string.IsNullOrWhiteSpace(csv.GetField("trip_distance")) 
                        ? 0.0m 
                        : csv.GetField<decimal>("tip_amount")
                    };
                    records.Add(record);
                }
            }
        }
        catch (IOException ex)
        {
            Console.WriteLine($"I/O error: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unexpected error: {ex.Message}");
        }

        Console.WriteLine($"Records: {records.Count}");

        //take the first value from the  received groups
        var unique = records
            .GroupBy(r => new { r.tpep_pickup_datetime, r.tpep_dropoff_datetime, r.passenger_count})
            .Select(r => r.First())
            .ToList();

        //take all records where group amount is bigger than one and skip the first one in other cases
        var duplicates = records
            .GroupBy(r => new { r.tpep_pickup_datetime, r.tpep_dropoff_datetime, r.passenger_count })
            .Where(g => g.Count() > 1)
            .SelectMany(g => g.Skip(1))
            .ToList();

        Console.WriteLine($"Unique records: {unique.Count}");
        Console.WriteLine($"Duplicates found: {duplicates.Count}");

        try
        {
            using (var writer = new StreamWriter("duplicates.csv"))
            using (var csv = new CsvWriter(writer, CultureInfo.InvariantCulture))
            {
                csv.WriteRecords(duplicates);
            }
        }
        catch (IOException ex)
        {
            Console.WriteLine($"I/O error: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unexpected error: {ex.Message}");
        }

        BulkInsertion(unique);
    }

    static bool IsDateTime(string dateTime)
    {
        if (string.IsNullOrWhiteSpace(dateTime))
        {
            Console.WriteLine("Empty or null DateTime field encountered.");
            return false;
        }
        //en format fot match csv format
        // ParseExact to ensure correct compare
        string format = "MM/dd/yyyy hh:mm:ss tt"; 
        return DateTime.TryParseExact(dateTime, format, CultureInfo.InvariantCulture, DateTimeStyles.None, out _);
    }

    static string ConvertFlag(string flag)
    {
        return flag.Trim() == "Y" ? "Yes" : "No";
    }

    static DateTime ConvertToUtc(DateTime dateTime)
    {
        TimeZoneInfo EstTimezone = TimeZoneInfo.FindSystemTimeZoneById("Eastern Standard Time");
        return TimeZoneInfo.ConvertTimeToUtc(dateTime, EstTimezone);
    }

    static void BulkInsertion(List<CabInfo> list)
    {
        string connectionString = @"Data Source=DESKTOP-9TN4V09\SQLEXPRESS;Initial Catalog=ETLSQLProject;Integrated Security=True;Connect Timeout=30;Encrypt=True;Trust Server Certificate=True;Application Intent=ReadWrite;Multi Subnet Failover=False";
        try
        {
            //using SqlBulkCopy for efficient bulk insertion
            using (var conection = new SqlConnection(connectionString))
            {
                conection.Open();
                using (var bulkCopy = new SqlBulkCopy(conection))
                {
                    bulkCopy.DestinationTableName = "CabInfo";
                    var table = new DataTable();
                    table.Columns.Add("tpep_pickup_datetime", typeof(DateTime));
                    table.Columns.Add("tpep_dropoff_datetime", typeof(DateTime));
                    table.Columns.Add("passenger_count", typeof(int));
                    table.Columns.Add("trip_distance", typeof(float));
                    table.Columns.Add("store_and_fwd_flag", typeof(string));
                    table.Columns.Add("PULocationID", typeof(int));
                    table.Columns.Add("DOLocationID", typeof(int));
                    table.Columns.Add("fare_amount", typeof(decimal));
                    table.Columns.Add("tip_amount", typeof(decimal));

                    foreach (var record in list)
                    {
                        table.Rows.Add(record.tpep_pickup_datetime, record.tpep_dropoff_datetime, record.passenger_count, record.trip_distance,
                            record.store_and_fwd_flag, record.PULocationID, record.DOLocationID, record.fare_amount, record.tip_amount);
                    }
                    bulkCopy.WriteToServer(table);
                }
            }
        }
        catch (SqlException ex)
        {
            Console.WriteLine($"SQL error: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Unexpected error: {ex.Message}");
        }
    }
}