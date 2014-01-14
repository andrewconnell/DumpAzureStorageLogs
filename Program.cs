// derived from this post: http://blogs.msdn.com/b/windowsazurestorage/archive/2011/08/03/windows-azure-storage-logging-using-logs-to-track-storage-requests.aspx

using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.StorageClient;

namespace DumpAzureStorageLogs {
  class Program {
    const string ConnectionStringKey = "MSCloudShow";
    const string LogStartTime = "StartTime";
    const string LogEndTime = "EndTime";

    static void Main(string[] args) {
      if (args.Length < 3 || args.Length > 4) {
        Console.WriteLine("Usage: DumpLogs <service to search - blob|table|queue> <output file name> <Start time in UTC> <Optional End time in UTC>.");
        Console.WriteLine("Example: DumpLogs blob test.txt \"2011-06-26T22:30Z\" \"2011-06-26T22:50Z\"");
        return;
      }

      string connectionString = ConfigurationManager.AppSettings[ConnectionStringKey];

      CloudStorageAccount account = CloudStorageAccount.Parse(connectionString);
      CloudBlobClient blobClient = account.CreateCloudBlobClient();

      DateTime startTimeOfSearch = DateTime.Parse(args[2]);
      DateTime endTimeOfSearch = DateTime.UtcNow;

      if (args.Length == 4) {
        endTimeOfSearch = DateTime.Parse(args[3]);
      }

      List<CloudBlob> blobList = ListLogFiles(blobClient, args[0], startTimeOfSearch.ToUniversalTime(), endTimeOfSearch.ToUniversalTime());
      DumpLogs(blobList, args[1]);
    }

    /// <summary>
    /// Given service name, start time for search and end time for search, creates a prefix that can be used
    /// to efficiently get a list of logs that may match the search criteria
    /// </summary>
    /// <param name="service"></param>
    /// <param name="startTime"></param>
    /// <param name="endTime"></param>
    /// <returns></returns>
    static string GetSearchPrefix(string service, DateTime startTime, DateTime endTime) {
      StringBuilder prefix = new StringBuilder("$logs/");

      prefix.AppendFormat("{0}/", service);

      // if year is same then add the year
      if (startTime.Year == endTime.Year) {
        prefix.AppendFormat("{0}/", startTime.Year);
      } else {
        return prefix.ToString();
      }

      // if month is same then add the month
      if (startTime.Month == endTime.Month) {
        prefix.AppendFormat("{0:D2}/", startTime.Month);
      } else {
        return prefix.ToString();
      }

      // if day is same then add the day
      if (startTime.Day == endTime.Day) {
        prefix.AppendFormat("{0:D2}/", startTime.Day);
      } else {
        return prefix.ToString();
      }

      // if hour is same then add the hour
      if (startTime.Hour == endTime.Hour) {
        prefix.AppendFormat("log-{0:D2}00", startTime.Hour);
      }

      return prefix.ToString();
    }

    /// <summary>
    /// Given a service, start time, end time, provide list of log files
    /// </summary>
    /// <param name="blobClient"></param>
    /// <param name="serviceName">The name of the service interested in</param>
    /// <param name="startTimeForSearch">Start time for the search</param>
    /// <param name="endTimeForSearch">End time for the search</param>
    /// <returns></returns>
    static List<CloudBlob> ListLogFiles(CloudBlobClient blobClient, string serviceName, DateTime startTimeForSearch, DateTime endTimeForSearch) {
      List<CloudBlob> selectedLogs = new List<CloudBlob>();

      // form the prefix to search. Based on the common parts in start and end time, this prefix is formed
      string prefix = GetSearchPrefix(serviceName, startTimeForSearch, endTimeForSearch);

      Console.WriteLine("Prefix used for log listing = {0}", prefix);

      // List the blobs using the prefix
      IEnumerable<IListBlobItem> blobs = blobClient.ListBlobsWithPrefix(
          prefix,
          new BlobRequestOptions() {
            UseFlatBlobListing = true,
            BlobListingDetails = BlobListingDetails.Metadata
          });


      // iterate through each blob and figure the start and end times in the metadata
      foreach (IListBlobItem item in blobs) {
        CloudBlob log = item as CloudBlob;
        if (log != null) {
          // we will exclude the file if the file does not have log entries in the interested time range.
          DateTime startTime = DateTime.Parse(log.Metadata[LogStartTime]).ToUniversalTime();
          DateTime endTime = DateTime.Parse(log.Metadata[LogEndTime]).ToUniversalTime();

          bool exclude = (startTime > endTimeForSearch || endTime < startTimeForSearch);

          Console.WriteLine("{0} Log {1} Start={2:U} End={3:U}.",
              exclude ? "Ignoring" : "Selected",
              log.Uri.AbsoluteUri,
              startTime,
              endTime);

          if (!exclude) {
            selectedLogs.Add(log);
          }
        }
      }

      return selectedLogs;
    }


    /// <summary>
    /// Dump all log entries to file irrespective of the time.
    /// </summary>
    /// <param name="blobList"></param>
    /// <param name="fileName"></param>
    static void DumpLogs(List<CloudBlob> blobList, string fileName) {

      if (blobList.Count > 0) {
        Console.WriteLine("Dumping log entries from {0} files to '{1}'", blobList.Count, fileName);
      } else {
        Console.WriteLine("No logs files have selected.");
      }

      using (StreamWriter writer = new StreamWriter(fileName)) {
        writer.Write(
            "Log version; Transaction Start Time; REST Operation Type; Transaction Status; HTTP Status; E2E Latency; Server Latency; Authentication Type; Accessing Account; Owner Account; Service Type;");
        writer.Write(
            "Request url; Object Key; RequestId; Operation #; User IP; Request Version; Request Header Size; Request Packet Size; Response Header Size;");
        writer.WriteLine(
            "Response Packet Size; Request Content Length; Request MD5; Server MD5; Etag returned; LMT; Condition Used; User Agent; Referrer; Client Request Id");
        foreach (CloudBlob blob in blobList) {
          using (Stream stream = blob.OpenRead()) {
            using (StreamReader reader = new StreamReader(stream)) {
              string logEntry;
              while ((logEntry = reader.ReadLine()) != null) {
                // if it's the RSS request or the episode request...
                if (logEntry.Contains("mp3.xml") || logEntry.Contains("microsoftcloudshow_e"))
                  writer.WriteLine(logEntry);
              }
            }
          }
        }
      }
    }

  }
}
