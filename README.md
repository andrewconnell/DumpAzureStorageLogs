DumpAzureStorageLogs
====================

.NET console app used to dump the log entries from an Azure blob storage account to a text file.

I built this app from taking some code found in Azure blogs and adding a filter to only include the files I'm intersted in viewing the analytics for. Specifically this is used for the [Microsoft Cloud Show](http://www.microsoftcloudshow.com), my podcast. I am only intersted in hits on the episode MP3 files and the RSS feed for the podcast.

##Usage
To run, enter the following (*all times in zulu*):

`DumpAzureStorageLogs.exe blob logdump.txt "YYYY-MM-DDTHH:MMZ" "YYYY-MM-DDTHH:MMZ"`