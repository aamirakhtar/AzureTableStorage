using System;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageTest
{
    public class Common
    {
        /// <summary>
        /// Get storage account from connection string by parsing the account
        /// the user hasn't updated this to valid values. 
        /// </summary>
        /// <param name="storageConnectionString">Connection string for the storage service or the emulator</param>
        /// <returns>CloudStorageAccount object</returns>
        public static CloudStorageAccount GetStorageAccountFromConnectionString(string storageConnectionString)
        {
            CloudStorageAccount storageAccount;
            try
            {
                storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            }
            catch (FormatException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the application.");
                throw;
            }
            catch (ArgumentException)
            {
                Console.WriteLine("Invalid storage account information provided. Please confirm the AccountName and AccountKey are valid in the app.config file - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            return storageAccount;
        }


        /// <summary>
        /// Create a table
        /// </summary>
        /// <returns>A CloudTable object</returns>
        public static async Task<CloudTable> CreateTableAsync(string tableName)
        {
            CloudStorageAccount storageAccount = GetStorageAccountFromConnectionString(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();

            Console.WriteLine("Create a Table for the demo");

            CloudTable table = tableClient.GetTableReference(tableName);
            try
            {
                if (await table.CreateIfNotExistsAsync())
                {
                    Console.WriteLine("Created Table named: {0}", tableName);
                }
                else
                {
                    Console.WriteLine("Table {0} already exists", tableName);
                }
            }
            catch (StorageException)
            {
                Console.WriteLine("If you are running with the default configuration please make sure you have started the storage emulator. Press the Windows key and type Azure Storage to select and run it from the list of applications - then restart the sample.");
                Console.ReadLine();
                throw;
            }

            Console.WriteLine();
            return table;
        }
    }
}