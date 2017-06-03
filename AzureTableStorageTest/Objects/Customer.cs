using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageTest.Objects
{
    public class Customer : TableEntity
    {
        public Customer()
        {
        }

        public Customer(string rowKey, string partitionKey)
        {
            RowKey = rowKey;
            PartitionKey = partitionKey;
        }

        public string Email { get; set; }
        public string PhoneNumber { get; set; }
    }
}
