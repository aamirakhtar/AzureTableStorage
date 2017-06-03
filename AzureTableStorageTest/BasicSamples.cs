using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using AzureTableStorageTest.Objects;
using System.ComponentModel;

namespace AzureTableStorageTest
{
    public class BasicSamples
    {
        public async Task RunSamples()
        {
            Console.WriteLine("Azure Table Storage - Basic Samples\n");
            Console.WriteLine();

            string tableName = "Customers" + Guid.NewGuid().ToString().Substring(0, 5);

            // Create or reference an existing table
            CloudTable table = await Common.CreateTableAsync(tableName);

            try
            {
                // Demonstrate basic CRUD functionality 
                await BasicDataOperationsAsync(table);

                // Create a SAS and try CRUD operations with the SAS.
                await BasicDataOperationsWithSasAsync(table);
            }
            finally
            {
                // Delete the table
                //await table.DeleteIfExistsAsync();
            }
        }

        private static async Task BasicDataOperationsAsync(CloudTable table)
        {
            Customer customer = new Customer("1", "Aamir Akhtar")
            {
                Email = "aamiradvantage@gmail.com",
                PhoneNumber = "425-555-0101"
            };

            // this will insert if entity does not exists else merge with the existing one
            customer = await InsertOrMergeEntityAsync<Customer>(table, customer);

            customer.PhoneNumber = "425-555-0105";
            await InsertOrMergeEntityAsync<Customer>(table, customer);
            Console.WriteLine();

            customer = await GetEntityAsync<Customer>(table, "1", "Aamir Akhtar");
            Console.WriteLine();

            // Demonstrate how to Delete an entity
            //Console.WriteLine("Delete the entity. ");
            //await DeleteEntityAsync(table, customer);
            Console.WriteLine();
        }

        /// <summary>
        /// Demonstrates basic CRUD operations using a SAS for authentication.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns>A Task object</returns>
        private static async Task BasicDataOperationsWithSasAsync(CloudTable table)
        {
            string sharedAccessPolicyName = "customer-policy";

            // Create a shared access policy on the table.
            // The access policy may be optionally used to provide constraints for
            // shared access signatures on the table.
            await CreateSharedAccessPolicy(table, sharedAccessPolicyName);

            // Generate an ad-hoc SAS on the table, then test the SAS. It permits all CRUD operations on the table.
            string adHocTableSAS = GetTableSasUri(table);

            // Create an instance of a customer entity.
            Customer customer1 = new Customer("2", "Johnson Mary")
            {
                Email = "mary@gmail.com",
                PhoneNumber = "425-555-0105"
            };
            await TestTableSAS(adHocTableSAS, customer1);

            // Generate a SAS URI for the table, using the stored access policy to set constraints on the SAS.
            // Then test the SAS. All CRUD operations should succeed.
            string sharedPolicyTableSAS = GetTableSasUri(table, sharedAccessPolicyName);

            // Create an instance of a customer entity.
            Customer customer2 = new Customer("3", "Wilson Joe")
            {
                Email = "joe@contoso.com",
                PhoneNumber = "425-555-0106"
            };
            await TestTableSAS(sharedPolicyTableSAS, customer2);
        }

        /// <summary>
        /// Returns a URI containing a SAS for the table.
        /// </summary>
        /// <param name="table">A CloudTable object.</param>
        /// <param name="storedPolicyName">A string containing the name of the stored access policy. If null, an ad-hoc SAS is created.</param>
        /// <returns>A string containing the URI for the table, with the SAS token appended.</returns>
        private static string GetTableSasUri(CloudTable table, string storedPolicyName = null)
        {
            string sasTableToken;

            // If no stored policy is specified, create a new access policy and define its constraints.
            if (storedPolicyName == null)
            {
                // Note that the SharedAccessTablePolicy class is used both to define the parameters of an ad-hoc SAS, and 
                // to construct a shared access policy that is saved to the table's shared access policies. 
                SharedAccessTablePolicy adHocPolicy = new SharedAccessTablePolicy()
                {
                    // Permissions enable users to add, update, query, and delete entities in the table.
                    SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
                    Permissions = SharedAccessTablePermissions.Add | SharedAccessTablePermissions.Update |
                        SharedAccessTablePermissions.Query | SharedAccessTablePermissions.Delete
                };

                // Generate the shared access signature on the table, setting the constraints directly on the signature.
                sasTableToken = table.GetSharedAccessSignature(adHocPolicy, null);
            }
            else
            {
                // Generate the shared access signature on the table. In this case, all of the constraints for the
                // shared access signature are specified on the stored access policy, which is provided by name.
                // It is also possible to specify some constraints on an ad-hoc SAS and others on the stored access policy.
                // However, a constraint must be specified on one or the other; it cannot be specified on both.
                sasTableToken = table.GetSharedAccessSignature(null, storedPolicyName);

                Console.WriteLine("SAS for table (stored access policy): {0}", sasTableToken);
                Console.WriteLine();
            }

            // Return the URI string for the table, including the SAS token.
            return table.Uri + sasTableToken;
        }

        private static async Task CreateSharedAccessPolicy(CloudTable table, string policyName)
        {
            // Create a new shared access policy and define its constraints.
            // The access policy provides add, update, and query permissions.
            SharedAccessTablePolicy sharedPolicy = new SharedAccessTablePolicy()
            {
                // Permissions enable users to add, update, query, and delete entities in the table.
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(24),
                Permissions = SharedAccessTablePermissions.Add | SharedAccessTablePermissions.Update |
                        SharedAccessTablePermissions.Query | SharedAccessTablePermissions.Delete
            };

            try
            {
                // Get the table's existing permissions.
                TablePermissions permissions = await table.GetPermissionsAsync();

                // Add the new policy to the table's permissions, and update the table's permissions.
                permissions.SharedAccessPolicies.Add(policyName, sharedPolicy);
                await table.SetPermissionsAsync(permissions);

                //Console.WriteLine("Wait 30 seconds for pemissions to propagate");
                //Thread.Sleep(30);
            }
            catch (StorageException ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// Tests a table SAS to determine which operations it allows.
        /// </summary>
        /// <param name="sasUri">A string containing a URI with a SAS appended.</param>
        /// <param name="customer">The customer entity.</param>
        /// <returns>A Task object</returns>
        private static async Task TestTableSAS(string sasUri, Customer customer)
        {
            // Try performing table operations with the SAS provided.
            // Note that the storage account credentials are not required here; the SAS provides the necessary
            // authentication information on the URI.

            // Return a reference to the table using the SAS URI.
            CloudTable table = new CloudTable(new Uri(sasUri));

            // Upsert (add/update) operations: insert an entity.
            // This operation requires both add and update permissions on the SAS.
            try
            {
                // Insert the new entity.
                customer = await InsertOrMergeEntityAsync<Customer>(table, customer);

                Console.WriteLine("Add operation succeeded for SAS {0}", sasUri);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("Add operation failed for SAS {0}", sasUri);
                    Console.WriteLine("Additional error information: " + e.Message);
                    Console.WriteLine();
                }
                else
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                    throw;
                }
            }

            // Read operation: query an entity.
            // This operation requires read permissions on the SAS.
            Customer customerRead = null;
            try
            {
                //TableOperation retrieveOperation = TableOperation.Retrieve<Customer>(customer.PartitionKey, customer.RowKey);
                //TableResult result = await table.ExecuteAsync(retrieveOperation);
                //customerRead = result.Result as Customer;
                customerRead = await GetEntityAsync<Customer>(table, customer.PartitionKey, customer.RowKey);
                if (customerRead != null)
                {
                    Console.WriteLine("\t{0}\t{1}\t{2}\t{3}", customerRead.PartitionKey, customerRead.RowKey, customerRead.Email, customerRead.PhoneNumber);
                }

                Console.WriteLine("Read operation succeeded for SAS {0}", sasUri);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("Read operation failed for SAS {0}", sasUri);
                    Console.WriteLine("Additional error information: " + e.Message);
                    Console.WriteLine();
                }
                else
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                    throw;
                }
            }

            // Delete operation: delete an entity.
            try
            {
                //if (customerRead != null)
                //{
                //    await DeleteEntityAsync<Customer>(table, customerRead);
                //}

                Console.WriteLine("Delete operation succeeded for SAS {0}", sasUri);
                Console.WriteLine();
            }
            catch (StorageException e)
            {
                if (e.RequestInformation.HttpStatusCode == 403)
                {
                    Console.WriteLine("Delete operation failed for SAS {0}", sasUri);
                    Console.WriteLine("Additional error information: " + e.Message);
                    Console.WriteLine();
                }
                else
                {
                    Console.WriteLine(e.Message);
                    Console.ReadLine();
                    throw;
                }
            }

            Console.WriteLine();
        }

        private static async Task<T> GetEntityAsync<T>(CloudTable table, string rowKey, string partitionKey) where T : class
        {
            try
            {
                TableOperation retrieveOperation = TableOperation.Retrieve<Customer>(partitionKey, rowKey);
                TableResult result = await table.ExecuteAsync(retrieveOperation);

                return (T)result.Result;
                //return (T)TypeDescriptor.GetConverter(typeof(T)).ConvertTo(result.Result, typeof(Customer));
            }
            catch (StorageException ex)
            {
                throw ex;
            }
        }

        private static async Task<T> InsertOrReplaceEntityAsync<T>(CloudTable table, T entity) where T : class
        {
            try
            {
                if (entity != null)
                {
                    TableOperation insertOrReplaceOperation = TableOperation.InsertOrReplace((ITableEntity)entity);

                    TableResult result = await table.ExecuteAsync(insertOrReplaceOperation);
                    return (T)result.Result;
                }

                return default(T);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static async Task<T> InsertOrMergeEntityAsync<T>(CloudTable table, T entity) where T : class
        {
            try
            {
                if (entity != null)
                {
                    TableOperation insertOrMergeOperation = TableOperation.InsertOrMerge((ITableEntity)entity);

                    TableResult result = await table.ExecuteAsync(insertOrMergeOperation);
                    return (T)result.Result;
                }

                return default(T);
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        private static async Task DeleteEntityAsync<T>(CloudTable table, T entity) where T : class
        {
            try
            {
                if (entity != null)
                {
                    TableOperation deleteOperation = TableOperation.Delete((ITableEntity)entity);
                    await table.ExecuteAsync(deleteOperation);
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}