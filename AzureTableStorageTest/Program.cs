using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace AzureTableStorageTest
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Azure Table Storage - Samples\n");
            Console.WriteLine();

            //BasicSamples basicSamples = new BasicSamples();
            //basicSamples.RunSamples().Wait();

            AdvancedSamples advancedSamples = new AdvancedSamples();
            advancedSamples.RunSamples().Wait();

            Console.WriteLine();
            Console.WriteLine("Press any key to exit");
            Console.Read();
        }
    }
}
