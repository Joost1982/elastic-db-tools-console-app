using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace AddShardsConsoleApp;

internal class ShardMapService
{
    private readonly string? _databaseName;
    private readonly string? _serverName;
    private readonly string? _connectionString;
    private readonly ShardMapManager _shardMapManager;

    public ShardMapService(string? database, string? server, string? connectionString)
    {
        _databaseName = database;
        _serverName = server;
        _connectionString = connectionString;
        //_shardMapManager = GetShardMapManager();
        _shardMapManager = CreateShardMapManagerIfNotExists();
    }

    public int ShowMenu()
    {
        Console.WriteLine("\n*****************************************************");
        Console.WriteLine("SHARD MAP MENU");
        Console.WriteLine("*****************************************************\n");
        Console.WriteLine("Make your choice (1, 2, 3, 4 or 5 and press ENTER):");
        Console.WriteLine("1. List all existing shard maps");
        Console.WriteLine("2. Add shard map");
        Console.WriteLine("3. Delete shard map");
        Console.WriteLine("4. Select shard map (for listing / adding / deleting shards)");
        Console.WriteLine("5. Quit");
        Console.WriteLine("*****************************************************\n");
        Console.Write("1, 2, 3, 4 or 5: ");
        int choice = Convert.ToInt32(Console.ReadLine());
        return choice;
    }

    public void SwitchChoice(int choice)
    {
        switch (choice)
        {
            case 1:
                PrintAllExistingShardMaps();
                break;
            case 2:
                GetInfoForAddingShardMap();
                break;
            case 3:
                GetInfoForDeletingShardMap();
                break;
            case 4:
                SelectShardMap();
                break;
            default:
                break;
        }
    }

    private void SelectShardMap()
    {

        Console.WriteLine("Please enter the shard name and press ENTER");
        Console.Write("Shard Map Name: ");
        string shardName = Console.ReadLine();
        
        ShardService shardService = new(_databaseName, _serverName, _shardMapManager, shardName);

        int choice = shardService.ShowMenu();
        while (choice != 4)
        {
            shardService.SwitchChoice(choice);
            choice = shardService.ShowMenu();
        }
    }


    private void GetInfoForAddingShardMap()
    {
        Console.WriteLine("\nPlease enter the name for the shard map you want to add [and press ENTER]:");
        string shardMapName = Console.ReadLine() ?? "";
        Console.WriteLine($"\nNB: You are going to create a shard map with name [{shardMapName}] on server [{_serverName}]");
        Console.WriteLine("Do you want to proceed? (Choose 'Y' to proceed and 'N' to cancel and press ENTER)");
        Console.Write("Y/N: ");
        string choice = Console.ReadLine();
        if (choice?.ToLower() == "y")
        {
            Console.WriteLine("\nTrying to create shard map...\n");
            CreateShardMapIfNotExists(shardMapName);
        }
    }

    private void CreateShardMapIfNotExists(string shardMapName)
    {
        var existingShardMaps = _shardMapManager?.GetShardMaps().ToList() ?? new List<ShardMap>();

        foreach (var shard in existingShardMaps)
        {
            if (shard.Name.Equals(shardMapName))
            {
                Console.WriteLine($"  {shardMapName} already exists, no new shard created.");
                return;
            }
        }
        _shardMapManager?.CreateListShardMap<int>(shardMapName);
        Console.WriteLine($"  {shardMapName} does not exist, new shard map created.");
    }

    private void GetInfoForDeletingShardMap()
    {
        Console.WriteLine("\nPlease enter the shard map name that you want to delete [and press ENTER]:");
        string shardMapName = Console.ReadLine() ?? "";
        Console.WriteLine($"\nNB: You are going to delete a shard map with name [{shardMapName}] on server [{_serverName}]");
        Console.WriteLine("Do you want to proceed? (Choose 'Y' to proceed and 'N' to cancel and press ENTER)");
        Console.Write("Y/N: ");
        string choice = Console.ReadLine();
        if (choice?.ToLower() == "y")
        {
            Console.WriteLine("\nTrying to delete shard map...\n");
            DeleteShardMapIfExists(shardMapName);
        }
    }

    private void DeleteShardMapIfExists(string shardMapName)
    {
        var existingShardMaps = _shardMapManager?.GetShardMaps().ToList() ?? new List<ShardMap>();

        foreach (var shard in existingShardMaps)
        {
            if (shard.Name.Equals(shardMapName))
            {
                _shardMapManager?.DeleteShardMap(shard);
                Console.WriteLine($"  {shardMapName} deleted.");
                return;
            }
        }
        Console.WriteLine($"  {shardMapName} does not exist, nothing deleted.");
    }

    private void PrintAllExistingShardMaps()
    {
        var shardMaps = _shardMapManager?.GetShardMaps().ToList() ?? new List<ShardMap>();
        Console.WriteLine($"\nShard Maps in database [{_databaseName}] op server [{_serverName}]:");
        foreach (ShardMap shardMap in shardMaps)
            Console.WriteLine($"  " + shardMap.Name);
    }

    private ShardMapManager GetShardMapManager()
    {
        Console.WriteLine("Trying to get a reference to the ShardMapManager on " + _serverName + "...");
        try
        {
            ShardMapManager? shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(_connectionString, ShardMapManagerLoadPolicy.Lazy); // because it already exists!
            Console.WriteLine("  ShardMapManager found.");

            return shardMapManager;
        }
        catch
        {
            throw new Exception($"Could not get ShardMapManager reference. Please check connection string.");
        }
    }


    private ShardMapManager CreateShardMapManagerIfNotExists()
    {
        ShardMapManager shardMapManager;
        bool shardMapManagerExists = ShardMapManagerFactory.TryGetSqlShardMapManager(
                                                _connectionString,
                                                ShardMapManagerLoadPolicy.Lazy,
                                                out shardMapManager);

        if (shardMapManagerExists)
        {
            Console.WriteLine("Shard Map Manager already exists");
            shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(_connectionString, ShardMapManagerLoadPolicy.Lazy); // because it already exists!
        }
        else
        {
            // Create the Shard Map Manager.
            ShardMapManagerFactory.CreateSqlShardMapManager(_connectionString);
            Console.WriteLine("Created SqlShardMapManager");

            shardMapManager = ShardMapManagerFactory.GetSqlShardMapManager(
                    _connectionString,
                    ShardMapManagerLoadPolicy.Lazy);
        }
        
        return shardMapManager;
    }


}
