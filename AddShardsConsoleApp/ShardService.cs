using Microsoft.Azure.SqlDatabase.ElasticScale.ShardManagement;

namespace AddShardsConsoleApp;

internal class ShardService
{
    private readonly string? _databaseName;
    private readonly string? _serverName;
    private readonly ShardMapManager _shardMapManager;
    private readonly ShardMap? _shardMap;

    public ShardService(string? database, string? server, ShardMapManager shardMapManager, string? shardMapName)
    {
        _databaseName = database;
        _serverName = server;
        _shardMapManager = shardMapManager;
        _shardMap = GetShardMap(shardMapName ?? "");
    }

    public int ShowMenu()
    {
        Console.WriteLine("\n*****************************************************");
        Console.WriteLine($"SHARDs MENU [selected shard map: {_shardMap?.Name}]");
        Console.WriteLine("*****************************************************\n");
        Console.WriteLine("Make your choice (1, 2, 3 or 4 and press ENTER):");
        Console.WriteLine("1. List all existing shards");
        Console.WriteLine("2. Add shard");
        Console.WriteLine("3. Delete shard");
        Console.WriteLine("4. Go back");
        Console.WriteLine("*****************************************************\n");
        Console.Write("1, 2, 3 or 4: ");
        int choice = Convert.ToInt32(Console.ReadLine());
        return choice;
    }

    public void SwitchChoice(int choice)
    {
        switch (choice)
        {
            case 1:
                PrintAllExistingShards();
                break;
            case 2:
                GetInfoForAddingShard();
                break;
            case 3:
                GetInfoForDeletingShard();
                break;
            default:
                break;
        }

    }

    private void PrintAllExistingShards()
    {
        List<Shard>? shards = _shardMap?.GetShards().ToList() ?? new List<Shard>();
        Console.WriteLine($"\nShards in database [{_databaseName}] op server [{_serverName}]:");
        foreach (Shard shard in shards)
            Console.WriteLine($"  " + shard.Location.Database);
    }

    private void GetInfoForAddingShard()
    {
        Console.WriteLine("\nPlease enter the database name that you want to add to the shard map [and press ENTER]:");
        string shardName = Console.ReadLine() ?? "";
        Console.WriteLine($"\nPlease enter the server name where the database is located [and press ENTER]. " +
            $"\nLeave blank if you want to use the server where the \nShardMapManager is also located ({_serverName}):");
        string server = Console.ReadLine() ?? "";
        if (server?.Length == 0) server = _serverName;
        Console.WriteLine($"\nNB: You are going to create a shard with name [{shardName}] on server [{server}]");
        Console.WriteLine("Do you want to proceed? (Choose 'Y' to proceed and 'N' to cancel and press ENTER)");
        Console.Write("Y/N: ");
        string choice = Console.ReadLine();
        if (choice?.ToLower() == "y")
        {
            Console.WriteLine("\nTrying to create shard...\n");
            CreateShardIfNotExists(shardName, server);
        }
    }

    private void GetInfoForDeletingShard()
    {
        Console.WriteLine("\nPlease enter the shard name that you want to delete from the shard map [and press ENTER]:");
        string shardName = Console.ReadLine() ?? "";
        Console.WriteLine($"\nNB: You are going to delete a shard with name [{shardName}] from the shard map [{_shardMap?.Name}]");
        Console.WriteLine("Do you want to proceed? (Choose 'Y' to proceed and 'N' to cancel and press ENTER)");
        Console.Write("Y/N: ");
        string choice = Console.ReadLine();
        if (choice?.ToLower() == "y")
        {
            Console.WriteLine("\nTrying to delete shard...\n");
            DeleteShardIfExists(shardName);
        }
    }

    private void CreateShardIfNotExists(string shardName, string server)
    {
        List<string>? shardsInShardMap = _shardMap?.GetShards().Select(shard => shard.Location.Database).ToList();

        if (!shardsInShardMap.Contains(shardName))
        {
            Console.WriteLine($"  {shardName} does not exist, new shard created.");
            _shardMap?.CreateShard(new ShardLocation($"{server}", shardName));
        }
        else
            Console.WriteLine($"  {shardName} already exists, no new shard created.");
    }

    private void DeleteShardIfExists(string shardName)
    {
        var shardList = _shardMap?.GetShards() ?? new List<Shard>();
        foreach (var shard in shardList)
        {
            if (shard.Location.Database.Equals(shardName)) 
            {
                _shardMap?.DeleteShard(shard);
                Console.WriteLine($"  {shardName} deleted.");
                return;
            }
        }
        Console.WriteLine($"  {shardName} does not exist, nothing deleted.");
    }

    public ShardMap? GetShardMap(string shardMapName)
    {
            Console.WriteLine("Trying to get a reference to the ShardMap...");
            try
            {
                ShardMap shardMap = _shardMapManager?.GetShardMap(shardMapName);
                Console.WriteLine("  ShardMap found (" + shardMapName + ")");
                return shardMap;
            }
            catch
            {
                throw new Exception($"Could not get ShardMap reference. Is the name correct?");
            }
    }
}
