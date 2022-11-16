namespace AddShardsConsoleApp;

// Console app to add shard maps and shards to the ShardMapManager.
// Set server and credentials env vars before running this app.
// (app checks the end vars before showing the user menu)
// JvdH 20221116

internal class Program
{
    private static string? databaseName;
    private static string? serverName;
    private static string? userName;
    private static string? userPassword;
    private static string? connectionString;

    static void Main(string[] args)
    {
        try
        {
            CheckEnvVars(); // Check if env vars are set and throw error if not.

            ShardMapService shardMapService = new(databaseName, serverName, connectionString);
            int choice = shardMapService.ShowMenu();
            while (choice != 5)
            {
                shardMapService.SwitchChoice(choice);
                choice = shardMapService.ShowMenu();
            }

        }
        catch (Exception e)
        {
            Console.WriteLine("Error: " + e.Message);
        }
    }

    private static void CheckEnvVars()
    {
        serverName = Environment.GetEnvironmentVariable("ShardMapDatabaseServer") ?? throw new Exception("Error: server name where database with shard map is located not set."); ;
        userName = Environment.GetEnvironmentVariable("ShardMapDatabaseUser") ?? throw new Exception("Error: user name for database server of shard map not set.");
        userPassword = Environment.GetEnvironmentVariable("ShardMapDatabasePassword") ?? throw new Exception("Error: password for database server of shard map not set.");
        databaseName = Environment.GetEnvironmentVariable("ShardMapDatabaseName") ?? throw new Exception("Error: database name where shardmap is located not set.");
        connectionString = $"Data Source={serverName};Initial Catalog={databaseName};User ID= {userName};Password= {userPassword};Encrypt=True";
    }
}