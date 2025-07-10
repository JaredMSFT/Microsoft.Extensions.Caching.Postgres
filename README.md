# Microsoft.Extensions.Caching.Postgres

A distributed cache implementation using PostgreSQL for .NET applications.

## Installation

Install the NuGet package:
```powershell
dotnet add package Microsoft.Extensions.Caching.Postgres
```
## Configuration

### 1. Configure appsettings.json

Add the following configuration sections to your `appsettings.json` file.  It's also recommended to configure your connection string to use connection pooling:
```json
  "ConnectionStrings": {
    "PostgresCache": "Host=localhost;Port=5432;Username=postgres;Password=yourpassword;Database=yourdatabase;Pooling=true;MinPoolSize=0;MaxPoolSize=100;Timeout=15;"
  },
  "PostgresCache": {
    "SchemaName": "public",
    "TableName": "cache",
    "CreateIfNotExists": true,
    "UseWAL": false,
    "ExpiredItemsDeletionInterval": "00:30:00",
    "DefaultSlidingExpiration": "00:20:00"
  }
```
### Configuration Options

| Property | Required | Default | Description |
|----------|------|---------|-------------|
| `ConnectionString` | yes | | PostgreSQL connection string |
| `SchemaName` | yes | | Schema name for the cache table |
| `TableName` | yes | | Name of the cache table |
| `CreateIfNotExists` | no | `false` | Whether to create the table if it doesn't exist |
| `UseWAL` | no | `false` | Whether to use Write-Ahead Logging |
| `ExpiredItemsDeletionInterval` | no | `00:30:00`| Interval for cleaning up expired items (default: 30 minutes) |
| `DefaultSlidingExpiration` | no | `00:20:00` | Default sliding expiration for cache entries |

### 2. Register the Service

#### Using Configuration Binding
```csharp
using Microsoft.Extensions.DependencyInjection;

var builder = WebApplication.CreateBuilder(args);

// Register Postgres distributed cache
builder.Services.AddDistributedPostgresCache(options => {
    options.ConnectionString = builder.Configuration.GetConnectionString("PostgresCache");
    options.SchemaName = builder.Configuration.GetValue<string>("PostgresCache:SchemaName", "public");
    options.TableName = builder.Configuration.GetValue<string>("PostgresCache:TableName", "cache");
    options.CreateIfNotExists = builder.Configuration.GetValue<bool>("PostgresCache:CreateIfNotExists", true);
    options.UseWAL = builder.Configuration.GetValue<bool>("PostgresCache:UseWAL", false);
    
    // Optional: Configure expiration settings

    var expirationInterval = builder.Configuration.GetValue<string>("PostgresCache:ExpiredItemsDeletionInterval");
    if (!string.IsNullOrEmpty(expirationInterval) && TimeSpan.TryParse(expirationInterval, out var interval)) {
        options.ExpiredItemsDeletionInterval = interval;
    }
    
    var slidingExpiration = builder.Configuration.GetValue<string>("PostgresCache:DefaultSlidingExpiration");
    if (!string.IsNullOrEmpty(slidingExpiration) && TimeSpan.TryParse(slidingExpiration, out var sliding)) {
        options.DefaultSlidingExpiration = sliding;
    }
});

var app = builder.Build();
```

### 3. Using the Cache

Once configured, you can inject and use `IDistributedCache` in your services:
using Microsoft.Extensions.Caching.Distributed;

```csharp
public class MyService {
    private readonly IDistributedCache _cache; 

    public MyService(IDistributedCache cache) {
        _cache = cache;
    }

    public async Task<string> GetDataAsync(string key) {
        var cachedData = await _cache.GetStringAsync(key);
        
        if (cachedData == null) {
            // Fetch data from source
            var data = await FetchDataFromSource();
            
            // Cache the data with options
            var options = new DistributedCacheEntryOptions {
                AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(30),
                SlidingExpiration = TimeSpan.FromMinutes(5)
            };
            
            await _cache.SetStringAsync(key, data, options);
            return data;
        }
        
        return cachedData;
    }
}
```
## Environment Variables

You can also configure the cache using environment variables with the prefix `PostgresCache__`:

```bash
PostgresCache__ConnectionString="Host=localhost;Port=5432;Username=postgres;Password=yourpassword;Database=yourdatabase"
PostgresCache__SchemaName="public"
PostgresCache__TableName="cache"
PostgresCache__CreateIfNotExists="true"
```

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
