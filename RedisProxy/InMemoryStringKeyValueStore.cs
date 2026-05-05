using System.Collections.Concurrent;

namespace spearedis.RedisProxy;

public sealed class InMemoryStringKeyValueStore : IStringKeyValueStore
{
    private readonly ConcurrentDictionary<string, string> _values = new(StringComparer.Ordinal);

    public void Set(string key, string value)
    {
        _values[key] = value;
    }

    public bool TryGet(string key, out string? value)
    {
        if (_values.TryGetValue(key, out var existing))
        {
            value = existing;
            return true;
        }

        value = null;
        return false;
    }
}
