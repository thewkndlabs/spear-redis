namespace spearedis.RedisProxy;

public interface IStringKeyValueStore
{
    void Set(string key, string value);

    bool TryGet(string key, out string? value);
}
