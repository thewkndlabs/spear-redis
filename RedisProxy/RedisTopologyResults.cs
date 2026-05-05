namespace spearedis.RedisProxy;

public readonly record struct RedisReadResult(bool Success, bool Found, string? Value, string? Error)
{
    public static RedisReadResult Missing() => new(true, false, null, null);

    public static RedisReadResult FoundValue(string value) => new(true, true, value, null);

    public static RedisReadResult Failed(string error) => new(false, false, null, error);
}

public readonly record struct RedisWriteResult(bool Success, string? Error)
{
    public static RedisWriteResult Ok() => new(true, null);

    public static RedisWriteResult Failed(string error) => new(false, error);
}
