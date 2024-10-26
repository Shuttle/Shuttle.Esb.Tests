namespace Shuttle.Esb.Tests;

public class IdempotenceCounter
{
    private readonly object _lock = new();

    public int ProcessedCount { get; private set; }

    public void Processed()
    {
        lock (_lock)
        {
            ProcessedCount++;
        }
    }
}