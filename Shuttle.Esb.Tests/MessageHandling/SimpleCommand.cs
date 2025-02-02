using Shuttle.Core.Contract;

namespace Shuttle.Esb.Tests;

public class SimpleCommand : object
{
    public SimpleCommand()
        : this(Guard.AgainstNullOrEmptyString(typeof(SimpleCommand).FullName))
    {
    }

    public SimpleCommand(string name)
    {
        Name = name;
    }

    public string Context { get; set; } = string.Empty;

    public string Name { get; set; }
}