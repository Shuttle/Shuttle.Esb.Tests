using System;

namespace Shuttle.ESB.Tests
{
    [Serializable]
    public class SimpleMessage : object
    {
        public string Name { get; set; }
    }
}
