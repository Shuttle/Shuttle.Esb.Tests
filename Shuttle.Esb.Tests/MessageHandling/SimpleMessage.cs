using System;

namespace Shuttle.Esb.Tests
{
	[Serializable]
	public class SimpleMessage : object
	{
		public string Name { get; set; }
	}
}