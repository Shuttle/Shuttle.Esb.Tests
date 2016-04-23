namespace Shuttle.Esb.Tests
{
	internal class ExceptionAssertion
	{
		public ExceptionAssertion(string name)
		{
			Name = name;
		}

		public string Name { get; private set; }

		public void MarkAsRun()
		{
			HasRun = true;
		}

		public bool HasRun { get; private set; }
	}
}