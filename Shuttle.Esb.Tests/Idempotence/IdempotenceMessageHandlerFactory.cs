﻿using System;
using System.Collections.Generic;
using System.Reflection;

namespace Shuttle.Esb.Tests
{
	internal class IdempotenceMessageHandlerFactory : IMessageHandlerFactory
	{
		private readonly Type _type = typeof (IdempotenceCommand);
		private readonly List<Type> _messageTypesHandled = new List<Type>();
		private readonly IdempotenceCounter _counter = new IdempotenceCounter();

		public void Initialize(IServiceBus bus)
		{
			_messageTypesHandled.Add(_type);
		}

		public object GetHandler(object message)
		{
			if (message.GetType() == _type)
			{
				return new IdempotenceHandler(_counter);
			}

			throw new Exception("Can only handle type of 'IdempotenceCommand'.");
		}

		public void ReleaseHandler(object handler)
		{
		}

	    public IMessageHandlerFactory RegisterHandlers()
	    {
	        return this;
	    }

	    public IMessageHandlerFactory RegisterHandlers(Assembly assembly)
	    {
            return this;
        }

	    public IMessageHandlerFactory RegisterHandler(Type type)
	    {
	        return this;
	    }

	    public IEnumerable<Type> MessageTypesHandled
		{
			get { return _messageTypesHandled; }
		}

		public int ProcessedCount
		{
			get { return _counter.ProcessedCount; }
		}
	}
}