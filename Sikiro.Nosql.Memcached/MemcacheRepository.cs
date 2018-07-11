using System;
using Enyim.Caching;
using Enyim.Caching.Configuration;
using Enyim.Caching.Memcached;
using Sikiro.Tookits.Extension;
using Sikiro.Tookits.Interfaces;

namespace Sikiro.Nosql.Memcached
{
    public class MemcacheRepository : ICache
    {
        private readonly MemcachedClient _memcachedClient;
        private static readonly MemcachedClientConfiguration Config = new MemcachedClientConfiguration();

        static MemcacheRepository()
        {
            var connString = "Memcached".ValueOfAppSetting();
            Config.AddServer(connString);
            Config.Protocol = MemcachedProtocol.Text;
        }

        public MemcacheRepository()
        {
            _memcachedClient = new MemcachedClient(Config);
        }

        public bool Add(string key, string value, int seconds = 0)
        {
            return seconds <= 0
                ? _memcachedClient.Store(StoreMode.Add, key, value)
                : _memcachedClient.Store(StoreMode.Add, key, value, DateTime.Now.AddSeconds(seconds));
        }

        public bool Add<T>(string key, T value, int seconds = 0) where T : class, new()
        {
            return Add(key, value.ToJson(), seconds);
        }

        public bool Set(string key, string value, int seconds = 0)
        {
            return seconds <= 0
                ? _memcachedClient.Store(StoreMode.Set, key, value)
                : _memcachedClient.Store(StoreMode.Set, key, value, DateTime.Now.AddSeconds(seconds));
        }

        public bool Set<T>(string key, T value, int seconds = 0) where T : class, new()
        {
            return Set(key, value.ToJson(), seconds);
        }

        public string Get(string key)
        {
            return _memcachedClient.Get(key) as string;
        }

        public T Get<T>(string key) where T : class
        {
            return Get(key).FromJson<T>();
        }

        public string GetOrAdd(string key, Func<string> aquire, int seconds = 0)
        {
            var data = Get(key);
            if (data.IsNullOrEmpty())
            {
                data = aquire();
                if (!data.IsNullOrEmpty())
                    Add(key, data, seconds);
            }
            return data;
        }

        public T GetOrAdd<T>(string key, Func<T> aquire, int seconds = 0) where T : class, new()
        {
            var data = Get<T>(key);
            if (data.IsNull())
            {
                data = aquire();
                if (data.IsNotNull())
                    Add(key, data, seconds);
            }
            return data;
        }

        public string GetOrSet(string key, Func<string> aquire, int seconds = 0)
        {
            var data = Get(key);
            if (data.IsNullOrEmpty())
            {
                data = aquire();
                if (!data.IsNullOrEmpty())
                    Set(key, data, seconds);
            }
            return data;
        }

        public T GetOrSet<T>(string key, Func<T> aquire, int seconds = 0) where T : class, new()
        {
            var data = Get<T>(key);
            if (data.IsNull())
            {
                data = aquire();
                if (data.IsNotNull())
                    Set(key, data, seconds);
            }
            return data;
        }

        public bool Contains(string key)
        {
            return _memcachedClient.Get(key) != null;
        }

        public bool Remove(string key)
        {
            return _memcachedClient.Remove(key);
        }
    }
}
