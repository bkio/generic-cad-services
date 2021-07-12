/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using BCloudServiceUtilities;
using BCommonUtilities;
using System;
using System.Threading;

namespace CADProcessService.Endpoints.Utilities
{
    public class MemoryLocker
    {
        private static int LOCK_TRIES = 30;
        private static void MakeQueryParameters(out BMemoryQueryParameters _QueryParameters, string _ItemKey)
        {
            _QueryParameters = new BMemoryQueryParameters();
            _QueryParameters.Domain = "LOCKING";
            _QueryParameters.SubDomain = "LOCKER";
            _QueryParameters.Identifier = _ItemKey;
        }

        public static bool LockedAction(string LockItemKey, IBMemoryServiceInterface _MemoryService, Func<bool> LockedAction)
        {
            try
            {
                MakeQueryParameters(out BMemoryQueryParameters QueryParameters, "LOCKS");

                int Tries = 0;

                while (!_MemoryService.SetKeyValueConditionally(QueryParameters, new Tuple<string, BPrimitiveType>(LockItemKey, new BPrimitiveType("busy")), null, false))
                {
                    Tries++;

                    if (Tries >= LOCK_TRIES)
                    {
                        _MemoryService.SetKeyValue(QueryParameters, new Tuple<string, BPrimitiveType>[]
                        {
                        new Tuple<string, BPrimitiveType>(LockItemKey, new BPrimitiveType("busy"))
                        }, null, false);
                        break;
                    }

                    Thread.Sleep(1000);
                }

                if (!_MemoryService.SetKeyExpireTime(QueryParameters, new TimeSpan(0, 0, 30)))
                {
                    return false;
                }

                bool MethodState = LockedAction.Invoke();

                if (_MemoryService.DeleteKey(QueryParameters, LockItemKey, null, false))
                {
                    return MethodState;
                }
                else
                {
                    return false;
                }
            }
            catch (Exception)
            {
                return false;
            }
        }
    }
}
