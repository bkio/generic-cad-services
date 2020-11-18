/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints.Common;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using Metadata = CADFileService.Endpoints.Structures.Metadata;

namespace CADFileService.Controllers
{
    public class Controller_AttributeTables
    {
        private static Controller_AttributeTables Instance = null;
        private Controller_AttributeTables() { }
        public static Controller_AttributeTables Get()
        {
            if (Instance == null)
            {
                Instance = new Controller_AttributeTables();
            }
            return Instance;
        }

        private const string METADATA_V_DELIMITER = "[(---)]";
        private const string METADATA_KV_DELIMITER = "[(___)]";
        private const string METADATA_U_DELIMITER = "[(:::)]";

        public const string MODEL_METADATA_PREFIX = "MM_";
        public const string REVISION_METADATA_PREFIX = "VM_";
        public const string REVISION_METADATA_MRV_DELIMITER = "_";

        private static readonly string[] AttributeTables = new string[]
        {
            AttributeKeyToOwnerDBEntry.TABLE(),
            AttributeKVPairToOwnerDBEntry.TABLE(),
            AttributeKUPairToOwnerDBEntry.TABLE(),
            AttributeKVPairUserToOwnerDBEntry.TABLE()
        };
        private static readonly string[] AttributeTableKeys = new string[]
        {
            AttributeKeyToOwnerDBEntry.KEY_NAME,
            AttributeKVPairToOwnerDBEntry.KEY_NAME,
            AttributeKUPairToOwnerDBEntry.KEY_NAME,
            AttributeKVPairUserToOwnerDBEntry.KEY_NAME
        };
        private static readonly Func<AttributeKeyGeneration_Input, string>[] AttributeKeyGeneration = new Func<AttributeKeyGeneration_Input, string>[]
        {
            new Func<AttributeKeyGeneration_Input, string>((AttributeKeyGeneration_Input _Input) =>
            {
                return _Input.MetadataKey;
            }),
            new Func<AttributeKeyGeneration_Input, string>((AttributeKeyGeneration_Input _Input) =>
            {
                return _Input.MetadataKey + METADATA_KV_DELIMITER + _Input.MetadataCombinedValues;
            }),
            new Func<AttributeKeyGeneration_Input, string>((AttributeKeyGeneration_Input _Input) =>
            {
                return _Input.MetadataKey + METADATA_U_DELIMITER + _Input.UserID;
            }),
            new Func<AttributeKeyGeneration_Input, string>((AttributeKeyGeneration_Input _Input) =>
            {
                return _Input.MetadataKey + METADATA_KV_DELIMITER + _Input.MetadataCombinedValues + METADATA_U_DELIMITER + _Input.UserID;
            })
        };

        public static BPrimitiveType MakeKey(string _MetadataKey, string _UserID)
        {
            return new BPrimitiveType(_MetadataKey + METADATA_U_DELIMITER + _UserID);
        }
        public static BPrimitiveType MakeKey(string _MetadataKey, List<string> _MetadataValues_Ref, string _UserID)
        {
            string Result = _MetadataKey + METADATA_KV_DELIMITER;

            var MetadataValues_Local = new List<string>(_MetadataValues_Ref);

            var c = 0;
            MetadataValues_Local.Sort();
            foreach (var DataValue in MetadataValues_Local)
            {
                Result += DataValue;
                if (++c < MetadataValues_Local.Count)
                {
                    Result += METADATA_V_DELIMITER;
                }
            }
            Result += METADATA_U_DELIMITER + _UserID;

            return new BPrimitiveType(Result);
        }

        private struct AttributeKeyGeneration_Input
        {
            public string MetadataKey;
            public string MetadataCombinedValues;
            public string UserID;
        }

        public class MetadataLocator
        {
            public readonly bool bIsModelMetadata;

            public readonly string ModelID;
            public readonly int RevisionIndex = -1;
            public readonly ulong MetadataNodeID = 0;

            private MetadataLocator() { }
            private MetadataLocator(string _ModelID) 
            {
                bIsModelMetadata = true;
                ModelID = _ModelID;
            }
            private MetadataLocator(string _ModelID, int _RevisionIndex, ulong _MetadataNodeID)
            {
                bIsModelMetadata = false;
                ModelID = _ModelID;
                RevisionIndex = _RevisionIndex;
                MetadataNodeID = _MetadataNodeID;
            }
            public static MetadataLocator ItIsModelMetadata(string _ModelID)
            {
                return new MetadataLocator(_ModelID);
            }
            public static MetadataLocator ItIsRevisionMetadata(string _ModelID, int _RevisionIndex, ulong _MetadataNodeID)
            {
                return new MetadataLocator(_ModelID, _RevisionIndex, _MetadataNodeID);
            }

            public override string ToString()
            {
                if (bIsModelMetadata)
                {
                    return MODEL_METADATA_PREFIX + ModelID;
                }
                return REVISION_METADATA_PREFIX + ModelID + REVISION_METADATA_MRV_DELIMITER + RevisionIndex + REVISION_METADATA_MRV_DELIMITER + MetadataNodeID;
            }
        }

        public enum EAddRemove
        {
            Add,
            Remove
        }
        public enum EKillProcedureIfGetClearanceFails
        {
            Yes,
            No
        }
        public bool AddRemoveMetadataSets_AttributesTables(
            WebServiceBaseTimeoutableDeliveryEnsurerUserProcessor _Request,
            string _UserID,
            MetadataLocator _MetadataLocator,
            List<Metadata> _MetadataList_Ref,
            EAddRemove _Operation,
            EKillProcedureIfGetClearanceFails _KillProcedureIfGetClearanceFails,
            out BWebServiceResponse _FailureResponse,
            Action<string> _ErrorMessageAction = null)
        {
            _FailureResponse = BWebResponse.InternalError("");
            if (_MetadataList_Ref == null || _MetadataList_Ref.Count == 0) return true;

            //Full copy
            var MetadataList_Local = new List<Metadata>();
            for (int i = 0; i < _MetadataList_Ref.Count; i++)
            {
                var Current = _MetadataList_Ref[i];
                MetadataList_Local.Add(new Metadata()
                {
                    MetadataKey = Current.MetadataKey,
                    MetadataValues = new List<string>(Current.MetadataValues)
                });
            }

            var OperationInstance = new AddRemoveMetadataSets_AttributesTables_Operation();
            if (!OperationInstance.ClearanceInstance.GetClearanceForAll(_Request, MetadataList_Local, _ErrorMessageAction))
            {
                if (_KillProcedureIfGetClearanceFails == EKillProcedureIfGetClearanceFails.Yes)
                {
                    OperationInstance.ClearanceInstance.SetClearanceForObtained(_ErrorMessageAction);
                    _FailureResponse = BWebResponse.InternalError("Atomic operation control has failed.");
                    return false;
                }
            }
            //From now on, there should not be a case that it returns false.

            OperationInstance.ProcedureInstance.Perform(_Request.CachedContext, _Operation, MetadataList_Local, _MetadataLocator, _UserID);

            OperationInstance.ClearanceInstance.SetClearanceForObtained(_ErrorMessageAction);

            return true;
        }
        private class AddRemoveMetadataSets_AttributesTables_Operation
        {
            public readonly Procedure ProcedureInstance = new Procedure();
            public readonly Clearance ClearanceInstance = new Clearance();

            public class Procedure
            {
                public void Perform(
                    HttpListenerContext _Context,
                    EAddRemove _Operation, 
                    List<Metadata> _MetadataList,
                    MetadataLocator _MetadataLocator, 
                    string _UserID)
                {
                    Action<HttpListenerContext, string, string, BPrimitiveType, string, BPrimitiveType[]> Function;
                    if (_Operation == EAddRemove.Add)
                    {
                        Function = Controller_DeliveryEnsurer.Get().DB_AddElementsToArrayItem_FireAndForget;
                    }
                    else
                    {
                        Function = Controller_DeliveryEnsurer.Get().DB_RemoveElementsFromArrayItem_FireAndForget;
                    }

                    int ParallelOperationsNumber = _MetadataList.Count * AttributeTables.Length;

                    var ParallelOperationsStack = new Stack<bool>(ParallelOperationsNumber);
                    for (var i = 0; i < ParallelOperationsNumber; i++) ParallelOperationsStack.Push(true);

                    var WaitFor = new ManualResetEvent(false);

                    foreach (var Data in _MetadataList)
                    {
                        var CombinedValues = ""; var c = 0;
                        Data.MetadataValues.Sort();
                        foreach (var DataValue in Data.MetadataValues)
                        {
                            CombinedValues += DataValue;
                            if (++c < Data.MetadataValues.Count)
                            {
                                CombinedValues += METADATA_V_DELIMITER;
                            }
                        }

                        for (var j = 0; j < AttributeTables.Length; j++)
                        {
                            var Table = AttributeTables[j];
                            var Key = AttributeTableKeys[j];
                            var AttributeKey = AttributeKeyGeneration[j](new AttributeKeyGeneration_Input()
                            {
                                MetadataKey = Data.MetadataKey,
                                MetadataCombinedValues = CombinedValues,
                                UserID = _UserID
                            });
                            BTaskWrapper.Run(() =>
                            {
                                //Only the metadata key as index
                                Function(
                                    _Context,
                                    Table,
                                    Key,
                                    new BPrimitiveType(AttributeKey),
                                    AttributeKeyDBEntryBase.METADATA_LOCATOR_PROPERTY,
                                    new BPrimitiveType[]
                                    {
                                        new BPrimitiveType(_MetadataLocator.ToString())
                                    });

                                lock (ParallelOperationsStack)
                                {
                                    ParallelOperationsStack.TryPop(out bool _);
                                    if (ParallelOperationsStack.Count == 0)
                                    {
                                        try
                                        {
                                            WaitFor.Set();
                                        }
                                        catch (Exception) { }
                                    }
                                }
                            });
                        }
                    }

                    try
                    {
                        if (ParallelOperationsNumber > 0)
                        {
                            WaitFor.WaitOne();
                        }
                        WaitFor.Close();
                    }
                    catch (Exception) { }
                }
            }

            public class Clearance
            {
                private WebServiceBaseTimeoutableDeliveryEnsurerUserProcessor RequestOwnerProcessor;

                private readonly HashSet<string> ClearanceObtainedFor = new HashSet<string>();
                public bool GetClearanceForAll(
                    WebServiceBaseTimeoutableDeliveryEnsurerUserProcessor _RequestOwnerProcessor,
                    List<Metadata> _MetadataList,
                    Action<string> _ErrorMessageAction = null)
                {
                    RequestOwnerProcessor = _RequestOwnerProcessor;

                    int ParallelOperationsNumber = _MetadataList.Count * AttributeTables.Length;

                    var ParallelOperationsStack = new Stack<bool>(ParallelOperationsNumber);
                    for (var i = 0; i < ParallelOperationsNumber; i++) ParallelOperationsStack.Push(true);

                    int FailedClearanceOps = 0;

                    var WaitFor = new ManualResetEvent(false);

                    for (var i = 0; i < _MetadataList.Count; i++)
                    {
                        var Data = _MetadataList[i];
                        for (var j = 0; j < AttributeTables.Length; j++)
                        {
                            var Table = AttributeTables[j];
                            BTaskWrapper.Run(() =>
                            {
                                if (!RequestOwnerProcessor.OwnerProcessor.TryGetTarget(out WebServiceBaseTimeoutableProcessor Processor) 
                                    || !Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(
                                        Processor,
                                        Table,
                                        Data.MetadataKey,
                                        _ErrorMessageAction))
                                {
                                    Interlocked.Increment(ref FailedClearanceOps);
                                    return;
                                }
                                lock (ClearanceObtainedFor)
                                {
                                    ClearanceObtainedFor.Add(Data.MetadataKey);
                                }
                                lock (ParallelOperationsStack)
                                {
                                    ParallelOperationsStack.TryPop(out bool _);
                                    if (ParallelOperationsStack.Count == 0)
                                    {
                                        try
                                        {
                                            WaitFor.Set();
                                        }
                                        catch (Exception) { }
                                    }
                                }
                            });
                        }
                    }

                    try
                    {
                        if (ParallelOperationsNumber > 0)
                        {
                            WaitFor.WaitOne();
                        }
                        WaitFor.Close();
                    }
                    catch (Exception) { }

                    return FailedClearanceOps == 0;
                }
                public void SetClearanceForObtained(Action<string> _ErrorMessageAction = null)
                {
                    var WaitFor = new ManualResetEvent(false);

                    var ParallelOperationsStack = new Stack<bool>();
                    int ParallelOperationsNumber = ClearanceObtainedFor.Count * AttributeTables.Length;
                    for (var i = 0; i < ParallelOperationsNumber; i++) ParallelOperationsStack.Push(true);

                    lock (ClearanceObtainedFor)
                    {
                        foreach (var MetadataKey in ClearanceObtainedFor)
                        {
                            for (var j = 0; j < AttributeTables.Length; j++)
                            {
                                var Table = AttributeTables[j];
                                BTaskWrapper.Run(() =>
                                {
                                    if (RequestOwnerProcessor.OwnerProcessor.TryGetTarget(out WebServiceBaseTimeoutableProcessor Processor))
                                    {
                                        Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(
                                            Processor,
                                            Table,
                                            MetadataKey,
                                            _ErrorMessageAction);
                                    }

                                    lock (ParallelOperationsStack)
                                    {
                                        ParallelOperationsStack.TryPop(out bool _);
                                        if (ParallelOperationsStack.Count == 0)
                                        {
                                            try
                                            {
                                                WaitFor.Set();
                                            }
                                            catch (Exception) { }
                                        }
                                    }
                                });
                            }
                        }
                        ClearanceObtainedFor.Clear();
                    }

                    try
                    {
                        if (ParallelOperationsNumber > 0)
                        {
                            WaitFor.WaitOne();
                        }
                        WaitFor.Close();
                    }
                    catch (Exception) { }
                }
            }
        }
    }
}