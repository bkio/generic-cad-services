/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints
{
    partial class InternalCalls
    {
        internal class CleanupCall : InternalWebServiceBaseTimeoutable
        {
            private readonly IBDatabaseServiceInterface DatabaseService;
            private readonly IBMemoryServiceInterface MemoryService;

            public CleanupCall(string _InternalCallPrivateKey, IBDatabaseServiceInterface _DatabaseService, IBMemoryServiceInterface _MemoryService) : base(_InternalCallPrivateKey)
            {
                DatabaseService = _DatabaseService;
                MemoryService = _MemoryService;
            }

            public override BWebServiceResponse OnRequest_Interruptable(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
            {
                Cleanup_UniqueFileFields(_ErrorMessageAction);
                //Cleanup_AttributeTables(_ErrorMessageAction);

                return BWebResponse.StatusOK("OK.");
            }

            private void Cleanup_UniqueFileFields(Action<string> _ErrorMessageAction)
            {
                if (!DatabaseService.ScanTable(
                    UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                    out List<JObject> UniqueFieldsEntries,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Cleanup_UniqueFileFields: Table does not exist or ScanTable operation has failed.");
                    return;
                }
                if (UniqueFieldsEntries.Count == 0)
                {
                    return;
                }

                foreach (var Current in UniqueFieldsEntries)
                {
                    if (!Current.ContainsKey(ModelDBEntry.KEY_NAME_MODEL_ID)) continue;

                    if (!Current.ContainsKey(UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME)) continue;
                    var ModelUniqueName = (string)Current[UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME];

                    var ModelID = (string)Current[ModelDBEntry.KEY_NAME_MODEL_ID];
                    var Casted = JsonConvert.DeserializeObject<UniqueFileFieldsDBEntry>(Current.ToString());

                    try
                    {
                        if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), ModelID, _ErrorMessageAction))
                        {
                            continue;
                        }

                        bool bDeleteEntry = false;

                        if (!DatabaseService.GetItem(
                            ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                            ModelDBEntry.KEY_NAME_MODEL_ID,
                            new BPrimitiveType(ModelID),
                            ModelDBEntry.Properties,
                            out JObject ModelObject,
                            _ErrorMessageAction))
                        {
                            continue;
                        }
                        if (ModelObject == null)
                        {
                            //Model does not exist
                            bDeleteEntry = true;
                        }
                        else
                        {
                            var Model = JsonConvert.DeserializeObject<ModelDBEntry>(ModelObject.ToString());

                            bDeleteEntry = ModelUniqueName != Model.ModelName;
                        }

                        if (bDeleteEntry)
                        {
                            DatabaseService.DeleteItem(
                                UniqueFileFieldsDBEntry.DBSERVICE_UNIQUEFILEFIELDS_TABLE(),
                                UniqueFileFieldsDBEntry.KEY_NAME_MODEL_UNIQUE_NAME,
                                new BPrimitiveType(ModelUniqueName),
                                out JObject _,
                                EBReturnItemBehaviour.DoNotReturn,
                                _ErrorMessageAction);
                        }
                    }
                    finally
                    {
                        Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), ModelID, _ErrorMessageAction);
                    }
                }
            }

            private void Cleanup_AttributeTables(Action<string> _ErrorMessageAction)
            {
                //TBD
            }
        }
    }
}