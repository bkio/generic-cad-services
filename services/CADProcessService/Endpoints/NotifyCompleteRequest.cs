using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities;
using ServiceUtilities.All;
using ServiceUtilities.Common;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace CADProcessService.Endpoints
{
    public class NotifyCompleteRequest : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBMemoryServiceInterface MemoryService;
        private readonly IBPubSubServiceInterface PubSubService;

        public NotifyCompleteRequest(IBMemoryServiceInterface _MemoryService, IBDatabaseServiceInterface _DatabaseService, IBPubSubServiceInterface _PubSubService) : base()
        {
            MemoryService = _MemoryService;
            DatabaseService = _DatabaseService;
            PubSubService = _PubSubService;
        }

        public override BWebServiceResponse OnRequest_Interruptable_DeliveryEnsurerUser(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (_Context.Request.HttpMethod != "POST")
            {
                _ErrorMessageAction?.Invoke("StartProcessRequest: POST methods is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST methods is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            using (var InputStream = _Context.Request.InputStream)
            {
                using (var ResponseReader = new StreamReader(InputStream))
                {
                    ConversionProgressInfo ProgressInfo = JsonConvert.DeserializeObject<ConversionProgressInfo>(ResponseReader.ReadToEnd());

                    if (DatabaseService.GetItem(
                            ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                            ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                            new BPrimitiveType(ProgressInfo.ProcessId.ToString()),
                            ProcessHistoryDBEntry.Properties,
                            out JObject _HistoryObject
                        ))
                    {
                        ProcessHistoryDBEntry HistoryEntry = _HistoryObject.ToObject<ProcessHistoryDBEntry>();
                        HistoryEntry.ProcessStatus = ProgressInfo.ProcessStatus;
                        HistoryEntry.CurrentProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage;
                        HistoryEntry.HistoryRecords.Add(new HistoryRecord()
                        {
                            ProcessInfo = ProgressInfo.Info,
                            RecordDate = DateTime.Now.ToString(),
                            RecordProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage
                        });

                        if (!DatabaseService.UpdateItem(
                            ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                            ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                            new BPrimitiveType(ProgressInfo.ProcessId.ToString()),
                            JObject.Parse(JsonConvert.SerializeObject(HistoryEntry)),
                            out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction))
                        {
                            //Error?
                        }
                    }
                    else
                    {
                        ProcessHistoryDBEntry NewEntry = new ProcessHistoryDBEntry
                        {
                            HistoryRecords = new List<HistoryRecord>()
                            {
                                new HistoryRecord()
                                {
                                    ProcessInfo = ProgressInfo.Info,
                                    RecordDate = DateTime.Now.ToString(),
                                    RecordProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage
                                }
                            },

                            CurrentProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage,
                            ModelName = ProgressInfo.ProgressDetails.ModelName,
                            RevisionIndex = ProgressInfo.ProgressDetails.ModelRevision,
                            ProcessStatus = ProgressInfo.ProcessStatus
                        };

                        if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), ProgressInfo.ProcessId.ToString(), _ErrorMessageAction))
                        {
                            return BWebResponse.InternalError($"Failed to get access to database record");
                        }

                        DatabaseService.UpdateItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(ProgressInfo.ProcessId.ToString()),
                            JObject.Parse(JsonConvert.SerializeObject(NewEntry)),
                            out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction);

                        Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(
                            InnerProcessor,
                            ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                            ProgressInfo.ProcessId.ToString(),
                            _ErrorMessageAction);
                    }

                    if (ProgressInfo.ProcessFailed)
                    {
                        if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), ProgressInfo.ProcessId.ToString(), _ErrorMessageAction))
                        {
                            return BWebResponse.InternalError($"Failed to get access to database record");
                        }

                        if (DatabaseService.GetItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(ProgressInfo.ConversionId),
                            FileConversionDBEntry.Properties,
                            out JObject ConversionObject))
                        {
                            FileConversionDBEntry ConversionEntry = ConversionObject.ToObject<FileConversionDBEntry>();

                            ConversionEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;
                            ConversionEntry.ConversionStage = ProgressInfo.ProgressDetails.GlobalCurrentStage;

                            if (!DatabaseService.UpdateItem(
                                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                                new BPrimitiveType(ProgressInfo.ConversionId),
                                JObject.Parse(JsonConvert.SerializeObject(ConversionEntry)),
                                out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                                null,
                                _ErrorMessageAction))
                            {
                                return BWebResponse.Conflict("Failed to update file conversion entry");
                            }
                        }

                        Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), ProgressInfo.ProcessId.ToString(), _ErrorMessageAction);
                        //Do cad service pubsub here
                    }
                    else
                    {
                        if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), ProgressInfo.ProcessId.ToString(), _ErrorMessageAction))
                        {
                            return BWebResponse.InternalError($"Failed to get access to database record");
                        }

                        if (DatabaseService.GetItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(ProgressInfo.ConversionId),
                            FileConversionDBEntry.Properties,
                            out JObject ConversionObject))
                        {
                            FileConversionDBEntry ConversionEntry = ConversionObject.ToObject<FileConversionDBEntry>();

                            ConversionEntry.ConversionStatus = (int)EInternalProcessStage.ProcessComplete;
                            ConversionEntry.ConversionStage = ProgressInfo.ProgressDetails.GlobalCurrentStage;

                            if (!DatabaseService.UpdateItem(
                                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                                new BPrimitiveType(ProgressInfo.ConversionId),
                                JObject.Parse(JsonConvert.SerializeObject(ConversionEntry)),
                                out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                                null,
                                _ErrorMessageAction))
                            {
                                return BWebResponse.Conflict("Failed to update file conversion entry");
                            }
                        }

                        Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), ProgressInfo.ProcessId.ToString(), _ErrorMessageAction);
                    }

                    if (DatabaseService.GetItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(ProgressInfo.VMId),
                    WorkerVMListDBEntry.Properties,
                    out JObject _VMEntry,
                    _ErrorMessageAction))
                    {
                        if (_VMEntry != null)
                        {
                            WorkerVMListDBEntry Entry = _VMEntry.ToObject<WorkerVMListDBEntry>();
                            if (ProgressInfo.ProgressDetails.GlobalCurrentStage != Entry.CurrentProcessStage)
                            {
                                Entry.CurrentProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage;
                                Entry.ProcessStartDate = DateTime.Now.ToString();
                                Entry.VMStatus = (int)EVMStatus.Available;

                                DatabaseService.UpdateItem(
                                WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                                WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                                new BPrimitiveType(ProgressInfo.VMId),
                                JObject.Parse(JsonConvert.SerializeObject(Entry)),
                                out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                                null,
                                _ErrorMessageAction);
                            }
                        }
                        else
                        {
                            return BWebResponse.InternalError($"Failed to get database record");
                        }
                    }


                }
            }

            return BWebResponse.StatusOK("Success");
        }
    }
}
