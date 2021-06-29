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
    public class NotifyProgressRequest : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBMemoryServiceInterface MemoryService;
        private readonly IBPubSubServiceInterface PubSubService;

        public NotifyProgressRequest(IBMemoryServiceInterface _MemoryService, IBDatabaseServiceInterface _DatabaseService, IBPubSubServiceInterface _PubSubService) : base()
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
                _ErrorMessageAction?.Invoke("NotifyProcessRequest: POST methods is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST methods is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            using (var InputStream = _Context.Request.InputStream)
            {
                using (var ResponseReader = new StreamReader(InputStream))
                {
                    string Payload = ResponseReader.ReadToEnd();
                    ConversionProgressInfo ProgressInfo = JsonConvert.DeserializeObject<ConversionProgressInfo>(Payload);

                    if (!UpdateProcessHistoryRecord(ProgressInfo, _ErrorMessageAction, out BWebServiceResponse FailureResponse))
                    {
                        return FailureResponse;
                    }

                    if (!UpdateFileConversionRecord(ProgressInfo, _ErrorMessageAction, out FailureResponse))
                    {
                        return FailureResponse;
                    }

                    if (!UpdateVMEntryRecord(ProgressInfo, _ErrorMessageAction, out FailureResponse))
                    {
                        return FailureResponse;
                    }
                }
            }

            return BWebResponse.StatusOK("Success");
        }

        private bool UpdateProcessHistoryRecord(ConversionProgressInfo ProgressInfo, Action<string> _ErrorMessageAction, out BWebServiceResponse _FailureResponse)
        {
            _FailureResponse = BWebResponse.InternalError("");
            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), ProgressInfo.ProcessId.ToString(), _ErrorMessageAction))
                {
                    _FailureResponse = BWebResponse.InternalError($"Failed to get access to database record");
                    return false;
                }

                ProcessHistoryDBEntry HistoryEntry = null;

                if (DatabaseService.GetItem(
                    ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                    ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                    new BPrimitiveType(ProgressInfo.ProcessId.ToString()),
                    ProcessHistoryDBEntry.Properties,
                    out JObject _HistoryObject
                    ))
                {
                    if (_HistoryObject != null)
                    {
                        HistoryEntry = _HistoryObject.ToObject<ProcessHistoryDBEntry>();
                        HistoryEntry.ProcessStatus = ProgressInfo.ProcessStatus;
                        HistoryEntry.ProcessStatusInfo = ProgressInfo.Info;
                        HistoryEntry.CurrentProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage;
                        HistoryEntry.ModelName = ProgressInfo.ProgressDetails.ModelName;

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
                            _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateProcessHistoryRecord-> Failed to update process history entry. ProgressInfo.ProcessId: {ProgressInfo.ProcessId}");
                            _FailureResponse = BWebResponse.InternalError($"Failed to update process history entry.");
                            return false;
                        }
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateProcessHistoryRecord-> Failed to get process history entry, database get item returned false. ProgressInfo.ProcessId: {ProgressInfo.ProcessId}");
                }

                if (HistoryEntry == null)
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
                        ProcessStatus = ProgressInfo.ProcessStatus,
                    };

                    if (!DatabaseService.UpdateItem(
                        ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                        ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                        new BPrimitiveType(ProgressInfo.ProcessId.ToString()),
                        JObject.Parse(JsonConvert.SerializeObject(NewEntry)),
                        out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                        null,
                        _ErrorMessageAction))
                    {
                        _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateProcessHistoryRecord-> Failed to update process history entry. ProgressInfo.ProcessId: {ProgressInfo.ProcessId}");
                        _FailureResponse = BWebResponse.InternalError($"Failed to update process history entry.");
                        return false;
                    }
                }
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateProcessHistoryRecord-> Error occurred. Message: {ex.Message} -  StackTrace: {ex.StackTrace}");
                _FailureResponse = BWebResponse.InternalError($"Failed to update record in process history entry.");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), ProgressInfo.ProcessId.ToString(), _ErrorMessageAction);
            }
            return true;
        }

        private bool UpdateFileConversionRecord(ConversionProgressInfo ProgressInfo, Action<string> _ErrorMessageAction, out BWebServiceResponse _FailureResponse)
        {
            _FailureResponse = BWebResponse.InternalError("");

            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), ProgressInfo.ConversionId.ToString(), _ErrorMessageAction))
                {
                    _FailureResponse = BWebResponse.InternalError($"Failed to get access to database record");
                    return false;
                }

                if (DatabaseService.GetItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(ProgressInfo.ConversionId),
                    FileConversionDBEntry.Properties,
                    out JObject ConversionObject))
                {
                    if (ConversionObject != null)
                    {
                        FileConversionDBEntry ConversionEntry = ConversionObject.ToObject<FileConversionDBEntry>();

                        if (ProgressInfo.ProcessFailed)
                        {
                            ConversionEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;
                        }
                        else
                        {
                            ConversionEntry.ConversionStatus = ProgressInfo.ProcessStatus;
                        }
                        ConversionEntry.ConversionStage = ProgressInfo.ProgressDetails.GlobalCurrentStage;
                        ConversionEntry.Error = ProgressInfo.Error;

                        if (!DatabaseService.UpdateItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(ProgressInfo.ConversionId),
                            JObject.Parse(JsonConvert.SerializeObject(ConversionEntry)),
                            out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction))
                        {
                            _FailureResponse = BWebResponse.Conflict("Failed to update file conversion entry.");
                            _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateFileConversionRecord->Failed to update file conversion entry. ProgressInfo.ConversionId: {ProgressInfo.ConversionId}");
                            return false;
                        }
                    }
                    else
                    {
                        _FailureResponse = BWebResponse.Conflict($"Failed to get file conversion entry, value is empty.");
                        _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateFileConversionRecord->Failed to get file conversion entry, value is empty. ProgressInfo.ConversionId: {ProgressInfo.ConversionId}");
                        return false;
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateFileConversionRecord-> Failed to get file conversion entry, database get item returned false. ProgressInfo.ConversionId: {ProgressInfo.ConversionId}");
                }
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateFileConversionRecord-> Error occurred. Message: {ex.Message} -  StackTrace: {ex.StackTrace}");
                _FailureResponse = BWebResponse.InternalError($"Failed to update file conversion entry.");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(), ProgressInfo.ConversionId.ToString(), _ErrorMessageAction);
            }
            return true;
        }

        private bool UpdateVMEntryRecord(ConversionProgressInfo ProgressInfo, Action<string> _ErrorMessageAction, out BWebServiceResponse _FailureResponse)
        {
            _FailureResponse = BWebResponse.InternalError("");

            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), ProgressInfo.VMId.ToString(), _ErrorMessageAction))
                {
                    _FailureResponse = BWebResponse.InternalError($"Failed to get access to database record");
                    return false;
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
                            if (ProgressInfo.ProcessFailed)
                            {
                                Entry.LastKnownProcessStatus = (int)EProcessStatus.Failed;
                            }
                            else
                            {
                                Entry.LastKnownProcessStatus = (int)EProcessStatus.Processing;
                            }
                            Entry.LastKnownProcessStatusInfo = ProgressInfo.Info;

                            if (!DatabaseService.UpdateItem(
                                WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                                WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                                new BPrimitiveType(ProgressInfo.VMId),
                                JObject.Parse(JsonConvert.SerializeObject(Entry)),
                                out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                                null,
                                _ErrorMessageAction))
                            {
                                _FailureResponse = BWebResponse.Conflict("Failed to update vm entry.");
                                _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateVMEntryRecord-> Failed to update vm entry. ProgressInfo.VMId: {ProgressInfo.VMId}");
                                return false;
                            }
                        }
                    }
                    else
                    {
                        _FailureResponse = BWebResponse.Conflict($"Failed to get vm entry, value is empty.");
                        _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateVMEntryRecord-> Failed to get vm entry, value is empty. ProgressInfo.VMId: {ProgressInfo.VMId}");
                        return false;
                    }
                }
                else
                {
                    _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateVMEntryRecord-> Failed to get vm entry, database get item returned false. ProgressInfo.VMId: {ProgressInfo.VMId}");
                }
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"NotifyProcessRequest: UpdateVMEntryRecord-> Error occurred. Message: {ex.Message} -  StackTrace: {ex.StackTrace}");
                _FailureResponse = BWebResponse.InternalError($"Failed to update file conversion entry.");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), ProgressInfo.VMId.ToString(), _ErrorMessageAction);
            }
            return true;
        }
    }
}
