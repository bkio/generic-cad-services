using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities.All;
using ServiceUtilities_All.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace CADProcessService.Endpoints
{
    public class NotifyCompleteRequest : BppWebServiceBase
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

        protected override BWebServiceResponse OnRequestPP(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
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
                        HistoryEntry.ProcessInfo = ProgressInfo.Info;
                    }
                    else
                    {
                        ProcessHistoryDBEntry NewEntry = new ProcessHistoryDBEntry
                        {
                            HistoryRecordDate = DateTime.Now.ToString(),
                            CurrentProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage,
                            ModelName = ProgressInfo.ProgressDetails.ModelName,
                            RevisionIndex = ProgressInfo.ProgressDetails.ModelRevision,
                            ProcessInfo = ProgressInfo.Info,
                            ProcessStatus = ProgressInfo.ProcessStatus
                        };

                        DatabaseService.UpdateItem(
                            FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                            FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                            new BPrimitiveType(ProgressInfo.ProcessId.ToString()),
                            JObject.Parse(JsonConvert.SerializeObject(NewEntry)),
                            out JObject _ExistingObject, EBReturnItemBehaviour.DoNotReturn,
                            null,
                            _ErrorMessageAction);
                    }

                    if (DatabaseService.GetItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(ProgressInfo.VMId),
                    WorkerVMListDBEntry.Properties,
                    out JObject _VMEntry,
                    _ErrorMessageAction))
                    {
                        WorkerVMListDBEntry Entry = _VMEntry.ToObject<WorkerVMListDBEntry>();
                        if (ProgressInfo.ProgressDetails.GlobalCurrentStage != Entry.CurrentProcessStage)
                        {
                            Entry.CurrentProcessStage = ProgressInfo.ProgressDetails.GlobalCurrentStage;
                            Entry.StageProcessStartDates.Add(DateTime.Now.ToString());
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
                }
            }

            return BWebResponse.StatusOK("Success");
        }
    }
}
