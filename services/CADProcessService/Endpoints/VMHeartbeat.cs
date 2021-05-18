/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceUtilities;
using ServiceUtilities.Common;

namespace CADProcessService.Endpoints
{
    partial class InternalCalls
    {
        internal class VMHeartbeat : InternalWebServiceBaseTimeoutable
        {
            private readonly IBDatabaseServiceInterface DatabaseService;

            public VMHeartbeat(IBDatabaseServiceInterface _DatabaseService, string _InternalCallPrivateKey) : base(_InternalCallPrivateKey)
            {
                DatabaseService = _DatabaseService;
            }

            public override BWebServiceResponse OnRequest_Interruptable(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
            {
                if (_Context.Request.HttpMethod != "POST")
                {
                    _ErrorMessageAction?.Invoke("VMHeartbeat: POST method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                    return BWebResponse.MethodNotAllowed("POST method is accepted. But received request method: " + _Context.Request.HttpMethod);
                }

                string RequestedVirtualMachineId = null;

                using (var InputStream = _Context.Request.InputStream)
                {
                    var NewObjectJson = new JObject();

                    using (var ResponseReader = new StreamReader(InputStream))
                    {
                        try
                        {
                            var ParsedBody = JObject.Parse(ResponseReader.ReadToEnd());

                            if (!ParsedBody.ContainsKey("virtualMachineId"))
                            {
                                return BWebResponse.BadRequest("Request body must contain all necessary fields. If the process mode is selected VirtualMachine, request body has to have virtualMachineId field.");
                            }

                            var RequestedVirtualMachineIdToken = ParsedBody["virtualMachineId"];
                            if (RequestedVirtualMachineIdToken.Type != JTokenType.String)
                            {
                                return BWebResponse.BadRequest("Request body contains invalid fields.");
                            }

                            RequestedVirtualMachineId = (string)RequestedVirtualMachineIdToken;
                        }
                        catch (Exception e)
                        {
                            _ErrorMessageAction?.Invoke("Read request body stage has failed. Exception: " + e.Message + ", Trace: " + e.StackTrace);
                            return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                        }
                    }
                }

                return ProcessHeartbeat(RequestedVirtualMachineId, _ErrorMessageAction);
            }

            private BWebServiceResponse ProcessHeartbeat(string _RequestedVirtualMachineId, Action<string> _ErrorMessageAction = null)
            {
                if (!DatabaseService.GetItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(_RequestedVirtualMachineId),
                    WorkerVMListDBEntry.Properties,
                    out JObject _ReturnObject, _ErrorMessageAction
                    ) || _ReturnObject == null)
                {
                    return BWebResponse.InternalError("Database error.");
                }

                WorkerVMListDBEntry VirtualMachineEntry = _ReturnObject.ToObject<WorkerVMListDBEntry>();

                if (VirtualMachineEntry != null)
                {
                    if (!DatabaseService.ScanTable(ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(), out List<JObject> ProcessHistoriesJO, _ErrorMessageAction))
                    {
                        return BWebResponse.InternalError("Scan-table operation has failed.");
                    }

                    string _LastHistoryObjectString = null;
                    DateTime _LastHistoryDate = DateTime.MinValue;
                    foreach (var CurrentHistory in ProcessHistoriesJO)
                    {
                        var History = JsonConvert.DeserializeObject<ProcessHistoryDBEntry>(CurrentHistory.ToString());
                        if (History.ModelName.Equals(VirtualMachineEntry.ModelName) && History.RevisionIndex == VirtualMachineEntry.RevisionIndex)
                        {
                            if (Methods.TryParseDateTimeFromUtcNowString(History.HistoryRecordDate, out DateTime ParsedHistoryDate))
                            {
                                if (ParsedHistoryDate.Ticks > _LastHistoryDate.Ticks)
                                {
                                    _LastHistoryDate = new DateTime(ParsedHistoryDate.Ticks);
                                    _LastHistoryObjectString = CurrentHistory.ToString();
                                }
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(_LastHistoryObjectString))
                    {
                        var LastHistory = JsonConvert.DeserializeObject<ProcessHistoryDBEntry>(_LastHistoryObjectString);
                        if (LastHistory.ProcessStatus == (int)EProcessStatus.Canceled
                            || LastHistory.ProcessStatus == (int)EProcessStatus.Failed)
                        {
                            return BWebResponse.ServiceUnavailable("The current process is canceled or failed.");
                        }
                    }
                    else
                    {
                        return BWebResponse.NotFound($"The current process is not found with RequestedVirtualMachineId:{_RequestedVirtualMachineId}.");
                    }
                }
                else
                {
                    return BWebResponse.NotFound($"The current process is not found with RequestedVirtualMachineId:{_RequestedVirtualMachineId}.");
                }

                return BWebResponse.StatusOK("OK");
            }
        }
    }
}