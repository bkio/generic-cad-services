/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using CADProcessService.K8S;
using ServiceUtilities;
using ServiceUtilities.Common;
using CADProcessService.Endpoints.Controllers;

namespace CADProcessService.Endpoints
{
    internal class StopProcessRequest : WebServiceBaseTimeoutable
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private readonly IBVMServiceInterface VirtualMachineService;

        private readonly Dictionary<string, string> VirtualMachineDictionary;

        public StopProcessRequest(IBDatabaseServiceInterface _DatabaseService, IBVMServiceInterface _VirtualMachineService, Dictionary<string, string> _VirtualMachineDictionary) : base()
        {
            DatabaseService = _DatabaseService;
            VirtualMachineService = _VirtualMachineService;
            VirtualMachineDictionary = _VirtualMachineDictionary;
        }

        public override BWebServiceResponse OnRequest_Interruptable(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Interruptable_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Interruptable_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            if (_Context.Request.HttpMethod != "POST")
            {
                _ErrorMessageAction?.Invoke("StopProcessRequest: POST methods is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST methods is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            string BucketName = null;
            string RelativeFileUrl = null;
            string ConversionID_FromRelativeUrl_UrlEncoded = null;
            int ProcessMode = (int)EProcessMode.Undefined;
            string RequestedVirtualMachineId = null;

            using (var InputStream = _Context.Request.InputStream)
            {
                var NewObjectJson = new JObject();

                using (var ResponseReader = new StreamReader(InputStream))
                {
                    try
                    {
                        var ParsedBody = JObject.Parse(ResponseReader.ReadToEnd());

                        if (!ParsedBody.ContainsKey("processMode"))
                        {
                            return BWebResponse.BadRequest("Request body must contain all necessary fields.");
                        }
                        var ProcessModeToken = ParsedBody["processMode"];
                        if (ProcessModeToken.Type != JTokenType.Integer)
                        {
                            return BWebResponse.BadRequest("Request body contains invalid fields.");
                        }
                        ProcessMode = (int)ProcessModeToken;

                        if (ProcessMode == (int)EProcessMode.Kubernetes)
                        {
                            if (!ParsedBody.ContainsKey("bucketName")
                                || !ParsedBody.ContainsKey("rawFileRelativeUrl"))
                            {
                                return BWebResponse.BadRequest("Request body must contain all necessary fields. If the process mode is selected Kubernetes, request body has to have rawFileRelativeUrl and bucketName fields.");
                            }

                            var BucketNameToken = ParsedBody["bucketName"];
                            var RawFileRelativeUrlToken = ParsedBody["rawFileRelativeUrl"];
                            if (BucketNameToken.Type != JTokenType.String
                                || RawFileRelativeUrlToken.Type != JTokenType.String)
                            {
                                return BWebResponse.BadRequest("Request body contains invalid fields.");
                            }
                        
                            BucketName = (string)BucketNameToken;
                            RelativeFileUrl = (string)RawFileRelativeUrlToken;
                            ConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode((string)RawFileRelativeUrlToken);
                        }

                        if (ProcessMode == (int)EProcessMode.VirtualMachine)
                        { 
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
                    }
                    catch (Exception e)
                    {
                        _ErrorMessageAction?.Invoke("Read request body stage has failed. Exception: " + e.Message + ", Trace: " + e.StackTrace);
                        return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                    }
                }
            }

            if (ProcessMode == (int)EProcessMode.Kubernetes)
            {
                if (!ProcessKubernetes(ConversionID_FromRelativeUrl_UrlEncoded, BucketName, RelativeFileUrl, _ErrorMessageAction, out BWebServiceResponse FailureResponse))
                {
                    return FailureResponse;
                }
            }
            else if (ProcessMode == (int)EProcessMode.VirtualMachine)
            {
                if (!ProcessVirtualMachine(_Context, RequestedVirtualMachineId, _ErrorMessageAction, out BWebServiceResponse FailureResponse))
                {
                    return FailureResponse;
                }
            }
            else
            {
                return BWebResponse.BadRequest("Undefined process mode selected. Please check the process mode that you select.");
            }

            return BWebResponse.StatusAccepted("Request has been accepted; process is now being stopped.");
        }

        private bool ProcessKubernetes(
            string ConversionID_FromRelativeUrl_UrlEncoded,
            string BucketName,
            string RelativeFileUrl,
            Action<string> _ErrorMessageAction,
            out BWebServiceResponse _FailureResponse)
        {
            _FailureResponse = BWebResponse.InternalError("");

            //Temporarily, TODO: Change this when the implementation is in place.
            if (!DatabaseService.DeleteItem(
                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                new BPrimitiveType(ConversionID_FromRelativeUrl_UrlEncoded),
                out JObject _, EBReturnItemBehaviour.DoNotReturn,
                _ErrorMessageAction))
            {
                if (!DatabaseService.GetItem(
                    FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                    FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                    new BPrimitiveType(ConversionID_FromRelativeUrl_UrlEncoded),
                    FileConversionDBEntry.Properties,
                    out JObject _ReturnObject,
                    _ErrorMessageAction)
                    || _ReturnObject != null)
                {
                    _FailureResponse = BWebResponse.InternalError("Database error.");
                    return false;
                }
            }

            if (!BatchProcessingCreationService.Instance.StopBatchProcess(BucketName, RelativeFileUrl, _ErrorMessageAction))
            {
                _FailureResponse = BWebResponse.InternalError("Failed to stop pod or connect to kubernetess");
                return false;
            }

            return true;
        }

        private bool ProcessVirtualMachine(
            HttpListenerContext _Context,
            string _RequestedVirtualMachineId,
            Action<string> _ErrorMessageAction,
            out BWebServiceResponse FailureResponse)
        {
            FailureResponse = BWebResponse.InternalError("");

            if (!DatabaseService.GetItem(
                WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                new BPrimitiveType(_RequestedVirtualMachineId),
                WorkerVMListDBEntry.Properties,
                out JObject _ReturnObject, _ErrorMessageAction
                ) || _ReturnObject == null)
            {
                FailureResponse = BWebResponse.InternalError("Database error.");
                return false;
            }

            WorkerVMListDBEntry VirtualMachineEntry = _ReturnObject.ToObject<WorkerVMListDBEntry>();

            if (VirtualMachineEntry != null)
            {
                string VirtualMachineName = VirtualMachineDictionary[_RequestedVirtualMachineId];

                if (!VirtualMachineService.StopInstances(new string[] { VirtualMachineName },
                    () =>
                    {
                        if (!UpdateDBVirtualMachineAvailability(_RequestedVirtualMachineId, _ErrorMessageAction, VirtualMachineName, VirtualMachineEntry))
                        {
                            _ErrorMessageAction?.Invoke($"Failed to update worker-vm-list database for virtual machine [{VirtualMachineName}]");
                        }

                        if (!UpdateDBProcessHistory(_Context, VirtualMachineEntry, _ErrorMessageAction))
                        {
                            _ErrorMessageAction?.Invoke($"Failed to update process-history database for virtual machine [{VirtualMachineName}]");
                        }

                        Controller_BatchProcess.Get().BroadcastBatchProcessAction(new Action_BatchProcessFailed()
                        {
                            ModelName = VirtualMachineEntry.ModelName,
                            RevisionIndex = VirtualMachineEntry.RevisionIndex
                        },_ErrorMessageAction);
                    },
                    () =>
                    {
                        _ErrorMessageAction?.Invoke($"Failed to stop virtual machine [{VirtualMachineName}]");
                    }, _ErrorMessageAction))
                {
                    FailureResponse = BWebResponse.InternalError($"Failed to stop virtual machine [{VirtualMachineName}]");
                    return false;
                }
            }
            else
            {
                FailureResponse = BWebResponse.InternalError("Database object is empty.");
                return false;
            }

            return true;
        }

        private bool UpdateDBVirtualMachineAvailability(
            string _RequestedVirtualMachineId,
            Action<string> _ErrorMessageAction,
            string VirtualMachineName,
            WorkerVMListDBEntry _VirtualMachineEntry)
        {
            try
            {
                if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), _RequestedVirtualMachineId, _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"Failed to update db for [{VirtualMachineName}] because atomic db operation has been failed.");
                    return false;
                }
                _VirtualMachineEntry.VMStatus = (int)EVMStatus.Available;

                if (!DatabaseService.UpdateItem(
                WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                new BPrimitiveType(_RequestedVirtualMachineId),
                JObject.Parse(JsonConvert.SerializeObject(_VirtualMachineEntry)),
                out JObject _, EBReturnItemBehaviour.DoNotReturn,
                DatabaseService.BuildAttributeNotExistCondition(WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID),
                _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke($"Failed to update worker-vm-list table for [{VirtualMachineName}]");
                    return false;
                }
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to update worker-vm-list table for [{VirtualMachineName}]. Error: {ex.Message}. Trace: {ex.StackTrace}");
                return false;
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), _RequestedVirtualMachineId, _ErrorMessageAction);
            }
            return true;
        }

        private bool UpdateDBProcessHistory(
            HttpListenerContext _Context,
            WorkerVMListDBEntry _VirtualMachineEntry,
            Action<string> _ErrorMessageAction)
        {
            try
            {
                if (!Methods.GenerateNonExistentUniqueID(this, DatabaseService, 
                    ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                    ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                    ProcessHistoryDBEntry.MustHaveProperties,
                    EGetClearance.Yes,
                    out string ProcessID,
                    out BWebServiceResponse _,
                    _ErrorMessageAction))
                {
                    _ErrorMessageAction?.Invoke("Failed to generate non-existent unique id for process-history table.");
                    return false;
                }

                var NewProcessHistoryObject = new JObject() { 
                    [ProcessHistoryDBEntry.MODEL_UNIQUE_NAME_PROPERTY] = _VirtualMachineEntry.ModelName,
                    [ProcessHistoryDBEntry.MODEL_REVISION_INDEX_PROPERTY] = _VirtualMachineEntry.RevisionIndex,
                    [ProcessHistoryDBEntry.CURRENT_PROCESS_STAGE_PROPERTY] = _VirtualMachineEntry.CurrentProcessStage,
                    [ProcessHistoryDBEntry.HISTORY_RECORD_DATE_PROPERTY] = Methods.GetNowAsLongDateAndTimeString(),
                    [ProcessHistoryDBEntry.PROCESS_STATUS_PROPERTY] = (int)EProcessStatus.Canceled,
                    [ProcessHistoryDBEntry.PROCESS_INFO_PROPERTY] = "Stop process has been called."
                };
                Controller_DeliveryEnsurer.Get().DB_PutItem_FireAndForget(
                    _Context,
                    ProcessHistoryDBEntry.DBSERVICE_PROCESS_HISTORY_TABLE(),
                    ProcessHistoryDBEntry.KEY_NAME_PROCESS_ID,
                    new BPrimitiveType(ProcessID),
                    JObject.Parse(JsonConvert.SerializeObject(NewProcessHistoryObject)));
            }
            catch (System.Exception ex)
            {
                _ErrorMessageAction?.Invoke($"Failed to add process-history record. Error: {ex.Message}. Trace: {ex.StackTrace}");
                return false;
            }
            return true;
        }
    }
}