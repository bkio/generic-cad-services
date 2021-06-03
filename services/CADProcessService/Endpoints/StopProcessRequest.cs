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
using CADProcessService.Endpoints.Common;
using System.Linq;

namespace CADProcessService.Endpoints
{
    internal class StopProcessRequest : WebServiceBaseTimeoutable
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private readonly IBVMServiceInterface VirtualMachineService;

        private readonly Dictionary<string, string> VirtualMachineDictionary;

        private string RequestedVirtualMachineId = null;
        private string RequestedModelUniqueName = null;
        private int RequestedRevisionIndex = -1;

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

            using (var InputStream = _Context.Request.InputStream)
            {
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
                                || !ParsedBody.ContainsKey("fileRelativeUrl"))
                            {
                                return BWebResponse.BadRequest("Request body must contain all necessary fields. If the process mode is selected Kubernetes, request body has to have fileRelativeUrl and bucketName fields.");
                            }

                            var BucketNameToken = ParsedBody["bucketName"];
                            var FileRelativeUrlToken = ParsedBody["fileRelativeUrl"];
                            if (BucketNameToken.Type != JTokenType.String
                                || FileRelativeUrlToken.Type != JTokenType.String)
                            {
                                return BWebResponse.BadRequest("Request body contains invalid fields.");
                            }
                        
                            BucketName = (string)BucketNameToken;
                            RelativeFileUrl = (string)FileRelativeUrlToken;
                            ConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode((string)FileRelativeUrlToken);
                        }

                        if (ProcessMode == (int)EProcessMode.VirtualMachine)
                        {
                            if (!ParsedBody.ContainsKey("virtualMachineId") 
                                && !(ParsedBody.ContainsKey("modelUniqueName") && ParsedBody.ContainsKey("revisionIndex")))
                            {
                                return BWebResponse.BadRequest("Request body must contain all necessary fields. If the process mode is selected VirtualMachine, request body has to have virtualMachineId field or (modelUniqueName and revisionIndex fields).");
                            }

                            if (ParsedBody.ContainsKey("virtualMachineId"))
                            {
                                var RequestedVirtualMachineIdToken = ParsedBody["virtualMachineId"];
                                if (RequestedVirtualMachineIdToken.Type != JTokenType.String)
                                {
                                    return BWebResponse.BadRequest("Request body contains invalid fields.");
                                }

                                RequestedVirtualMachineId = (string)RequestedVirtualMachineIdToken;
                            }
                            if (ParsedBody.ContainsKey("modelUniqueName"))
                            {
                                var RequestedModelUniqueNameToken = ParsedBody["modelUniqueName"];
                                if (RequestedModelUniqueNameToken.Type != JTokenType.String)
                                {
                                    return BWebResponse.BadRequest("Request body contains invalid fields.");
                                }

                                RequestedModelUniqueName = (string)RequestedModelUniqueNameToken;
                            }
                            if (ParsedBody.ContainsKey("revisionIndex"))
                            {
                                var RequestedRevisionIndexToken = ParsedBody["revisionIndex"];
                                if (RequestedRevisionIndexToken.Type != JTokenType.Integer)
                                {
                                    return BWebResponse.BadRequest("Request body contains invalid fields.");
                                }

                                RequestedRevisionIndex = (int)RequestedRevisionIndexToken;
                            }
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
                if (!ProcessVirtualMachine(_ErrorMessageAction, out BWebServiceResponse FailureResponse))
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
                _FailureResponse = BWebResponse.InternalError("Failed to stop pod or connect to kubernetes");
                return false;
            }

            return true;
        }

        private bool ProcessVirtualMachine(
            Action<string> _ErrorMessageAction,
            out BWebServiceResponse FailureResponse)
        {
            FailureResponse = BWebResponse.InternalError("");

            if (RequestedVirtualMachineId == null)
            {
                if (!StopAllVirtualMachines(_ErrorMessageAction, out FailureResponse))
                {
                    return false;
                }
            }
            else
            {
                if (!StopSingleVirtualMachineById(_ErrorMessageAction, out FailureResponse))
                {
                    return false;
                }
            }

            return true;
        }

        private bool StopSingleVirtualMachineById(
            Action<string> _ErrorMessageAction,
            out BWebServiceResponse FailureResponse)
        {
            FailureResponse = BWebResponse.InternalError("");

            if (!DatabaseService.GetItem(
                    WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(),
                    WorkerVMListDBEntry.KEY_NAME_VM_UNIQUE_ID,
                    new BPrimitiveType(RequestedVirtualMachineId),
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
                if (!CommonMethods.StopVirtualMachine(
                    VirtualMachineService,
                    DatabaseService,
                    InnerProcessor,
                    VirtualMachineEntry,
                    RequestedVirtualMachineId,
                    _ErrorMessageAction,
                    out BWebServiceResponse _FailureResponse))
                {
                    FailureResponse = _FailureResponse;
                    return false;
                }
            }
            else
            {
                FailureResponse = BWebResponse.InternalError("Database object is empty or given virtualMachineId is wrong!");
                return false;
            }

            return true;
        }

        private bool StopAllVirtualMachines(
            Action<string> _ErrorMessageAction,
            out BWebServiceResponse FailureResponse)
        {
            FailureResponse = BWebResponse.InternalError("");
            if (!DatabaseService.ScanTable(WorkerVMListDBEntry.DBSERVICE_WORKERS_VM_LIST_TABLE(), out List<JObject> WorkerVMJList, _ErrorMessageAction))
            {
                _ErrorMessageAction?.Invoke("Scan-table operation has failed.");
                FailureResponse = BWebResponse.InternalError("Scan-table operation has failed.");
                return false;
            }

            if (WorkerVMJList.Count == 0)
            {
                _ErrorMessageAction?.Invoke("There is no virtual machine in db.");
                FailureResponse = BWebResponse.InternalError("There is no virtual machine in db.");
                return false;
            }

            var foundAtLeastOneRecord = false;
            foreach (var CurrentWorkerVMJObject in WorkerVMJList)
            {
                var CurrentWorkerVM = JsonConvert.DeserializeObject<WorkerVMListDBEntry>(CurrentWorkerVMJObject.ToString());
                var _RequestedVirtualMachineId = VirtualMachineDictionary.FirstOrDefault(x => x.Value.Equals(CurrentWorkerVM.VMName)).Key;

                if (CurrentWorkerVM.VMStatus == (int)EVMStatus.Available)
                {
                    continue;
                }

                if (!(CurrentWorkerVM.ModelName.Equals(RequestedModelUniqueName) && CurrentWorkerVM.RevisionIndex == RequestedRevisionIndex))
                {
                    continue;
                }

                foundAtLeastOneRecord = true;

                if (CurrentWorkerVM.LastKnownProcessStatus == (int)EProcessStatus.Idle
                        || CurrentWorkerVM.LastKnownProcessStatus == (int)EProcessStatus.Failed
                        || CurrentWorkerVM.LastKnownProcessStatus == (int)EProcessStatus.Completed)
                {
                    if (!CommonMethods.StopVirtualMachine(
                        VirtualMachineService,
                        DatabaseService,
                        InnerProcessor,
                        CurrentWorkerVM,
                        _RequestedVirtualMachineId,
                        _ErrorMessageAction,
                        out FailureResponse))
                    {
                        if (FailureResponse.ResponseContent.Type == EBStringOrStreamEnum.String)
                        {
                            _ErrorMessageAction?.Invoke(FailureResponse.ResponseContent.String);
                        }
                        return false;
                    }
                }
            }

            if (!foundAtLeastOneRecord)
            {
                FailureResponse = BWebResponse.InternalError("Given modelUniqueName and revisionIndex are wrong!");
                return false;
            }

            return true;
        }
    }
}