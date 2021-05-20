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
using CADProcessService.Endpoints.Common;

namespace CADProcessService.Endpoints
{
    internal class StopProcessRequest : WebServiceBaseTimeoutable
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        private readonly IBVMServiceInterface VirtualMachineService;

        public StopProcessRequest(IBDatabaseServiceInterface _DatabaseService, IBVMServiceInterface _VirtualMachineService) : base()
        {
            DatabaseService = _DatabaseService;
            VirtualMachineService = _VirtualMachineService;
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
                if (!CommonMethods.StopVirtualMachine(
                    VirtualMachineService, 
                    DatabaseService, 
                    InnerProcessor, 
                    VirtualMachineEntry, 
                    _RequestedVirtualMachineId, 
                    _ErrorMessageAction,
                    out BWebServiceResponse _FailureResponse))
                {
                    FailureResponse = _FailureResponse;
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
    }
}