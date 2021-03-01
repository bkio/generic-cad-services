/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.K8S;
using CADProcessService.Endpoints.Structures;
using ServiceUtilities.All;
using Newtonsoft.Json.Linq;

namespace CADProcessService.Endpoints
{
    public class GetOptimizerParametersRequest: BppWebServiceBase
    {
        private readonly IBDatabaseServiceInterface DatabaseService;

        public GetOptimizerParametersRequest(IBDatabaseServiceInterface _DatabaseService) : base()
        {
            DatabaseService = _DatabaseService;
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
            _ErrorMessageAction?.Invoke("Trying to get Optimizer Env Vars");
            if (_Context.Request.HttpMethod != "GET")
            {
                _ErrorMessageAction?.Invoke("Wrong type");
                return BWebResponse.BadRequest($"This service does not accept requests of type : {_Context.Request.HttpMethod}");
            }

            string Url = _Context.Request.RawUrl;
            int CopyStart = Url.LastIndexOf('/') + 1;
            string Podname = Url.Substring(CopyStart).TrimEnd('/');

            BatchProcessingCreationService.Instance.GetBucketAndFile(Podname, out string _Bucket, out string _Filename);

            if (!string.IsNullOrWhiteSpace(_Bucket) && !string.IsNullOrWhiteSpace(_Filename))
            {
                string NewConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode(_Filename);

                bool CanProceed = false;
                bool ProcessError = false;

                if (DatabaseService.GetItem(
                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                FileConversionDBEntry.Properties,
                out JObject ConversionObject
                ))
                {
                    if (ConversionObject != null && ConversionObject.ContainsKey("conversionStatus"))
                    {
                        EInternalProcessStage ExistingStatus = (EInternalProcessStage)(int)ConversionObject["conversionStatus"];

                        if (ExistingStatus == EInternalProcessStage.ProcessComplete)
                        {
                            CanProceed = true;
                        }
                        else if (ExistingStatus == EInternalProcessStage.ProcessFailed)
                        {
                            ProcessError = true;
                        }
                    }
                }else
                {
                    ProcessError = true;
                }

                if (CanProceed)
                {
                    Dictionary<string, string> EnvVars = BatchProcessingCreationService.Instance.GetOptimizerEnvVars(_Bucket, _Filename, Podname, _ErrorMessageAction);

                    if (EnvVars == null)
                    {
                        _ErrorMessageAction?.Invoke("EnvVars null");
                        return BWebResponse.InternalError("An error occured trying to generate optimizer parameters");
                    }

                    return BWebResponse.StatusOK("success", new JObject()
                    {
                        ["uploadRequestUrl"] = EnvVars["CAD_PROCESS_UPLOAD_REQUEST_URL"],
                        ["downloadHierarchyCfUrl"] = EnvVars["DOWNLOAD_HIERARCHY_CF"],
                        ["downloadGeometryCfUrl"] = EnvVars["DOWNLOAD_GEOMETRY_CF"],
                        ["downloadMetadataCfUrl"] = EnvVars["DOWNLOAD_METADATA_CF"]
                    });
                }
                else if(ProcessError)
                {
                    _ErrorMessageAction?.Invoke("Cad process Failed");
                    return BWebResponse.InternalError("Pixyz process has failed");
                }
                else
                {
                    _ErrorMessageAction?.Invoke("Not found");
                    return BWebResponse.NotFound("Cad process has not completed yet");
                }
            }
            else
            {
                _ErrorMessageAction?.Invoke("General failure");
                return BWebResponse.InternalError("An error occured trying to retreive pod details");
            }
        }
    }
}
