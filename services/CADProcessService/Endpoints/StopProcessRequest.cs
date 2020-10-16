/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.IO;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using CADProcessService.K8S;
using ServiceUtilities.All;
using Newtonsoft.Json.Linq;

namespace CADProcessService.Endpoints
{
    internal class StopProcessRequest : BppWebServiceBase
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBFileServiceInterface FileService;

        public StopProcessRequest(IBDatabaseServiceInterface _DatabaseService, IBFileServiceInterface _FileService) : base()
        {
            DatabaseService = _DatabaseService;
            FileService = _FileService;
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
                _ErrorMessageAction?.Invoke("StopProcessRequest: POST methods is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST methods is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            string BucketName = null;
            string RelativeFilename = null;
            string ConversionID_FromRelativeUrl_UrlEncoded = null;

            using (var InputStream = _Context.Request.InputStream)
            {
                var NewObjectJson = new JObject();

                using (var ResponseReader = new StreamReader(InputStream))
                {
                    try
                    {
                        var ParsedBody = JObject.Parse(ResponseReader.ReadToEnd());

                        if (!ParsedBody.ContainsKey("bucketName") ||
                            !ParsedBody.ContainsKey("rawFileRelativeUrl"))
                        {
                            return BWebResponse.BadRequest("Request body must contain all necessary fields.");
                        }
                        var BucketNameToken = ParsedBody["bucketName"];
                        var RawFileRelativeUrlToken = ParsedBody["rawFileRelativeUrl"];
                        if (BucketNameToken.Type != JTokenType.String ||
                            RawFileRelativeUrlToken.Type != JTokenType.String)
                        {
                            return BWebResponse.BadRequest("Request body contains invalid fields.");
                        }
                        BucketName = (string)BucketNameToken;
                        ConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode((string)RawFileRelativeUrlToken);
                        RelativeFilename = (string)RawFileRelativeUrlToken;
                    }
                    catch (Exception e)
                    {
                        _ErrorMessageAction?.Invoke("Read request body stage has failed. Exception: " + e.Message + ", Trace: " + e.StackTrace);
                        return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                    }
                }
            }

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
                    return BWebResponse.InternalError("Database error.");
                }
            }

            if(!BatchProcessingCreationService.Instance.StopBatchProcess(BucketName, RelativeFilename, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Failed to stop pod or connect to kubernetess");
            }

            return BWebResponse.StatusAccepted("Request has been accepted; process is now being stopped.");
        }
    }
}