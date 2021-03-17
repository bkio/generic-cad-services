/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using CADProcessService.K8S;
using ServiceUtilities.All;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADProcessService.Endpoints
{
    public class BatchJobCompleteRequest : BppWebServiceBase
    {
        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly IBFileServiceInterface FileService;
        private readonly IBMemoryServiceInterface MemoryService;

        public BatchJobCompleteRequest(IBDatabaseServiceInterface _DatabaseService, IBFileServiceInterface _FileService, IBMemoryServiceInterface _MemoryService) : base()
        {
            DatabaseService = _DatabaseService;
            FileService = _FileService;
            MemoryService = _MemoryService;
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
            _ErrorMessageAction?.Invoke("Job complete called");
            if (_Context.Request.HttpMethod != "GET")
            {
                return BWebResponse.BadRequest($"This service does not accept requests of type : {_Context.Request.HttpMethod}");
            }

            string Url = _Context.Request.RawUrl;
            int CopyStart = Url.LastIndexOf('/') + 1;


            string Podname = Url.Substring(CopyStart);

            BatchProcessingCreationService.Instance.GetBucketAndFile(Podname, out string _Bucket, out string _Filename);

            if (_Bucket == null || _Filename == null)
            {
                return BWebResponse.BadRequest($"The provided pod name does not exist");
            }

            string NewConversionID_FromRelativeUrl_UrlEncoded = WebUtility.UrlEncode(_Filename);

            var NewDBEntry = new FileConversionDBEntry()
            {
                ConversionStatus = (int)EInternalProcessStage.ProcessComplete
            };

            if (!DatabaseService.UpdateItem(
                FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
                FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
                new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
                JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
                out JObject _, EBReturnItemBehaviour.DoNotReturn,
                null,
                _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Database error");
            }

            if (!BatchProcessingCreationService.Instance.NotifyPodSucceded(Podname.Trim(), _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Failed to do Pod Completion or connect to kubernetes");
            }


            //Code for initial method of starting optimizer after pixyz completes
            //if (!BatchProcessingCreationService.Instance.StartUnrealOptimizer(_Bucket, _Filename))
            //{
            //    NewDBEntry.ConversionStatus = (int)EInternalProcessStage.ProcessFailed;

            //    if (!DatabaseService.UpdateItem(
            //        FileConversionDBEntry.DBSERVICE_FILE_CONVERSIONS_TABLE(),
            //        FileConversionDBEntry.KEY_NAME_CONVERSION_ID,
            //        new BPrimitiveType(NewConversionID_FromRelativeUrl_UrlEncoded),
            //        JObject.Parse(JsonConvert.SerializeObject(NewDBEntry)),
            //        out JObject _, EBReturnItemBehaviour.DoNotReturn,
            //        null,
            //        _ErrorMessageAction))
            //    {
            //        return BWebResponse.InternalError("Failed to start unreal optimizer and experienced a Database error");
            //    }

            //    return BWebResponse.InternalError("Failed to start unreal optimizer");
            //}


            return BWebResponse.StatusOK("Job completion confirmed. Unreal Optimizer scheduled.");
        }
    }
}
