/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADProcessService.K8S;
using ServiceUtilities.All;
using ServiceUtilities.Process.Procedure;
using Newtonsoft.Json.Linq;

namespace CADProcessService.Endpoints
{
    public class GetSignedUploadUrlRequest : BppWebServiceBase
    {
        private const string UPLOAD_CONTENT_TYPE = "application/octet-stream";
        private const int UPLOAD_URL_VALIDITY_MINUTES = 1440;

        private readonly IBFileServiceInterface FileService;
        private readonly string CadFileStorageBucketName;

        public GetSignedUploadUrlRequest(
            IBFileServiceInterface _FileService, 
            string _CadFileStorageBucketName) : base()
        {
            FileService = _FileService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
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
            if (_Context.Request.HttpMethod != "GET")
            {
                return BWebResponse.BadRequest($"This service does not accept requests of type : {_Context.Request.HttpMethod}");
            }

            string FileTypeStr = "";
            string Filename = "file";

            try
            {
                FileTypeStr = _Context.Request.QueryString.Get("fileType").ToLower().Trim().TrimStart('.');
            }
            catch
            {
                return BWebResponse.BadRequest($"Expected parameters have not been provided");
            }

            EProcessedFileType FileTypeEnum = EProcessedFileType.NONE_OR_RAW;

            if (Constants.ProcessedFileType_Enum_Map.ContainsKey(FileTypeStr))
            {
                FileTypeEnum = Constants.ProcessedFileType_Enum_Map[FileTypeStr];
            }
            else
            {
                return BWebResponse.BadRequest($"Invalid File Type was provided");
            }

            try
            {
                Filename = _Context.Request.QueryString.Get("fileName");
            }
            catch
            {
                //This is an optional field so do nothing
            }

            if (string.IsNullOrWhiteSpace(Filename))
            {
                Filename = "file";
            }

            string Url = _Context.Request.RawUrl;
            int CopyStart = Url.LastIndexOf('/') + 1;
            int CopyEnd = Url.IndexOf("?");
            string Podname = Url.Substring(CopyStart, CopyEnd - CopyStart).TrimEnd('/');

            if (CopyEnd == -1)
            {
                return BWebResponse.BadRequest($"Expected parameters have not been provided");
            }

            BatchProcessingCreationService.Instance.GetBucketAndFile(Podname, out string _Bucket, out string _Filename);

            if (!string.IsNullOrWhiteSpace(_Bucket) && !string.IsNullOrWhiteSpace(_Filename))
            {
                string RawStrippedPath = _Filename.TrimStart("raw/");
                int FilenameStripLength = RawStrippedPath.LastIndexOf('/');
                RawStrippedPath = RawStrippedPath.Substring(0, FilenameStripLength);

                if (!FileService.CreateSignedURLForUpload(out string SignedUploadUrl, CadFileStorageBucketName, $"{Constants.ProcessedFileType_FolderPrefix_Map[FileTypeEnum]}{RawStrippedPath}/{Filename}.{Constants.ProcessedFileType_Extension_Map[FileTypeEnum]}", UPLOAD_CONTENT_TYPE, UPLOAD_URL_VALIDITY_MINUTES, _ErrorMessageAction))
                {
                    return BWebResponse.InternalError("Failed to create Upload Url");
                }

                return BWebResponse.StatusOK("success", new JObject() { ["uploadUrl"] = SignedUploadUrl });
            }
            else
            {
                return BWebResponse.NotFound("Could not find pod details");
            }
        }
    }
}
