using BCloudServiceUtilities;
using BWebServiceUtilities;
using ServiceUtilities.All;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;

namespace CADProcessService.Endpoints
{
    public class GetStageSignedUploadUrl : BppWebServiceBase
    {
        private const string UPLOAD_CONTENT_TYPE = "application/octet-stream";
        private const int UPLOAD_URL_VALIDITY_MINUTES = 1440;

        private readonly IBFileServiceInterface FileService;
        private readonly string CadFileStorageBucketName;

        public GetStageSignedUploadUrl(
            IBFileServiceInterface _FileService,
            string _CadFileStorageBucketName) : base()
        {
            FileService = _FileService;
            CadFileStorageBucketName = _CadFileStorageBucketName;
        }

        protected override BWebServiceResponse OnRequestPP(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            throw new NotImplementedException();
        }

        private void OnRequestPP_Internal(Action<string> _ErrorMessageAction = null)
        {
            int Stage = 0;
            int ModelRevision = 0;
            string ModelId = "";

            FileService.CreateSignedURLForUpload(out string UploadUrl, CadFileStorageBucketName, $"raw/{ModelId}/{ModelRevision}/{Stage}/File.zip", UPLOAD_CONTENT_TYPE, UPLOAD_URL_VALIDITY_MINUTES, _ErrorMessageAction);
        }
    }
}
