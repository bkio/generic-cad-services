using BWebServiceUtilities;
using CADProcessService.Endpoints.Structures;
using Newtonsoft.Json;
using ServiceUtilities.All;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;

namespace CADProcessService.Endpoints
{
    public class NotifyProgressRequest : BppWebServiceBase
    {
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


                }
            }


            return BWebResponse.NotImplemented($"Endpoint not yet implemented");
        }
    }
}
