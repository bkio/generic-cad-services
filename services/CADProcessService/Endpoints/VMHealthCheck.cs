/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Net;
using BWebServiceUtilities;
using ServiceUtilities;

namespace CADProcessService.Endpoints
{
    partial class InternalCalls
    {
        internal class VMHealthCheck : InternalWebServiceBaseTimeoutable
        {
            public VMHealthCheck(string _InternalCallPrivateKey) : base(_InternalCallPrivateKey)
            {
            }

            public override BWebServiceResponse OnRequest_Interruptable(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
            {
                if (_Context.Request.HttpMethod != "GET")
                {
                    _ErrorMessageAction?.Invoke("VMHealthCheck: GET method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                    return BWebResponse.MethodNotAllowed("GET method is accepted. But received request method: " + _Context.Request.HttpMethod);
                }

                return BWebResponse.StatusOK("VM Health check has successfully been completed.");
            }
        }
    }
}
