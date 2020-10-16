/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Endpoints.Structures;
using CADFileService.Endpoints.Common;
using ServiceUtilities;
using Newtonsoft.Json.Linq;

namespace CADFileService.Endpoints
{
    partial class InternalCalls
    {
        internal class CheckModelsExist : InternalWebServiceBaseTimeoutable
        {
            private readonly IBDatabaseServiceInterface DatabaseService;

            public CheckModelsExist(string _InternalCallPrivateKey, IBDatabaseServiceInterface _DatabaseService) : base(_InternalCallPrivateKey)
            {
                DatabaseService = _DatabaseService;
            }

            public override BWebServiceResponse OnRequest_Interruptable(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
            {
                if (_Context.Request.HttpMethod != "POST")
                {
                    _ErrorMessageAction?.Invoke("CheckModelsExist: POST method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                    return BWebResponse.MethodNotAllowed("POST method is accepted. But received request method: " + _Context.Request.HttpMethod);
                }

                string RequestPayload = null;
                JObject ParsedBody;
                try
                {
                    using (var InputStream = _Context.Request.InputStream)
                    {
                        using var ResponseReader = new StreamReader(InputStream);
                    
                        RequestPayload = ResponseReader.ReadToEnd();
                        ParsedBody = JObject.Parse(RequestPayload);
                    }
                }
                catch (Exception e)
                {
                    _ErrorMessageAction?.Invoke("CheckModelsExist-> Malformed request body. Body content: " + RequestPayload + ", Exception: " + e.Message + ", Trace: " + e.StackTrace);
                    return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                }

                //get UserModels from parsed request body
                var UserModelIDs = new List<string>();
                if (ParsedBody["userModelIds"].Type != JTokenType.Array)
                {
                    return BWebResponse.BadRequest("Request is invalid.");
                }
                var UserModelsJArray = (JArray)ParsedBody["userModelIds"];

                foreach (var CurrentUserModelID in UserModelsJArray)
                {
                    if (CurrentUserModelID.Type != JTokenType.String)
                    {
                        return BWebResponse.BadRequest("Request is invalid.");
                    }

                    var UserModelID = (string)CurrentUserModelID;
                    if (!UserModelIDs.Contains(UserModelID))
                    {
                        UserModelIDs.Add(UserModelID);
                    }
                }

                //get UserSharedModels from parsed request body
                var UserSharedModelIDs = new List<string>();
                if (ParsedBody["userSharedModelIds"].Type != JTokenType.Array)
                {
                    return BWebResponse.BadRequest("Request is invalid.");
                }
                var UserSharedModelsJArray = (JArray)ParsedBody["userSharedModelIds"];

                foreach (var CurrentUserSharedModelID in UserSharedModelsJArray)
                {
                    if (CurrentUserSharedModelID.Type != JTokenType.String)
                    {
                        return BWebResponse.BadRequest("Request is invalid.");
                    }

                    var UserSharedModelID = (string)CurrentUserSharedModelID;
                    if (!UserSharedModelIDs.Contains(UserSharedModelID))
                    {
                        UserSharedModelIDs.Add(UserSharedModelID);
                    }
                }

                var CheckedUserModelIDs = new JArray();
                foreach (var ModelID in UserModelIDs)
                {
                    var ModelKey = new BPrimitiveType(ModelID);

                    if (!CommonMethods.TryGettingModelInfo(
                        DatabaseService,
                        ModelID,
                        out JObject _,
                        true, out ModelDBEntry ModelData,
                        out BWebServiceResponse _FailedResponse,
                        _ErrorMessageAction))
                    {
                        if (_FailedResponse.StatusCode >= 400 && _FailedResponse.StatusCode < 500)
                        {
                            continue;
                        }
                        else if (_FailedResponse.StatusCode >= 500)
                        {
                            return BWebResponse.InternalError("Getting user model info operation has been failed.");
                        }
                    }

                    if (!CheckedUserModelIDs.Contains(ModelID))
                    {
                        CheckedUserModelIDs.Add(ModelID);
                    }
                }

                var CheckedUserSharedModelIDs = new JArray();
                foreach (var SharedModelID in UserSharedModelIDs)
                {
                    var ModelKey = new BPrimitiveType(SharedModelID);

                    if (!CommonMethods.TryGettingModelInfo(
                        DatabaseService,
                        SharedModelID,
                        out JObject _,
                        true, out ModelDBEntry ModelData,
                        out BWebServiceResponse _FailedResponse,
                        _ErrorMessageAction))
                    {
                        if (_FailedResponse.StatusCode >= 400 && _FailedResponse.StatusCode < 500)
                        {
                            continue;
                        }
                        else if (_FailedResponse.StatusCode >= 500)
                        {
                            return BWebResponse.InternalError("Getting user shared model info operation has been failed.");
                        }
                    }

                    if (!CheckedUserSharedModelIDs.Contains(SharedModelID))
                    {
                        CheckedUserSharedModelIDs.Add(SharedModelID);
                    }
                }

                return BWebResponse.StatusOK("Check models have successfully been completed.", new JObject()
                {
                    ["checkedUserModelIds"] = CheckedUserModelIDs,
                    ["checkedUserSharedModelIds"] = CheckedUserSharedModelIDs
                });
            }
        }
    }
}