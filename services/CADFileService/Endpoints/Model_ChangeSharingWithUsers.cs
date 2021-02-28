/// MIT License, Copyright Burak Kara, burak@burak.io, https://en.wikipedia.org/wiki/MIT_License

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using BCloudServiceUtilities;
using BCommonUtilities;
using BWebServiceUtilities;
using CADFileService.Controllers;
using CADFileService.Endpoints.Common;
using CADFileService.Endpoints.Structures;
using ServiceUtilities;
using ServiceUtilities.PubSubUsers.PubSubRelated;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace CADFileService
{
    internal class Model_ChangeSharingWithUsers : WebServiceBaseTimeoutableDeliveryEnsurerUser
    {
        private ServiceUtilities.Common.AuthorizedRequester AuthorizedUser;

        private readonly string InternalCallPrivateKey;

        private readonly string AuthServiceEndpoint;

        private string RequestedModelID;

        private readonly IBDatabaseServiceInterface DatabaseService;
        private readonly string RestfulUrlParameter_ModelsKey;

        public Model_ChangeSharingWithUsers(string _InternalCallPrivateKey, string _AuthServiceEndpoint, IBDatabaseServiceInterface _DatabaseService, string _RestfulUrlParameter_ModelsKey)
        {
            InternalCallPrivateKey = _InternalCallPrivateKey;
            AuthServiceEndpoint = _AuthServiceEndpoint;
            DatabaseService = _DatabaseService;
            RestfulUrlParameter_ModelsKey = _RestfulUrlParameter_ModelsKey;
        }

        public override BWebServiceResponse OnRequest_Interruptable_DeliveryEnsurerUser(HttpListenerContext _Context, Action<string> _ErrorMessageAction = null)
        {
            GetTracingService()?.On_FromGatewayToService_Received(_Context, _ErrorMessageAction);

            var Result = OnRequest_Internal(_Context, _ErrorMessageAction);

            GetTracingService()?.On_FromServiceToGateway_Sent(_Context, _ErrorMessageAction);

            return Result;
        }

        private BWebServiceResponse OnRequest_Internal(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            AuthorizedUser = ServiceUtilities.Common.Methods.GetAuthorizedRequester(_Context, _ErrorMessageAction);
            if (!AuthorizedUser.bParseSuccessful) return BWebResponse.InternalError(AuthorizedUser.ParseErrorMessage);

            if (_Context.Request.HttpMethod != "POST")
            {
                _ErrorMessageAction?.Invoke("Model_ChangeSharingWithUsers: POST method is accepted. But received request method:  " + _Context.Request.HttpMethod);
                return BWebResponse.MethodNotAllowed("POST method is accepted. But received request method: " + _Context.Request.HttpMethod);
            }

            if (!Controller_AtomicDBOperation.Get().GetClearanceForDBOperation(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction))
            {
                return BWebResponse.InternalError("Atomic operation control has failed.");
            }
            try
            {
                return ProcessRequestLocked(_Context, _ErrorMessageAction);
            }
            finally
            {
                Controller_AtomicDBOperation.Get().SetClearanceForDBOperationForOthers(InnerProcessor, ModelDBEntry.DBSERVICE_MODELS_TABLE(), RequestedModelID, _ErrorMessageAction);
            }
        }

        private BWebServiceResponse ProcessRequestLocked(HttpListenerContext _Context, Action<string> _ErrorMessageAction)
        {
            var RequestedModelName = RestfulUrlParameters[RestfulUrlParameter_ModelsKey];

            if (!CommonMethods.TryGettingModelID(
                DatabaseService,
                RequestedModelName,
                out RequestedModelID,
                out BWebServiceResponse FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            string RequestPayload = null;
            JObject ParsedBody;
            using (var InputStream = _Context.Request.InputStream)
            {
                using var ResponseReader = new StreamReader(InputStream);
                try
                {
                    RequestPayload = ResponseReader.ReadToEnd();
                    ParsedBody = JObject.Parse(RequestPayload);
                }
                catch (Exception e)
                {
                    _ErrorMessageAction?.Invoke("Model_ChangeSharingWithUsers-> Malformed request body. Body content: " + RequestPayload + ", Exception: " + e.Message + ", Trace: " + e.StackTrace);
                    return BWebResponse.BadRequest("Malformed request body. Request must be a valid json form.");
                }
            }

            var bShareWithAllExists = ParsedBody.ContainsKey("shareWithAll");
            var bEmailsExist = ParsedBody.ContainsKey("emails");
            var bUserIdsExist = ParsedBody.ContainsKey("userIds");

            var FinalUserIds = new List<string>();

            bool bShareWithAll = false;
            if (bShareWithAllExists/*Exist*/)
            {
                if (ParsedBody["shareWithAll"].Type != JTokenType.Boolean)
                {
                    return BWebResponse.BadRequest("Request is invalid.");
                }
                bShareWithAll = (bool)ParsedBody["shareWithAll"];

                if (bShareWithAll)
                {
                    if (bEmailsExist || bUserIdsExist)
                    {
                        return BWebResponse.BadRequest("Request has shareWithAll field; therefore cannot have emails or userIds.");
                    }

                    FinalUserIds.Add("*");
                }
            }

            if (!CommonMethods.TryGettingModelInfo(
                DatabaseService,
                RequestedModelID,
                out JObject _,
                true, out ModelDBEntry Model,
                out FailureResponse,
                _ErrorMessageAction))
            {
                return FailureResponse;
            }

            if (!bShareWithAll)
            {
                var EmailAddresses = new List<string>();

                if (bEmailsExist)
                {
                    if (ParsedBody["emails"].Type != JTokenType.Array)
                    {
                        return BWebResponse.BadRequest("Request is invalid.");
                    }
                    var AsJArray = (JArray)ParsedBody["emails"];

                    foreach (var Token in AsJArray)
                    {
                        if (Token.Type != JTokenType.String)
                        {
                            return BWebResponse.BadRequest("Request is invalid.");
                        }

                        var Lowered = ((string)Token).ToLower();
                        if (!EmailAddresses.Contains(Lowered))
                        {
                            EmailAddresses.Add(Lowered);
                        }
                    }
                }
                if (bUserIdsExist)
                {
                    if (ParsedBody["userIds"].Type != JTokenType.Array)
                    {
                        return BWebResponse.BadRequest("Request is invalid.");
                    }
                    var AsJArray = (JArray)ParsedBody["userIds"];

                    foreach (var Token in AsJArray)
                    {
                        if (Token.Type != JTokenType.String)
                        {
                            return BWebResponse.BadRequest("Request is invalid.");
                        }

                        var Lowered = ((string)Token).ToLower();
                        if (!FinalUserIds.Contains(Lowered))
                        {
                            FinalUserIds.Add(Lowered);
                        }
                    }
                }

                if (EmailAddresses.Count > 0)
                {
                    GetTracingService()?.On_FromServiceToService_Sent(_Context, _ErrorMessageAction);

                    var EmailsJArray = new JArray();

                    foreach (var CurEmail in EmailAddresses)
                        EmailsJArray.Add(CurEmail);

                    var RequestObject = new JObject()
                    {
                        ["emailAddresses"] = EmailsJArray
                    };

                    var Result = BWebServiceExtraUtilities.InterServicesRequest(new BWebServiceExtraUtilities.InterServicesRequestRequest()
                    {
                        DestinationServiceUrl = AuthServiceEndpoint + "/auth/internal/fetch_user_ids_from_emails?secret=" + InternalCallPrivateKey,
                        RequestMethod = "POST",
                        bWithAuthToken = true,
                        UseContextHeaders = _Context,
                        ContentType = "application/json",
                        Content = new BStringOrStream(RequestObject.ToString()),
                        ExcludeHeaderKeysForRequest = null
                    },
                    false,
                    _ErrorMessageAction);

                    GetTracingService()?.On_FromServiceToService_Received(_Context, _ErrorMessageAction);

                    if (!Result.bSuccess || Result.ResponseCode >= 400)
                    {
                        return new BWebServiceResponse(Result.ResponseCode, Result.Content, Result.ContentType);
                    }

                    string InterServicesRequestStringResponse = null;

                    JObject Map;
                    try
                    {
                        InterServicesRequestStringResponse = Result.Content.String;

                        var Json = JObject.Parse(InterServicesRequestStringResponse);
                        Map = (JObject)Json["map"];
                    }
                    catch (Exception e)
                    {
                        _ErrorMessageAction?.Invoke("Model_ChangeSharingWithUsers-> Malformed request body has been returned from auth service. Body content: " + InterServicesRequestStringResponse + ", Exception: " + e.Message + ", Trace: " + e.StackTrace);
                        return BWebResponse.InternalError("Malformed request body has been returned from auth service.");
                    }

                    foreach (var UserIdToken in Map.Values())
                    {
                        if (UserIdToken.Type == JTokenType.String)
                        {
                            var UserId = (string)UserIdToken;
                            if (!FinalUserIds.Contains(UserId))
                            {
                                FinalUserIds.Add(UserId);
                            }
                        }
                    }
                }
            }

            if (Model.ModelSharedWithUserIDs.OrderBy(t => t).SequenceEqual(FinalUserIds.OrderBy(t => t)))
            {
                return BWebResponse.StatusOK("No change has been done.");
            }

            var OldSharedList = new List<string>(Model.ModelSharedWithUserIDs);
            Model.ModelSharedWithUserIDs = FinalUserIds;

            Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                _Context,
                ModelDBEntry.DBSERVICE_MODELS_TABLE(),
                ModelDBEntry.KEY_NAME_MODEL_ID,
                new BPrimitiveType(RequestedModelID),
                JObject.Parse(JsonConvert.SerializeObject(Model)));

            var bOldHasStar = OldSharedList.Contains("*");
            var bNewHasStar = FinalUserIds.Contains("*");

            if (bOldHasStar && !bNewHasStar)
            {
                Controller_DeliveryEnsurer.Get().DB_DeleteItem_FireAndForget(
                    _Context,
                    GloballySharedModelIDsDBEntry.DBSERVICE_GLOBALLY_SHARED_MODEL_IDS_TABLE(),
                    GloballySharedModelIDsDBEntry.KEY_NAME_MODEL_ID,
                    new BPrimitiveType(RequestedModelID));
            }
            else if (!bOldHasStar && bNewHasStar)
            {
                Controller_DeliveryEnsurer.Get().DB_UpdateItem_FireAndForget(
                    _Context,
                    GloballySharedModelIDsDBEntry.DBSERVICE_GLOBALLY_SHARED_MODEL_IDS_TABLE(),
                    GloballySharedModelIDsDBEntry.KEY_NAME_MODEL_ID,
                    new BPrimitiveType(RequestedModelID),
                    JObject.Parse(JsonConvert.SerializeObject(new GloballySharedModelIDsDBEntry())));
            }

            Controller_ModelActions.Get().BroadcastModelAction(new Action_ModelSharedWithUserIdsChanged
            (
                RequestedModelID,
                Model.ModelOwnerUserID,
                FinalUserIds,
                OldSharedList,
                AuthorizedUser.UserID
            ),
            _ErrorMessageAction);

            return BWebResponse.StatusOK("Operation has been completed.");
        }
    }
}