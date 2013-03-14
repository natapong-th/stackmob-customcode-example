/**
 * Copyright 2012-2013 StackMob
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.stackmob.example;

import com.stackmob.core.customcode.CustomCodeMethod;
import com.stackmob.core.rest.ProcessedAPIRequest;
import com.stackmob.core.rest.ResponseToProcess;
import com.stackmob.sdkapi.SDKServiceProvider;
import com.stackmob.sdkapi.*;

import com.stackmob.core.InvalidSchemaException;
import com.stackmob.core.DatastoreException;

import java.net.HttpURLConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.lang.String;
import java.lang.Boolean;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

// ***DEPRECATED***
public class CreateRelationship implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_relationship";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("username");
	}
	
	@Override
	public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
		// only allow POST method
		String verb = request.getVerb().toString();
		if (!verb.equalsIgnoreCase("post")) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid method");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_METHOD, errParams); // http 405 - method not allowed
		}
		
		// try getting logged-in user
		String username = request.getLoggedInUser();
		SMString userId = new SMString(username);
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		
		// get requested friend's username
		String friendUsername = "";
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("username")) {
					friendUsername = jsonObj.getString("username");
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (friendUsername == null || friendUsername.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "username parameter not found");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		} else if (friendUsername.equals(username)) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "cannot add relationship with yourself");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		SMString friendId = new SMString(friendUsername);
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// check if friend's username exists
			// fetch friend object
			// - build query
			List<SMCondition> friendQuery = new ArrayList<SMCondition>();
			friendQuery.add(new SMEquals("username", friendId));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("relationships_by_user");
			fields.add("relationships_by_user.relationship_id");
			fields.add("relationships_by_user.type_by_receiver");
			fields.add("relationships_by_user.receiver");
			fields.add("relationships_by_user.receiver.username");
			fields.add("relationships_by_others");
			fields.add("relationships_by_others.relationship_id");
			fields.add("relationships_by_others.type_by_owner");
			fields.add("relationships_by_others.owner");
			fields.add("relationships_by_others.owner.username");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> friends = dataService.readObjects("user", friendQuery, 2, filter);
			if (friends != null && friends.size() == 1) {
				SMObject friendObject = friends.get(0);
				Map<String, Object> returnMap = new HashMap<String, Object>();
				// check if the relationship already exists
				// - relationships by user
				List<SMObject> relUserList = new ArrayList<SMObject>();
				if (friendObject.getValue().containsKey("relationships_by_user")) {
					SMList<SMObject> relUserValue = (SMList<SMObject>)friendObject.getValue().get("relationships_by_user");
					relUserList = relUserValue.getValue();
				}
				for (int i = 0; i < relUserList.size(); i++) {
					SMObject relObject = relUserList.get(i);
					SMObject userObject = (SMObject)relObject.getValue().get("receiver");
					SMString tempId = (SMString)userObject.getValue().get("username");
					if (tempId.getValue().equals(username)) {
						SMInt typeUserValue = (SMInt)relObject.getValue().get("type_by_receiver");
						Long typeUser = typeUserValue.getValue();
						// if it's deleted by user, change to friend
						// otherwise do not change
						if (typeUser.longValue() == 4L) {
							SMString relId = (SMString)relObject.getValue().get("relationship_id");
							List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
							relUpdates.add(new SMSet("type_by_receiver", new SMInt(2L)));
							dataService.updateObject("relationship", relId, relUpdates);
							returnMap.put("new_relationship", Boolean.FALSE);
							returnMap.put("relationship_id", relId.getValue());
							return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
						} else {
							HashMap<String, String> errMap = new HashMap<String, String>();
							errMap.put("error", "cannot create existing relationship");
							return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
						}
					}
				}
				// - relationships by others
				List<SMObject> relOthersList = new ArrayList<SMObject>();
				if (friendObject.getValue().containsKey("relationships_by_others")) {
					SMList<SMObject> relOthersValue = (SMList<SMObject>)friendObject.getValue().get("relationships_by_others");
					relOthersList = relOthersValue.getValue();
				}
				for (int i = 0; i < relOthersList.size(); i++) {
					SMObject relObject = relOthersList.get(i);
					SMObject userObject = (SMObject)relObject.getValue().get("owner");
					SMString tempId = (SMString)userObject.getValue().get("username");
					if (tempId.getValue().equals(username)) {
						SMInt typeUserValue = (SMInt)relObject.getValue().get("type_by_owner");
						Long typeUser = typeUserValue.getValue();
						// if it's deleted by user, change to friend
						// otherwise do not change
						if (typeUser.longValue() == 4L) {
							SMString relId = (SMString)relObject.getValue().get("relationship_id");
							List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
							relUpdates.add(new SMSet("type_by_receiver", new SMInt(2L)));
							dataService.updateObject("relationship", relId, relUpdates);
							returnMap.put("new_relationship", Boolean.FALSE);
							returnMap.put("relationship_id", relId.getValue());
							return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
						} else {
							HashMap<String, String> errMap = new HashMap<String, String>();
							errMap.put("error", "cannot create existing relationship");
							return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
						}
					}
				}
				// if not, create a new relationship
				Map<String, SMValue> relMap = new HashMap<String, SMValue>();
				relMap.put("sm_owner", new SMString("user/" + username));
				relMap.put("type_by_owner", new SMInt(2L));
				relMap.put("type_by_receiver", new SMInt(1L));
				relMap.put("change_by_owner", new SMBoolean(false));
				relMap.put("change_by_receiver", new SMBoolean(false));
				relMap.put("interaction_by_owner", new SMInt(1L));
				relMap.put("interaction_by_receiver", new SMInt(1L));
				SMObject relObject = dataService.createObject("relationship", new SMObject(relMap));
				SMString relId = (SMString)relObject.getValue().get("relationship_id");
				
				// add relationship in user's relationships_by_user
				List<SMString> relIdList = new ArrayList<SMString>();
				relIdList.add(relId);
				dataService.addRelatedObjects("user", userId, "relationships_by_user", relIdList);
				
				// add user as relationship's owner
				List<SMString> ownerIdList = new ArrayList<SMString>();
				ownerIdList.add(userId);
				dataService.addRelatedObjects("relationship", relId, "owner", ownerIdList);
				
				// add relationship in friend's relationships_by_others
				dataService.addRelatedObjects("user", friendId, "relationships_by_others", relIdList);
				
				// add friend as relationship's receiver
				List<SMString> receiverIdList = new ArrayList<SMString>();
				receiverIdList.add(friendId);
				dataService.addRelatedObjects("relationship", relId, "receiver", receiverIdList);
				
				// return created relationship data for local database
				returnMap.put("new_relationship", Boolean.TRUE);
				returnMap.put("relationship_id", relId.getValue());
				return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
			} else if (friends != null && friends.size() == 0) {
				// TO DO:
				// create relationship and mark as invitation
				
				return new ResponseToProcess(HttpURLConnection.HTTP_OK, null);
			} else {
				// TO DO:
				// handle user fetch error
				
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid friend fetch");
				errMap.put("detail", (friends == null ? "null fetch result" : ("fetch result count = " + friends.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
		} catch (InvalidSchemaException e) {
			HashMap<String, String> errMap = new HashMap<String, String>();
			errMap.put("error", "invalid_schema");
			errMap.put("detail", e.toString());
			return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
		} catch (DatastoreException e) {
			HashMap<String, String> errMap = new HashMap<String, String>();
			errMap.put("error", "datastore_exception");
			errMap.put("detail", e.toString());
			return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
		} catch (Exception e) {
			HashMap<String, String> errMap = new HashMap<String, String>();
			errMap.put("error", "unknown");
			errMap.put("detail", e.toString());
			return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap); // http 500 - internal server error
		}
	}
}
