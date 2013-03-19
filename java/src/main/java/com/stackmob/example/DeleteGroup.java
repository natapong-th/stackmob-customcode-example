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
import java.lang.Long;
import java.lang.System;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class DeleteGroup implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "delete_group";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("group_id");
	}
	
	@Override
	public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
		// only allow DELETE method
		String verb = request.getVerb().toString();
		if (!verb.equalsIgnoreCase("delete")) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid method");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_METHOD, errParams); // http 405 - method not allowed
		}
		
		// try getting logged-in user
		String username = request.getLoggedInUser();
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		SMString userId = new SMString(username);
		
		// get update parameters
		String groupIdString = "";
		try {
			groupIdString = request.getParams().get("group_id");
		} catch (Exception e) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid request parameter");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		SMString groupId = new SMString(groupIdString);
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch group object
			// - build query
			List<SMCondition> groupQuery = new ArrayList<SMCondition>();
			groupQuery.add(new SMEquals("group_id", groupId));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("owner");
			fields.add("owner.username");
			fields.add("owner.group_order");
			fields.add("relationships_by_owner");
			fields.add("relationships_by_others");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> groups = dataService.readObjects("group", groupQuery, 1, filter);
			if (groups != null && groups.size() == 1) {
				SMObject groupObject = groups.get(0);
				// check if this user is the owner
				SMObject ownerObject = (SMObject)groupObject.getValue().get("owner");
				SMString ownerId = (SMString)ownerObject.getValue().get("username");
				if (!ownerId.equals(userId)) {
					HashMap<String, String> errParams = new HashMap<String, String>();
					errParams.put("error", "requested groups are inaccessible by this user");
					return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
				}
				
				Map<String, Object> returnMap = new HashMap<String, Object>();
				List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
				List<SMString> groupIdList = new ArrayList<SMString>();
				groupIdList.add(groupId);
				// 1. remove from each relationship's groups
				// - relationships by owner
				List<SMString> relOwnerIds = new ArrayList<SMString>();
				if (ownerObject.getValue().containsKey("relationships_by_owner")) {
					SMList<SMString> relOwnerList = (SMList<SMString>)ownerObject.getValue().get("relationships_by_owner");
					relOwnerIds = relOwnerList.getValue();
				}
				for (int i = 0; i < relOwnerIds.size(); i++) {
					SMString relId = relOwnerIds.get(i);
					dataService.removeRelatedObjects("relationship", relId, "groups_by_owner", groupIdList, false);
				}
				// - relationships by others
				List<SMString> relOthersIds = new ArrayList<SMString>();
				if (ownerObject.getValue().containsKey("relationships_by_others")) {
					SMList<SMString> relOthersList = (SMList<SMString>)ownerObject.getValue().get("relationships_by_others");
					relOthersIds = relOthersList.getValue();
				}
				for (int i = 0; i < relOthersIds.size(); i++) {
					SMString relId = relOthersIds.get(i);
					dataService.removeRelatedObjects("relationship", relId, "groups_by_receiver", groupIdList, false);
				}
				// 2. remove from owner's groups
				dataService.removeRelatedObjects("user", ownerId, "groups", groupIdList, false);
				
				// 3. remove from owner's group order
				List<SMString> groupOrder = new ArrayList<SMString>();
				if (ownerObject.getValue().containsKey("group_order")) {
					SMList<SMString> groupOrderValue = (SMList<SMString>)ownerObject.getValue().get("group_order");
					groupOrder = groupOrderValue.getValue();
				}
				for (int i = 0; i < groupOrder.size(); i++) {
					SMString orderId = groupOrder.get(i);
					if (orderId.equals(groupId)) {
						groupOrder.remove(i);
						userUpdates.add(new SMSet("group_order", new SMList<SMString>(groupOrder)));
						returnMap.put("group_order", groupOrder);
						break;
					}
				}
				// 4. delete group
				dataService.deleteObject("group", groupId);
				
				// 5. update groups mod date
				long currentTime = System.currentTimeMillis();
				userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
				returnMap.put("groups_mod_date", new Long(currentTime));
				
				// update user
				dataService.updateObject("user", userId, userUpdates);
				
				// return updated data for local database
				returnMap.put("group_id", groupId);
				return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
			} else {
				// TO DO:
				// handle group fetch error
				
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid group fetch");
				errMap.put("detail", (groups == null ? "null fetch result" : ("fetch result count = " + groups.size())));
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
