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

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class UpdateRelationships implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "update_relationships";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("relationship_ids", "type");
	}
	
	@Override
	public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
		// only allow PUT method
		String verb = request.getVerb().toString();
		if (!verb.equalsIgnoreCase("put")) {
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
		
		// get update parameters
		String relIds = "";
		long type = 0L;
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("relationship_ids")) {
					relIds = jsonObj.getString("relationship_ids");
				}
				if (!jsonObj.isNull("type")) {
					type = Long.parseLong(jsonObj.getString("type"));
				}
			} catch (Exception e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (relIds.isEmpty() || type < 2 || type > 4) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid parameters");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch relationship objects
			// - build query
			List<SMCondition> relQuery = new ArrayList<SMCondition>();
			String[] relArray = relIds.split("|");
			List<SMString> relList = new ArrayList<SMString>();
			for (int i = 0; i < relArray.length; i++) {
				relList.add(new SMString(relArray[i]));
			}
			relQuery.add(new SMIn("relationship_id", relList));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			fields.add("relationship_id");
			fields.add("owner");
			fields.add("owner.username");
			fields.add("receiver");
			fields.add("receiver.username");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> rels = dataService.readObjects("relationship", relQuery, 1, filter);
			if (rels != null && rels.size() == relList.size()) {
				String foundRelIds = "";
				for (int i = 0; i < rels.size(); i++) {
					SMObject relObject = rels.get(i);
					SMString relId = (SMString) relObject.getValue().get("relationship_id");
					// check if fetched relationship is as requested
					int idx = relIds.indexOf(relId.getValue());
					if (idx != -1) {
						relIds = relIds.replaceFirst(relId.getValue(), "");
						foundRelIds = foundRelIds + relId.getValue() + "|";
					} else {
						HashMap<String, String> errMap = new HashMap<String, String>();
						errMap.put("error", "invalid relationship fetch");
						errMap.put("detail", "unexpected fetch result = " + relId.getValue());
						return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
					}
					// update type according to user's position (owner/receiver)
					SMObject ownerObject = (SMObject) relObject.getValue().get("owner");
					SMString ownerUsername = (SMString) ownerObject.getValue().get("username");
					SMObject receiverObject = (SMObject) relObject.getValue().get("receiver");
					SMString receiverUsername = (SMString) receiverObject.getValue().get("username");
					List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
					if (ownerUsername.getValue().equals(username)) {
						relUpdates.add(new SMSet("type_by_owner", new SMInt(type)));
					} else if (receiverUsername.getValue().equals(username)) {
						relUpdates.add(new SMSet("type_by_receiver", new SMInt(type)));
					} else {
						HashMap<String, String> errParams = new HashMap<String, String>();
						errParams.put("error", "requested relationships are inaccessible by this user");
						return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
					}
					dataService.updateObject("relationship", relId, relUpdates);
				}
				// return updated data for local database
				Map<String, Object> returnMap = new HashMap<String, Object>();
				returnMap.put("relationship_ids", foundRelIds);
				return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
			} else {
				// TO DO:
				// handle relationship fetch error
				
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid relationship fetch");
				errMap.put("detail", (rels == null ? "null fetch result" : ("fetch result count = " + rels.size())));
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
