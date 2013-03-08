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
import java.lang.String;
import java.lang.Boolean;

public class GetDatabase implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "get_database";
	}
	
	@Override
	public List<String> getParams() {
		return new ArrayList<String>();
	}
	
	@Override
	public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
		// only allow GET method
		String verb = request.getVerb().toString();
		if (!verb.equalsIgnoreCase("get")) {
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
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			Map<String, Object> returnMap = new HashMap<String, Object>();
			String[] userStringFields = {"username", "name", "profile_image_url", "group_order"};
			String[] friendStringFields = {"username", "name", "profile_image_url"};
			String[] statusStringFields = {"action", "place"};
			String[] groupStringFields = {"group_id", "title", "relationship_order"};
			// fetch user object
			// - build query
			List<SMCondition> userQuery = new ArrayList<SMCondition>();
			userQuery.add(new SMEquals("username", new SMString(username)));
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery);
			if (users != null && users.size() == 1) {
				SMObject userObject = users.get(0);
				// 1. username, name, profile image, group order
				for (int i = 0; i < userStringFields.length; i++) {
					SMValue fieldValue = userObject.getValue().get(userStringFields[i]);
					String value = ((SMString)fieldValue).getValue();
					returnMap.put(userStringFields[i], value);
				}
				// check if user already has status or not
				if (userObject.getValue().containsKey("status")) {
					returnMap.put("new_user", new SMBoolean(Boolean.FALSE));
					// fetch user's status
					// - get status id
					SMValue statusIdValue = userObject.getValue().get("status");
					// - build query
					List<SMCondition> statusQuery = new ArrayList<SMCondition>();
					statusQuery.add(new SMEquals("status_id", ((SMString)statusIdValue)));
					// - execute query
					List<SMObject> statuses = dataService.readObjects("status", statusQuery);
					if (statuses != null && statuses.size() == 1) {
						SMObject statusObject = statuses.get(0);
						// 2. action, place
						for (int i = 0; i < statusStringFields.length; i++) {
							SMValue fieldValue = statusObject.getValue().get(statusStringFields[i]);
							String value = ((SMString)fieldValue).getValue();
							returnMap.put(statusStringFields[i], value);
						}
						// 3. status mod date
						SMValue modDateValue = statusObject.getValue().get("status_mod_date");
						Long modDate = ((SMInt)modDateValue).getValue();
						returnMap.put("status_mod_date", modDate);
					} else {
						// TO DO:
						// handle status fetch error
						
					}
				} else {
					returnMap.put("new_user", new SMBoolean(Boolean.TRUE));
				}
				// 4. friends
				List<Map<String, Object>> friends = new ArrayList<Map<String, Object>>();
				// relationships by user
				List<SMString> relUserList = new ArrayList<SMString>();
				if (userObject.getValue().containsKey("relationships_by_user")) {
					SMValue relUserValue = userObject.getValue().get("relationships_by_user");
					relUserList = ((SMList<SMString>)relUserValue).getValue();
				}
				for (int i = 0; i < relUserList.size(); i++) {
					Map<String, Object> friendMap = new HashMap<String, Object>();
					// fetch user's relationship
					// - get relationship id
					SMString relIdString = relUserList.get(i);
					// - build query
					List<SMCondition> relQuery = new ArrayList<SMCondition>();
					relQuery.add(new SMEquals("relationship_id", relIdString));
					// - execute query
					List<SMObject> rels = dataService.readObjects("relationship", relQuery);
					if (rels != null && rels.size() == 1) {
						SMObject relObject = rels.get(0);
						// 4.1. relationship id
						SMValue relIdValue = relObject.getValue().get("relationship_id");
						String relId = ((SMString)relIdValue).getValue();
						friendMap.put("relationship_id", relId);
						// 4.2. type by user
						SMValue typeUserValue = relObject.getValue().get("type_by_owner");
						Long typeUser = ((SMInt)typeUserValue).getValue();
						friendMap.put("type_by_user", typeUser);
						// 4.3 type by friend
						SMValue typeFriendValue = relObject.getValue().get("type_by_receiver");
						Long typeFriend = ((SMInt)typeFriendValue).getValue();
						friendMap.put("type_by_friend", typeFriend);
						// fetch friend's user object
						// - get friend's username
						SMValue friendUsernameValue = relObject.getValue().get("receiver");
						// - build query
						List<SMCondition> friendQuery = new ArrayList<SMCondition>();
						friendQuery.add(new SMEquals("username", (SMString)friendUsernameValue));
						// - execute query
						List<SMObject> friendUsers = dataService.readObjects("user", friendQuery);
						if (friendUsers != null && friendUsers.size() == 1) {
							SMObject friendObject = friendUsers.get(0);
							// 4.4. username, name, profile image
							for (int j = 0; j < friendStringFields.length; j++) {
								SMValue fieldValue = friendObject.getValue().get(friendStringFields[j]);
								String value = ((SMString)fieldValue).getValue();
								friendMap.put(friendStringFields[j], value);
							}
							// fetch friend's status
							// - get status id
							SMValue friendStatusIdValue = friendObject.getValue().get("status");
							// - build query
							List<SMCondition> friendStatusQuery = new ArrayList<SMCondition>();
							friendStatusQuery.add(new SMEquals("status_id", ((SMString)friendStatusIdValue)));
							// - execute query
							List<SMObject> friendStatuses = dataService.readObjects("status", friendStatusQuery);
							if (friendStatuses != null && friendStatuses.size() == 1) {
								SMObject friendStatusObject = friendStatuses.get(0);
								// 4.5. action, place
								for (int k = 0; k < statusStringFields.length; k++) {
									SMValue fieldValue = friendStatusObject.getValue().get(statusStringFields[k]);
									String value = ((SMString)fieldValue).getValue();
									friendMap.put(statusStringFields[k], value);
								}
								// 4.6. status mod date
								SMValue modDateValue = friendStatusObject.getValue().get("status_mod_date");
								Long modDate = ((SMInt)modDateValue).getValue();
								friendMap.put("status_mod_date", modDate);
							} else {
								// TO DO:
								// handle friend's status fetch error
								
							}
						} else {
							// TO DO:
							// handle friend's user fetch error
							
						}
					} else {
						// TO DO:
						// handle relationship fetch error
						
					}
					friends.add(friendMap);
				}
				// relationships by others
				List<SMString> relOthersList = new ArrayList<SMString>();
				if (userObject.getValue().containsKey("relationships_by_others")) {
					SMValue relOthersValue = userObject.getValue().get("relationships_by_others");
					relOthersList = ((SMList<SMString>)relOthersValue).getValue();
				}
				for (int i = 0; i < relOthersList.size(); i++) {
					Map<String, Object> friendMap = new HashMap<String, Object>();
					// fetch user's relationship
					// - get relationship id
					SMString relIdString = relOthersList.get(i);
					// - build query
					List<SMCondition> relQuery = new ArrayList<SMCondition>();
					relQuery.add(new SMEquals("relationship_id", relIdString));
					// - execute query
					List<SMObject> rels = dataService.readObjects("relationship", relQuery);
					if (rels != null && rels.size() == 1) {
						SMObject relObject = rels.get(0);
						// 4.1. relationship id
						SMValue relIdValue = relObject.getValue().get("relationship_id");
						String relId = ((SMString)relIdValue).getValue();
						friendMap.put("relationship_id", relId);
						// 4.2. type by user
						SMValue typeUserValue = relObject.getValue().get("type_by_receiver");
						Long typeUser = ((SMInt)typeUserValue).getValue();
						friendMap.put("type_by_user", typeUser);
						// 4.3 type by friend
						SMValue typeFriendValue = relObject.getValue().get("type_by_owner");
						Long typeFriend = ((SMInt)typeFriendValue).getValue();
						friendMap.put("type_by_friend", typeFriend);
						// fetch friend's user object
						// - get friend's username
						SMValue friendUsernameValue = relObject.getValue().get("owner");
						// - build query
						List<SMCondition> friendQuery = new ArrayList<SMCondition>();
						friendQuery.add(new SMEquals("username", (SMString)friendUsernameValue));
						// - execute query
						List<SMObject> friendUsers = dataService.readObjects("user", friendQuery);
						if (friendUsers != null && friendUsers.size() == 1) {
							SMObject friendObject = friendUsers.get(0);
							// 4.4. username, name, profile image
							for (int j = 0; j < friendStringFields.length; j++) {
								SMValue fieldValue = friendObject.getValue().get(friendStringFields[j]);
								String value = ((SMString)fieldValue).getValue();
								friendMap.put(friendStringFields[j], value);
							}
							// fetch friend's status
							// - get status id
							SMValue friendStatusIdValue = friendObject.getValue().get("status");
							// - build query
							List<SMCondition> friendStatusQuery = new ArrayList<SMCondition>();
							friendStatusQuery.add(new SMEquals("status_id", ((SMString)friendStatusIdValue)));
							// - execute query
							List<SMObject> friendStatuses = dataService.readObjects("status", friendStatusQuery);
							if (friendStatuses != null && friendStatuses.size() == 1) {
								SMObject friendStatusObject = friendStatuses.get(0);
								// 4.5. action, place
								for (int k = 0; k < statusStringFields.length; k++) {
									SMValue fieldValue = friendStatusObject.getValue().get(statusStringFields[k]);
									String value = ((SMString)fieldValue).getValue();
									friendMap.put(statusStringFields[k], value);
								}
								// 4.6. status mod date
								SMValue modDateValue = friendStatusObject.getValue().get("status_mod_date");
								Long modDate = ((SMInt)modDateValue).getValue();
								friendMap.put("status_mod_date", modDate);
							} else {
								// TO DO:
								// handle friend's status fetch error
								
							}
						} else {
							// TO DO:
							// handle friend's user fetch error
							
						}
					} else {
						// TO DO:
						// handle relationship fetch error
						
					}
					friends.add(friendMap);
				}
				returnMap.put("friends", friends);
				// 5. groups
				List<Map<String, Object>> localGroups = new ArrayList<Map<String, Object>>();
				List<SMString> groupsList = new ArrayList<SMString>();
				if (userObject.getValue().containsKey("groups")) {
					SMValue groupsValue = userObject.getValue().get("groups");
					groupsList = ((SMList<SMString>)groupsValue).getValue();
				}
				for (int i = 0; i < groupsList.size(); i++) {
					Map<String, Object> groupMap = new HashMap<String, Object>();
					// fetch group
					// - get group id
					SMString groupId = groupsList.get(i);
					// - build query
					List<SMCondition> groupQuery = new ArrayList<SMCondition>();
					groupQuery.add(new SMEquals("group_id", groupId));
					// - execute query
					List<SMObject> groups = dataService.readObjects("group", groupQuery);
					if (groups != null && groups.size() == 1) {
						SMObject groupObject = groups.get(0);
						// 5.1. group id, title, friend order
						for (int j = 0; j < groupStringFields.length; j++) {
							SMValue fieldValue = groupObject.getValue().get(groupStringFields[j]);
							String value = ((SMString)fieldValue).getValue();
							groupMap.put(groupStringFields[j], value);
						}
						// 5.2. group members
						List<String> friendsList = new ArrayList<String>();
						// relationships by owner
						List<SMString> relsGroupOwnerList = new ArrayList<SMString>();
						if (groupObject.getValue().containsKey("relationships_by_owner")) {
							SMValue relsGroupOwnerValue = groupObject.getValue().get("relationships_by_owner");
							relsGroupOwnerList = ((SMList<SMString>)relsGroupOwnerValue).getValue();
						}
						for (int j = 0; j < relsGroupOwnerList.size(); j++) {
							friendsList.add(relsGroupOwnerList.get(j).getValue());
						}
						// relationships by others
						List<SMString> relsGroupOthersList = new ArrayList<SMString>();
						if (groupObject.getValue().containsKey("relationships_by_others")) {
							SMValue relsGroupOthersValue = groupObject.getValue().get("relationships_by_others");
							relsGroupOthersList = ((SMList<SMString>)relsGroupOthersValue).getValue();
						}
						for (int j = 0; j < relsGroupOthersList.size(); j++) {
							friendsList.add(relsGroupOthersList.get(j).getValue());
						}
						groupMap.put("friends", friendsList);
					} else {
						// TO DO:
						// handle group fetch error
						
					}
					localGroups.add(groupMap);
				}
				returnMap.put("groups", localGroups);
			} else {
				// TO DO:
				// handle user fetch error
				
			}
			return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
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
