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

public class GetDatabase implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "get_database";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("last_sync_date");
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
		
		// get the parameter
		long lastSyncDate = 0;
		try {
			lastSyncDate = Long.parseLong(request.getParams().get("last_sync_date"));
		} catch (Exception e) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "invalid request parameter");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			String[] userStringFields = {"name", "profile_image_url"};
			String[] statusStringFields = {"action", "place"};
			String[] groupStringFields = {"group_id", "title"};
			// fetch user object
			// - build query
			List<SMCondition> userQuery = new ArrayList<SMCondition>();
			userQuery.add(new SMEquals("username", new SMString(username)));
			// - build result filter
			// -- build required fields
			List<String> fields = new ArrayList<String>();
			// -- 1. user
			for (int i = 0; i < userStringFields.length; i++) {
				fields.add(userStringFields[i]);
			}
			for (int i = 0; i < statusStringFields.length; i++) {
				fields.add(statusStringFields[i]);
			}
			fields.add("group_order");
			fields.add("user_mod_date");
			fields.add("status_mod_date");
			fields.add("groups_mod_date");
			// -- 2. relationships by user
			fields.add("relationships_by_user");
			fields.add("relationships_by_user.relationship_id");
			fields.add("relationships_by_user.type_by_owner");
			fields.add("relationships_by_user.type_by_receiver");
			fields.add("relationships_by_user.invite_email");
			// -- 2.1. relationships by user's receiver
			fields.add("relationships_by_user.receiver");
			fields.add("relationships_by_user.receiver.username");
			for (int i = 0; i < userStringFields.length; i++) {
				fields.add("relationships_by_user.receiver." + userStringFields[i]);
			}
			for (int i = 0; i < statusStringFields.length; i++) {
				fields.add("relationships_by_user.receiver." + statusStringFields[i]);
			}
			fields.add("relationships_by_user.receiver.user_mod_date");
			fields.add("relationships_by_user.receiver.status_mod_date");
			// -- 2.2. relationships by user's events
			fields.add("relationships_by_user.events_by_receiver");
			fields.add("relationships_by_user.events_by_receiver.event_id");
			fields.add("relationships_by_user.events_by_receiver.type");
			fields.add("relationships_by_user.events_by_receiver.createddate");
			// -- 3. relationships by others
			fields.add("relationships_by_others");
			fields.add("relationships_by_others.relationship_id");
			fields.add("relationships_by_others.type_by_owner");
			fields.add("relationships_by_others.type_by_receiver");
			// -- 3.1. relationships by others' owner
			fields.add("relationships_by_others.owner");
			fields.add("relationships_by_others.owner.username");
			for (int i = 0; i < userStringFields.length; i++) {
				fields.add("relationships_by_others.owner." + userStringFields[i]);
			}
			for (int i = 0; i < statusStringFields.length; i++) {
				fields.add("relationships_by_others.owner." + statusStringFields[i]);
			}
			fields.add("relationships_by_others.owner.user_mod_date");
			fields.add("relationships_by_others.owner.status_mod_date");
			// -- 3.2. relationships by others' events
			fields.add("relationships_by_others.events_by_owner");
			fields.add("relationships_by_others.events_by_owner.event_id");
			fields.add("relationships_by_others.events_by_owner.type");
			fields.add("relationships_by_others.events_by_owner.createddate");
			// -- 4. groups
			fields.add("groups");
			for (int i = 0; i < groupStringFields.length; i++) {
				fields.add("groups." + groupStringFields[i]);
			}
			fields.add("groups.relationship_order");
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery, 2, filter);
			if (users == null || users.size() != 1) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid user fetch");
				errMap.put("detail", (users == null ? "null fetch result" : ("fetch result count = " + users.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
			SMObject userObject = users.get(0);
			Map<String, Object> returnMap = new HashMap<String, Object>();
 			// 1. username
			returnMap.put("username", username);
			// 2. name, profile image, group order (check user mod date)
			SMInt userModValue = (SMInt)userObject.getValue().get("user_mod_date");
			long userModDate = userModValue.getValue().longValue();
			if (lastSyncDate < userModDate) {
				for (int i = 0; i < userStringFields.length; i++) {
					SMString fieldValue = (SMString)userObject.getValue().get(userStringFields[i]);
					returnMap.put(userStringFields[i], fieldValue.getValue());
				}
				List<SMString> groupOrderList = new ArrayList<SMString>();
				if (userObject.getValue().containsKey("group_order")) {
					SMList<SMString> groupOrderValue = (SMList<SMString>)userObject.getValue().get("group_order");
					groupOrderList = groupOrderValue.getValue();
				}
				returnMap.put("group_order", groupOrderList);
			}
			// 3. action, place, status mod date (check status mod date)
			SMInt statusModValue = (SMInt)userObject.getValue().get("status_mod_date");
			long statusModDate = statusModValue.getValue().longValue();
			if (lastSyncDate < statusModDate) {
				for (int i = 0; i < statusStringFields.length; i++) {
					SMString fieldValue = (SMString)userObject.getValue().get(statusStringFields[i]);
					returnMap.put(statusStringFields[i], fieldValue.getValue());
				}
				returnMap.put("status_mod_date", statusModValue.getValue());
			}
			// 4. friends
			List<Map<String, Object>> friends = new ArrayList<Map<String, Object>>();
			// relationships by user
			List<SMObject> relUserList = new ArrayList<SMObject>();
			if (userObject.getValue().containsKey("relationships_by_user")) {
				SMList<SMObject> relUserValue = (SMList<SMObject>)userObject.getValue().get("relationships_by_user");
				relUserList = relUserValue.getValue();
			}
			for (int i = 0; i < relUserList.size(); i++) {
				SMObject relObject = relUserList.get(i);
				// do not return deleted friends
				SMInt typeUserValue = (SMInt)relObject.getValue().get("type_by_owner");
				Long typeUser = typeUserValue.getValue();
				if (typeUser.longValue() == 4L) {
					break;
				}
				
				Map<String, Object> friendMap = new HashMap<String, Object>();
				// 4.1. relationship id
				SMString relIdValue = (SMString)relObject.getValue().get("relationship_id");
				friendMap.put("relationship_id", relIdValue.getValue());
				// 4.2. type by user
				friendMap.put("type_by_user", typeUser);
				// 4.3. type by friend
				SMInt typeFriendValue = (SMInt)relObject.getValue().get("type_by_receiver");
				Long typeFriend = typeFriendValue.getValue();
				friendMap.put("type_by_friend", typeFriend);
				// check if this relationship is an invite
				SMString inviteValue = (SMString)relObject.getValue().get("invite_email");
				if (inviteValue.getValue().isEmpty()) {
					SMObject friendObject = (SMObject)relObject.getValue().get("receiver");
					// 4.4. username
					SMString friendIdValue = (SMString)friendObject.getValue().get("username");
					friendMap.put("username", friendIdValue.getValue());
					// 4.5. name, profile image (check user mod date)
					SMInt fUserModValue = (SMInt)friendObject.getValue().get("user_mod_date");
					long fUserModDate = fUserModValue.getValue().longValue();
					if (lastSyncDate < fUserModDate) {
						for (int j = 0; j < userStringFields.length; j++) {
							SMString fieldValue = (SMString)friendObject.getValue().get(userStringFields[j]);
							friendMap.put(userStringFields[j], fieldValue.getValue());
						}
					}
					// check if type is mutual friend
					if (typeUser.longValue() == 2L && typeFriend.longValue() == 2L) {
						// 4.6. action, place, status mod date (check status mod date)
						SMInt fStatusModValue = (SMInt)friendObject.getValue().get("status_mod_date");
						long fStatusModDate = fStatusModValue.getValue().longValue();
						if (lastSyncDate < fStatusModDate) {
							for (int j = 0; j < statusStringFields.length; j++) {
								SMString fieldValue = (SMString)friendObject.getValue().get(statusStringFields[j]);
								friendMap.put(statusStringFields[j], fieldValue.getValue());
							}
							friendMap.put("status_mod_date", fStatusModValue.getValue());
						}
						// 4.7. events
						List<SMObject> eventsList = new ArrayList<SMObject>();
						if (relObject.getValue().containsKey("events_by_receiver")) {
							SMList<SMObject> eventsValue = (SMList<SMObject>)relObject.getValue().get("events_by_receiver");
							eventsList = eventsValue.getValue();
						}
						friendMap.put("events", eventsList);
					}
				} else {
					friendMap.put("invite_email", inviteValue.getValue());
				}
				
				friends.add(friendMap);
			}
			// relationships by others
			List<SMObject> relOthersList = new ArrayList<SMObject>();
			if (userObject.getValue().containsKey("relationships_by_others")) {
				SMList<SMObject> relOthersValue = (SMList<SMObject>)userObject.getValue().get("relationships_by_others");
				relOthersList = relOthersValue.getValue();
			}
			for (int i = 0; i < relOthersList.size(); i++) {
				SMObject relObject = relOthersList.get(i);
				// do not return deleted friends
				SMInt typeUserValue = (SMInt)relObject.getValue().get("type_by_receiver");
				Long typeUser = typeUserValue.getValue();
				if (typeUser.longValue() == 4L) {
					break;
				}
				
				Map<String, Object> friendMap = new HashMap<String, Object>();
				// 4.1. relationship id
				SMString relIdValue = (SMString)relObject.getValue().get("relationship_id");
				friendMap.put("relationship_id", relIdValue.getValue());
				// 4.2. type by user
				friendMap.put("type_by_user", typeUser);
				// 4.3. type by friend
				SMInt typeFriendValue = (SMInt)relObject.getValue().get("type_by_owner");
				Long typeFriend = typeFriendValue.getValue();
				friendMap.put("type_by_friend", typeFriend);
				
				SMObject friendObject = (SMObject)relObject.getValue().get("owner");
				// 4.4. username
				SMString friendIdValue = (SMString)friendObject.getValue().get("username");
				friendMap.put("username", friendIdValue.getValue());
				// 4.5. name, profile image (check user mod date)
				SMInt fUserModValue = (SMInt)friendObject.getValue().get("user_mod_date");
				long fUserModDate = fUserModValue.getValue().longValue();
				if (lastSyncDate < fUserModDate) {
					for (int j = 0; j < userStringFields.length; j++) {
						SMString fieldValue = (SMString)friendObject.getValue().get(userStringFields[j]);
						friendMap.put(userStringFields[j], fieldValue.getValue());
					}
				}
				// check if type is mutual friend
				if (typeUser.longValue() == 2L && typeFriend.longValue() == 2L) {
					// 4.6. action, place, status mod date (check status mod date)
					SMInt fStatusModValue = (SMInt)friendObject.getValue().get("status_mod_date");
					long fStatusModDate = fStatusModValue.getValue().longValue();
					if (lastSyncDate < fStatusModDate) {
						for (int j = 0; j < statusStringFields.length; j++) {
							SMString fieldValue = (SMString)friendObject.getValue().get(statusStringFields[j]);
							friendMap.put(statusStringFields[j], fieldValue.getValue());
						}
						friendMap.put("status_mod_date", fStatusModValue.getValue());
					}
					// 4.7. events
					List<SMObject> eventsList = new ArrayList<SMObject>();
					if (relObject.getValue().containsKey("events_by_owner")) {
						SMList<SMObject> eventsValue = (SMList<SMObject>)relObject.getValue().get("events_by_owner");
						eventsList = eventsValue.getValue();
					}
					friendMap.put("events", eventsList);
				}
				friends.add(friendMap);
			}
			returnMap.put("friends", friends);
			
			// 5. groups (check groups mod date)
			SMInt groupsModValue = (SMInt)userObject.getValue().get("groups_mod_date");
			long groupsModDate = groupsModValue.getValue().longValue();
			if (lastSyncDate < groupsModDate) {
				List<Map<String, Object>> localGroups = new ArrayList<Map<String, Object>>();
				List<SMObject> groupsList = new ArrayList<SMObject>();
				if (userObject.getValue().containsKey("groups")) {
					SMList<SMObject> groupsValue = (SMList<SMObject>)userObject.getValue().get("groups");
					groupsList = groupsValue.getValue();
				}
				for (int i = 0; i < groupsList.size(); i++) {
					Map<String, Object> groupMap = new HashMap<String, Object>();
					SMObject groupObject = groupsList.get(i);
					// 5.1. group id, title
					for (int j = 0; j < groupStringFields.length; j++) {
						SMString fieldValue = (SMString)groupObject.getValue().get(groupStringFields[j]);
						groupMap.put(groupStringFields[j], fieldValue.getValue());
					}
					// 5.2. friend order
					List<SMString> friendOrderList = new ArrayList<SMString>();
					if (groupObject.getValue().containsKey("relationship_order")) {
						SMList<SMString> friendOrderValue = (SMList<SMString>)groupObject.getValue().get("relationship_order");
						friendOrderList = friendOrderValue.getValue();
					}
					groupMap.put("friend_order", friendOrderList);
					
					localGroups.add(groupMap);
				}
				returnMap.put("groups", localGroups);
			}
			// return the translated database
			long currentTime = System.currentTimeMillis();
			returnMap.put("last_sync_date", new Long(currentTime));
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
