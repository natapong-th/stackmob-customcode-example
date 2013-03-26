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
import java.lang.System;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class CreateRelationships implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "create_relationships";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("usernames", "group_id");
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
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		SMString userId = new SMString(username);
		
		// get requested friends' usernames and group to be added (if any) 
		List<SMString> reqIds = new ArrayList<SMString>();
		String groupIdString = "";
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				if (!jsonObj.isNull("usernames")) {
					JSONArray usernameArray = jsonObj.getJSONArray("usernames");
					for (int i = 0; i < usernameArray.length(); i++) {
						String friendUsername = usernameArray.getString(i);
						// only take usernames that are not current user
						// TO DO:
						// check if email is valid as well (in case of invite)
						
						if (!friendUsername.equals(username)) {
							reqIds.add(new SMString(friendUsername));
						}
					}
				}
				if (!jsonObj.isNull("group_id")) {
					groupIdString = jsonObj.getString("group_id");
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (reqIds.size() == 0) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "usernames parameter not found");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		SMString groupId = new SMString(groupIdString);
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch user object
			// - build query
			List<SMCondition> userQuery = new ArrayList<SMCondition>();
			userQuery.add(new SMEquals("username", userId));
			// - build result filter
			List<String> userFields = new ArrayList<String>();
			userFields.add("relationships_by_user");
			userFields.add("relationships_by_user.relationship_id");
			userFields.add("relationships_by_user.type_by_owner");
			userFields.add("relationships_by_user.type_by_receiver");
			userFields.add("relationships_by_user.invite_email");
			userFields.add("relationships_by_user.receiver");
			userFields.add("relationships_by_user.receiver.username");
			userFields.add("relationships_by_user.receiver.name");
			userFields.add("relationships_by_user.receiver.profile_image_url");
			userFields.add("relationships_by_user.receiver.action");
			userFields.add("relationships_by_user.receiver.place");
			userFields.add("relationships_by_user.receiver.status_mod_date");
			userFields.add("relationships_by_others");
			userFields.add("relationships_by_others.relationship_id");
			userFields.add("relationships_by_others.type_by_owner");
			userFields.add("relationships_by_others.type_by_receiver");
			userFields.add("relationships_by_others.owner");
			userFields.add("relationships_by_others.owner.username");
			userFields.add("relationships_by_others.owner.name");
			userFields.add("relationships_by_others.owner.profile_image_url");
			userFields.add("relationships_by_others.owner.action");
			userFields.add("relationships_by_others.owner.place");
			userFields.add("relationships_by_others.owner.status_mod_date");
			ResultFilters userFilter = new ResultFilters(0, -1, null, userFields);
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery, 2, userFilter);
			// report error if query failed
			if (users == null || users.size() != 1) {
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid user fetch");
				errMap.put("detail", (users == null ? "null fetch result" : ("fetch result count = " + users.size())));
				return new ResponseToProcess(HttpURLConnection.HTTP_INTERNAL_ERROR, errMap);
			}
			
			Map<String, Object> returnMap = new HashMap<String, Object>();
			SMObject userObject = users.get(0);
			List<Map<String, Object>> addedFriends = new ArrayList<Map<String, Object>>();
			List<SMString> userRelIds = new ArrayList<SMString>();
			List<SMString> othersRelIds = new ArrayList<SMString>();
			// check if the requested relationships already exist
			// - relationships by user
			List<SMObject> relUserList = new ArrayList<SMObject>();
			if (userObject.getValue().containsKey("relationships_by_user")) {
				SMList<SMObject> relUserValue = (SMList<SMObject>)userObject.getValue().get("relationships_by_user");
				relUserList = relUserValue.getValue();
			}
			for (int i = 0; i < relUserList.size(); i++) {
				SMObject relObject = relUserList.get(i);
				SMString friendId = (SMString)relObject.getValue().get("invite_email");
				// if not an invite, get username instead 
				if (friendId.getValue().isEmpty()) {
					SMObject friendObject = (SMObject)relObject.getValue().get("receiver");
					friendId = (SMString)friendObject.getValue().get("username");
					// if it's in the requested usernames, change to friend only if it's deleted by user
					if (reqIds.remove(friendId)) {
						SMInt typeUserValue = (SMInt)relObject.getValue().get("type_by_owner");
						Long typeUser = typeUserValue.getValue();
						if (typeUser.longValue() == 4L) {
							SMString relId = (SMString)relObject.getValue().get("relationship_id");
							List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
							relUpdates.add(new SMSet("type_by_owner", new SMInt(2L)));
							dataService.updateObject("relationship", relId, relUpdates);
							
							// return the friend's data according to relationship types
							Map<String, Object> friendMap = new HashMap<String, Object>();
							friendMap.put("relationship_id", relId);
							friendMap.put("username", friendId);
							SMInt typeFriendValue = (SMInt)relObject.getValue().get("type_by_receiver");
							Long typeFriend = typeFriendValue.getValue();
							if (typeFriend.longValue() > 2L) {
								friendMap.put("type_by_friend", new Long(2L));
							} else {
								friendMap.put("type_by_friend", typeFriend);
							}
							friendMap.put("name", (SMString)friendObject.getValue().get("name"));
							friendMap.put("profile_image_url", (SMString)friendObject.getValue().get("profile_image_url"));
							if (typeFriend.longValue() == 2L) {
								friendMap.put("action", (SMString)friendObject.getValue().get("action"));
								friendMap.put("place", (SMString)friendObject.getValue().get("place"));
								friendMap.put("status_mod_date", (SMInt)friendObject.getValue().get("status_mod_date"));
							}
							addedFriends.add(friendMap);
							
							userRelIds.add(relId);
						}
					}
				// otherwise, do the same thing but return friend data of an invite instead
				} else if (reqIds.remove(friendId)) {
					SMInt typeUserValue = (SMInt)relObject.getValue().get("type_by_owner");
					Long typeUser = typeUserValue.getValue();
					if (typeUser.longValue() == 4L) {
						SMString relId = (SMString)relObject.getValue().get("relationship_id");
						List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
						relUpdates.add(new SMSet("type_by_owner", new SMInt(2L)));
						dataService.updateObject("relationship", relId, relUpdates);
						
						// return the friend's data according to relationship types
						Map<String, Object> friendMap = new HashMap<String, Object>();
						friendMap.put("relationship_id", relId);
						friendMap.put("invite_email", friendId);
						SMInt typeFriendValue = (SMInt)relObject.getValue().get("type_by_receiver");
						Long typeFriend = typeFriendValue.getValue();
						if (typeFriend.longValue() > 2L) {
							friendMap.put("type_by_friend", new Long(2L));
						} else {
							friendMap.put("type_by_friend", typeFriend);
						}
						addedFriends.add(friendMap);
						
						userRelIds.add(relId);
					}
				}
			}
			// - relationships by others
			List<SMObject> relOthersList = new ArrayList<SMObject>();
			if (userObject.getValue().containsKey("relationships_by_others")) {
				SMList<SMObject> relOthersValue = (SMList<SMObject>)userObject.getValue().get("relationships_by_others");
				relOthersList = relOthersValue.getValue();
			}
			for (int i = 0; i < relOthersList.size(); i++) {
				SMObject relObject = relOthersList.get(i);
				SMObject friendObject = (SMObject)relObject.getValue().get("owner");
				SMString friendId = (SMString)friendObject.getValue().get("username");
				// if it's in the requested usernames, change to friend only if it's deleted by user
				if (reqIds.remove(friendId)) {
					SMInt typeUserValue = (SMInt)relObject.getValue().get("type_by_receiver");
					Long typeUser = typeUserValue.getValue();
					if (typeUser.longValue() == 4L) {
						SMString relId = (SMString)relObject.getValue().get("relationship_id");
						List<SMUpdate> relUpdates = new ArrayList<SMUpdate>();
						relUpdates.add(new SMSet("type_by_receiver", new SMInt(2L)));
						dataService.updateObject("relationship", relId, relUpdates);
						
						// return the friend's data according to relationship types
						Map<String, Object> friendMap = new HashMap<String, Object>();
						friendMap.put("relationship_id", relId);
						friendMap.put("username", friendId);
						SMInt typeFriendValue = (SMInt)relObject.getValue().get("type_by_owner");
						Long typeFriend = typeFriendValue.getValue();
						if (typeFriend.longValue() > 2L) {
							friendMap.put("type_by_friend", new Long(2L));
						} else {
							friendMap.put("type_by_friend", typeFriend);
						}
						friendMap.put("name", (SMString)friendObject.getValue().get("name"));
						friendMap.put("profile_image_url", (SMString)friendObject.getValue().get("profile_image_url"));
						if (typeFriend.longValue() == 2L) {
							friendMap.put("action", (SMString)friendObject.getValue().get("action"));
							friendMap.put("place", (SMString)friendObject.getValue().get("place"));
							friendMap.put("status_mod_date", (SMInt)friendObject.getValue().get("status_mod_date"));
						}
						addedFriends.add(friendMap);
						
						othersRelIds.add(relId);
					}
				}
			}
			// if the relationships do not exist, create new ones
			if (reqIds.size() > 0) {
				// fetch friend objects
				// - build query
				List<SMCondition> friendQuery = new ArrayList<SMCondition>();
				friendQuery.add(new SMIn("username", reqIds));
				// - build result filter
				List<String> friendFields = new ArrayList<String>();
				friendFields.add("username");
				friendFields.add("name");
				friendFields.add("profile_image_url");
				ResultFilters friendFilter = new ResultFilters(0, -1, null, friendFields);
				// - execute query
				List<SMObject> friends = dataService.readObjects("user", friendQuery, 0, friendFilter);
				// report error & return partial result if query failed
				if (friends == null || friends.size() > reqIds.size()) {
					returnMap.put("friends", addedFriends);
					returnMap.put("error", reqIds);
					return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
				}
				
				List<SMString> allRelIdList = new ArrayList<SMString>();
				// for each username that exists, create a new relationship
				for (int i = 0; i < friends.size(); i++) {
					SMObject friendObject = friends.get(i);
					SMString friendId = (SMString)friendObject.getValue().get("username");
					
					Map<String, SMValue> relMap = new HashMap<String, SMValue>();
					relMap.put("sm_owner", new SMString("user/" + username));
					relMap.put("type_by_owner", new SMInt(2L));
					relMap.put("type_by_receiver", new SMInt(1L));
					relMap.put("invite_email", new SMString(""));
					SMObject relObject = dataService.createObject("relationship", new SMObject(relMap));
					// get the new relationship id
					SMString relId = (SMString)relObject.getValue().get("relationship_id");
					// store relationship for adding in user's relationships_by_user later
					allRelIdList.add(relId);
					// add user as relationship's owner
					List<SMString> ownerIdList = new ArrayList<SMString>();
					ownerIdList.add(userId);
					dataService.addRelatedObjects("relationship", relId, "owner", ownerIdList);
					// add relationship in friend's relationships_by_others
					List<SMString> relIdList = new ArrayList<SMString>();
					relIdList.add(relId);
					dataService.addRelatedObjects("user", friendId, "relationships_by_others", relIdList);
					// add friend as relationship's receiver
					List<SMString> receiverIdList = new ArrayList<SMString>();
					receiverIdList.add(friendId);
					dataService.addRelatedObjects("relationship", relId, "receiver", receiverIdList);
					
					// and create a friend request event
					Map<String, SMValue> eventMap = new HashMap<String, SMValue>();
					eventMap.put("sm_owner", new SMString("user/" + username));
					eventMap.put("type", new SMInt(1L));
					SMObject eventObject = dataService.createObject("event", new SMObject(eventMap));
					// get the new event id
					SMString eventId = (SMString)eventObject.getValue().get("event_id");
					// add event in relationship's events_by_owner
					List<SMString> eventIdList = new ArrayList<SMString>();
					eventIdList.add(eventId);
					dataService.addRelatedObjects("relationship", relId, "events_by_owner", eventIdList);
					// add relationship as event's relationship_by_owner
					dataService.addRelatedObjects("event", eventId, "relationship_by_owner", relIdList);
					
					// remove from the requested username
					reqIds.remove(friendId);
					
					Map<String, Object> friendMap = new HashMap<String, Object>();
					friendMap.put("relationship_id", relId);
					friendMap.put("username", friendId);
					friendMap.put("type_by_friend", new Long(1L));
					friendMap.put("name", (SMString)friendObject.getValue().get("name"));
					friendMap.put("profile_image_url", (SMString)friendObject.getValue().get("profile_image_url"));
					addedFriends.add(friendMap);
					
					userRelIds.add(relId);
				}
				// for each username that does not exists, create a new invite
				for (int i = 0; i < reqIds.size(); i++) {
					SMString inviteId = reqIds.get(i);
					
					Map<String, SMValue> relMap = new HashMap<String, SMValue>();
					relMap.put("sm_owner", new SMString("user/" + username));
					relMap.put("type_by_owner", new SMInt(2L));
					relMap.put("type_by_receiver", new SMInt(1L));
					relMap.put("invite_email", inviteId);
					SMObject relObject = dataService.createObject("relationship", new SMObject(relMap));
					// get the new relationship id
					SMString relId = (SMString)relObject.getValue().get("relationship_id");
					// store relationship for adding in user's relationships_by_user later
					allRelIdList.add(relId);
					// add user as relationship's owner
					List<SMString> ownerIdList = new ArrayList<SMString>();
					ownerIdList.add(userId);
					dataService.addRelatedObjects("relationship", relId, "owner", ownerIdList);
					
					// and create a friend request event
					Map<String, SMValue> eventMap = new HashMap<String, SMValue>();
					eventMap.put("sm_owner", new SMString("user/" + username));
					eventMap.put("type", new SMInt(1L));
					SMObject eventObject = dataService.createObject("event", new SMObject(eventMap));
					// get the new event id
					SMString eventId = (SMString)eventObject.getValue().get("event_id");
					// add event in relationship's events_by_owner
					List<SMString> eventIdList = new ArrayList<SMString>();
					eventIdList.add(eventId);
					dataService.addRelatedObjects("relationship", relId, "events_by_owner", eventIdList);
					// add relationship as event's relationship_by_owner
					List<SMString> relIdList = new ArrayList<SMString>();
					relIdList.add(relId);
					dataService.addRelatedObjects("event", eventId, "relationship_by_owner", relIdList);
					
					Map<String, Object> friendMap = new HashMap<String, Object>();
					friendMap.put("relationship_id", relId);
					friendMap.put("invite_email", inviteId);
					addedFriends.add(friendMap);
					
					userRelIds.add(relId);
				}
				// add all new relationships in user's relationships_by_user
				dataService.addRelatedObjects("user", userId, "relationships_by_user", allRelIdList);
			}
			returnMap.put("friends", addedFriends);
			
			long currentTime = System.currentTimeMillis();
			// if group id parameter exists, add new relationships to the group			
			if (!groupIdString.isEmpty()) {
				// fetch group object
				// - build query
				List<SMCondition> groupQuery = new ArrayList<SMCondition>();
				groupQuery.add(new SMEquals("group_id", groupId));
				// - build result filter
				List<String> fields = new ArrayList<String>();
				fields.add("relationship_order");
				fields.add("owner");
				fields.add("owner.username");
				ResultFilters filter = new ResultFilters(0, -1, null, fields);
				// - execute query
				List<SMObject> groups = dataService.readObjects("group", groupQuery, 1, filter);
				// report error & return partial result if query failed
				if (groups == null || groups.size() != 1) {
					returnMap.put("error", groupId);
					return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
				}
				
				SMObject groupObject = groups.get(0);
				SMObject ownerObject = (SMObject)groupObject.getValue().get("owner");
				SMString ownerId = (SMString)ownerObject.getValue().get("username");
				// check if user is the owner of this group
				if (ownerId.equals(userId)) {
					List<SMString> groupIdList = new ArrayList<SMString>();
					groupIdList.add(groupId);
					
					// add group as groups_by_owner of each relationship by user
					for (int i = 0; i < userRelIds.size(); i++) {
						SMString relId = userRelIds.get(i);
						dataService.addRelatedObjects("relationship", relId, "groups_by_owner", groupIdList);
					}
					// add relationships by user as relationships_by_owner of the group
					dataService.addRelatedObjects("group", groupId, "relationships_by_owner", userRelIds);
					
					// add group as groups_by_receiver of each relationship by others
					for (int i = 0; i < othersRelIds.size(); i++) {
						SMString relId = othersRelIds.get(i);
						dataService.addRelatedObjects("relationship", relId, "groups_by_receiver", groupIdList);
					}
					// add relationships by others as relationships_by_others of the group
					dataService.addRelatedObjects("group", groupId, "relationships_by_others", othersRelIds);
					
					// update relationship order by appending all relationships
					SMList<SMString> relOrderValue = (SMList<SMString>)groupObject.getValue().get("relationship_order");
					List<SMString> relOrder = relOrderValue.getValue();
					relOrder.addAll(userRelIds);
					relOrder.addAll(othersRelIds);
					List<SMUpdate> groupUpdates = new ArrayList<SMUpdate>();
					groupUpdates.add(new SMSet("relationship_order", new SMList<SMString>(relOrder)));
					dataService.updateObject("group", groupId, groupUpdates);
					
					// change groups mod date
					List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
					userUpdates.add(new SMSet("groups_mod_date", new SMInt(currentTime)));
					dataService.updateObject("user", userId, userUpdates);
					
					returnMap.put("friend_order", relOrder);
				}
			}
			// return data for local database
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
