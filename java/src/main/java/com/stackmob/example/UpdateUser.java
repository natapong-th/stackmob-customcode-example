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

public class UpdateUser implements CustomCodeMethod {

	@Override
	public String getMethodName() {
		return "update_user";
	}
	
	@Override
	public List<String> getParams() {
		return Arrays.asList("name", "profile_image_url", "group_order", "action", "place");
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
		if (username == null || username.isEmpty()) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no user is logged in");
			return new ResponseToProcess(HttpURLConnection.HTTP_UNAUTHORIZED, errParams); // http 401 - unauthorized
		}
		SMString userId = new SMString(username);
		
		// get update parameters
		String name = "";
		boolean newName = false;
		String profileImage = "";
		boolean newImage = false;
		List<SMString> groupOrder = new ArrayList<SMString>();
		boolean newOrder = false;
		String action = "";
		boolean newAction = false;
		String place = "";
		boolean newPlace = false;
		if (!request.getBody().isEmpty()) {
			try {
				JSONObject jsonObj = new JSONObject(request.getBody());
				// do not allow empty name
				if (!jsonObj.isNull("name")) {
					name = jsonObj.getString("name");
					newName = !name.isEmpty();
				}
				if (!jsonObj.isNull("profile_image_url")) {
					profileImage = jsonObj.getString("profile_image_url");
					newImage = true;
				}
				if (!jsonObj.isNull("group_order")) {
					JSONArray groupArray = jsonObj.getJSONArray("group_order");
					for (int i = 0; i < groupArray.length(); i++) {
						String groupId = groupArray.getString(i);
						groupOrder.add(new SMString(groupId));
					}
					newOrder = true;
				}
				if (!jsonObj.isNull("action")) {
					action = jsonObj.getString("action");
					newAction = true;
				}
				if (!jsonObj.isNull("place")) {
					place = jsonObj.getString("place");
					newPlace = true;
				}
			} catch (JSONException e) {
				HashMap<String, String> errParams = new HashMap<String, String>();
				errParams.put("error", "invalid request body");
				return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
			}
		}
		if (!newName && !newImage && !newOrder && !newAction && !newPlace) {
			HashMap<String, String> errParams = new HashMap<String, String>();
			errParams.put("error", "no parameters to update");
			return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
		}
		
		// get the datastore service
		DataService dataService = serviceProvider.getDataService();
		
		// create a response
		try {
			// fetch user object
			// - build query
			List<SMCondition> userQuery = new ArrayList<SMCondition>();
			userQuery.add(new SMEquals("username", userId));
			// - build result filter
			List<String> fields = new ArrayList<String>();
			if (newName) fields.add("name");
			if (newImage) fields.add("profile_image_url");
			if (newOrder) {
				fields.add("groups");
				fields.add("group_order");
			}
			if (newAction) fields.add("action");
			if (newPlace) fields.add("place");
			if (newAction || newPlace) {
				fields.add("relationships_by_user");
				fields.add("relationships_by_user.relationship_id");
				fields.add("relationships_by_user.events_by_receiver");
				fields.add("relationships_by_user.events_by_receiver.event_id");
				fields.add("relationships_by_user.events_by_receiver.type");
				fields.add("relationships_by_others");
				fields.add("relationships_by_others.relationship_id");
				fields.add("relationships_by_others.events_by_owner");
				fields.add("relationships_by_others.events_by_owner.event_id");
				fields.add("relationships_by_others.events_by_owner.type");
			}
			ResultFilters filter = new ResultFilters(0, -1, null, fields);
			// - execute query
			List<SMObject> users = dataService.readObjects("user", userQuery, 2, filter);
			if (users != null && users.size() == 1) {
				SMObject userObject = users.get(0);
				Map<String, Object> returnMap = new HashMap<String, Object>();
				List<SMUpdate> userUpdates = new ArrayList<SMUpdate>();
				// check if name, profile image, or group order are different
				boolean userChanged = false;
				// 1. change name
				if (newName) {
					SMString oldName = (SMString)userObject.getValue().get("name");
					if (!oldName.equals(name)) {
						userUpdates.add(new SMSet("name", new SMString(name)));
						returnMap.put("name", name);
						userChanged = true;
					}
				}
				// 2. change profile image
				if (newImage) {
					SMString oldImage = (SMString)userObject.getValue().get("profile_image_url");
					if (!oldImage.equals(profileImage)) {
						userUpdates.add(new SMSet("profile_image_url", new SMString(profileImage)));
						returnMap.put("profile_image_url", profileImage);
						userChanged = true;
					}
				}
				// 3. change group order (only if group order is valid)
				if (newOrder) {
					List<SMString> groupList = new ArrayList<SMString>();
					if (userObject.getValue().containsKey("groups")) {
						SMList<SMString> groupListValue = (SMList<SMString>)userObject.getValue().get("groups");
						groupList = groupListValue.getValue();
					}
					boolean validOrder = (groupOrder.size() == groupList.size());
					if (validOrder) {
						for (int i = 0; i < groupList.size(); i++) {
							SMString groupId = groupList.get(i);
							boolean found = false;
							for (int j = 0; j < groupOrder.size(); j++) {
								if (groupOrder.get(j).equals(groupId)) {
									found = true;
									break;
								}
							}
							if (!found) {
								validOrder = false;
								break;
							}
						}
					}
					if (validOrder) {
						List<SMString> oldGroupOrder = ((SMList<SMString>)userObject.getValue().get("group_order")).getValue();
						boolean orderChanged = oldGroupOrder.size() != groupOrder.size();
						if (!orderChanged) {
							for (int i = 0; i < groupOrder.size(); i++) {
								if (!groupOrder.get(i).equals(oldGroupOrder.get(i))) {
									orderChanged = true;
									break;
								}
							}
						}
						if (orderChanged) {
							userUpdates.add(new SMSet("group_order", new SMList<SMString>(groupOrder)));
							returnMap.put("group_order", groupOrder);
							userChanged = true;
						}
					}
				}
				// 4. update user mod date if user is changed
				if (userChanged) {
					long currentTime = System.currentTimeMillis();
					userUpdates.add(new SMSet("user_mod_date", new SMInt(currentTime)));
					returnMap.put("user_mod_date", new Long(currentTime));
				}
				// check if action or place are different
				boolean statusChanged = false;
				// 5. change action
				if (newAction) {
					SMString oldAction = (SMString)userObject.getValue().get("action");
					if (!oldAction.getValue().equals(action)) {
						boolean fromJoining = oldAction.getValue().startsWith("joining:");
						boolean toJoining = action.startsWith("joining:");
						boolean toJoined = action.startsWith("joined:");
						// if change from joining, remove the joining events
						if (fromJoining) {
							SMString relId = new SMString(oldAction.getValue().substring(8)); // length of "joining:"
							// fetch relationship object
							// - build query
							List<SMCondition> relQuery = new ArrayList<SMCondition>();
							relQuery.add(new SMEquals("relationship_id", relId));
							// - build result filter
							List<String> relFields = new ArrayList<String>();
							relFields.add("type_by_owner");
							relFields.add("type_by_receiver");
							relFields.add("owner");
							relFields.add("owner.username");
							relFields.add("receiver");
							relFields.add("receiver.username");
							relFields.add("events_by_owner");
							relFields.add("events_by_owner.event_id");
							relFields.add("events_by_owner.type");
							relFields.add("events_by_receiver");
							relFields.add("events_by_receiver.event_id");
							relFields.add("events_by_receiver.type");
							ResultFilters relFilter = new ResultFilters(0, -1, null, relFields);
							// - execute query
							List<SMObject> rels = dataService.readObjects("relationship", relQuery, 1, relFilter);
							if (rels != null && rels.size() == 1) {
								SMObject relObject = rels.get(0);
								SMInt typeOwner = (SMInt)relObject.getValue().get("type_by_owner");
								SMInt typeReceiver = (SMInt)relObject.getValue().get("type_by_receiver");
								SMObject ownerObject = (SMObject)relObject.getValue().get("owner");
								SMString ownerId = (SMString)ownerObject.getValue().get("username");
								SMObject receiverObject = (SMObject)relObject.getValue().get("receiver");
								SMString receiverId = (SMString)receiverObject.getValue().get("username");
								String userRole = "";
								if (ownerId.equals(userId)) {
									userRole = "owner";
								} else if (receiverId.equals(userId)) {
									userRole = "receiver";
								}
								if (!userRole.isEmpty()) {
									String eventKey = "events_by_" + userRole;
									List<SMObject> eventsList = new ArrayList<SMObject>();
									if (relObject.getValue().containsKey(eventKey)) {
										SMList<SMObject> eventsValue = (SMList<SMObject>)relObject.getValue().get(eventKey);
										eventsList = eventsValue.getValue();
									}
									List<SMString> joinEventIdList = new ArrayList<SMString>();
									for (int i = 0; i < eventsList.size(); i++) {
										SMObject eventObject = eventsList.get(i);
										SMInt eventType = (SMInt)eventObject.getValue().get("type");
										if (eventType.getValue().longValue() == 4L) {
											joinEventIdList.add((SMString)eventObject.getValue().get("event_id"));
										}
									}
									if (joinEventIdList.size() > 0) {
										dataService.removeRelatedObjects("relationship", relId, eventKey, joinEventIdList, true);
									}
									// if not change to joined the same friend, add a cancel event (mutual friend only)
									if (!(toJoined && relId.getValue().equals(action.substring(7))) && typeOwner.getValue().longValue() == 2L && typeOwner.getValue().longValue() == 2L) {
										Map<String, SMValue> eventMap = new HashMap<String, SMValue>();
										eventMap.put("sm_owner", new SMString("user/" + username));
										eventMap.put("type", new SMInt(5L));
										SMObject eventObject = dataService.createObject("event", new SMObject(eventMap));
										// get the new event id
										SMString eventId = (SMString)eventObject.getValue().get("event_id");
										// add event in relationship's events_by_owner
										List<SMString> cancelEventIdList = new ArrayList<SMString>();
										cancelEventIdList.add(eventId);
										dataService.addRelatedObjects("relationship", relId, eventKey, cancelEventIdList);
										// add relationship as event's relationship
										List<SMString> relIdList = new ArrayList<SMString>();
										relIdList.add(relId);
										dataService.addRelatedObjects("event", eventId, "relationship_by_" + userRole, relIdList);
									}
								}
							}
						}
						boolean validAction = true;
						boolean fixPlace = false;
						// if change to joining, create a joining event to the freind
						if (toJoining) {
							SMString relId = new SMString(action.substring(8)); // length of "joining:"
							// fetch relationship object
							// - build query
							List<SMCondition> relQuery = new ArrayList<SMCondition>();
							relQuery.add(new SMEquals("relationship_id", relId));
							// - build result filter
							List<String> relFields = new ArrayList<String>();
							relFields.add("type_by_owner");
							relFields.add("type_by_receiver");
							relFields.add("owner");
							relFields.add("owner.username");
							relFields.add("receiver");
							relFields.add("receiver.username");
							ResultFilters relFilter = new ResultFilters(0, -1, null, relFields);
							// - execute query
							List<SMObject> rels = dataService.readObjects("relationship", relQuery, 1, relFilter);
							if (rels != null && rels.size() == 1) {
								SMObject relObject = rels.get(0);
								// only allow if user is in this relationship and is mutual friend
								SMInt typeOwner = (SMInt)relObject.getValue().get("type_by_owner");
								SMInt typeReceiver = (SMInt)relObject.getValue().get("type_by_receiver");
								SMObject ownerObject = (SMObject)relObject.getValue().get("owner");
								SMString ownerId = (SMString)ownerObject.getValue().get("username");
								SMObject receiverObject = (SMObject)relObject.getValue().get("receiver");
								SMString receiverId = (SMString)receiverObject.getValue().get("username");
								String userRole = "";
								if (ownerId.equals(userId)) {
									userRole = "owner";
								} else if (receiverId.equals(userId)) {
									userRole = "receiver";
								}
								if (!userRole.isEmpty() && typeOwner.getValue().longValue() == 2L && typeOwner.getValue().longValue() == 2L) {
									Map<String, SMValue> eventMap = new HashMap<String, SMValue>();
									eventMap.put("sm_owner", new SMString("user/" + username));
									eventMap.put("type", new SMInt(4L));
									SMObject eventObject = dataService.createObject("event", new SMObject(eventMap));
									// get the new event id
									SMString eventId = (SMString)eventObject.getValue().get("event_id");
									// add event in relationship's events_by_owner
									List<SMString> joinEventIdList = new ArrayList<SMString>();
									joinEventIdList.add(eventId);
									dataService.addRelatedObjects("relationship", relId, "events_by_" + userRole, joinEventIdList);
									// add relationship as event's relationship
									List<SMString> relIdList = new ArrayList<SMString>();
									relIdList.add(relId);
									dataService.addRelatedObjects("event", eventId, "relationship_by_" + userRole, relIdList);
									
									// must be no place 
									place = "";
									fixPlace = true;
								} else {
									validAction = false;
								}
							} else {
								validAction = false;
							}
						// if change to joined, copy action & place from the friend
						} else if (toJoined) {
							SMString relId = new SMString(action.substring(8)); // length of "joining:"
							// fetch relationship object
							// - build query
							List<SMCondition> relQuery = new ArrayList<SMCondition>();
							relQuery.add(new SMEquals("relationship_id", relId));
							// - build result filter
							List<String> relFields = new ArrayList<String>();
							relFields.add("type_by_owner");
							relFields.add("type_by_receiver");
							relFields.add("owner");
							relFields.add("owner.username");
							relFields.add("owner.action");
							relFields.add("owner.place");
							relFields.add("receiver");
							relFields.add("receiver.username");
							relFields.add("receiver.action");
							relFields.add("receiver.place");
							ResultFilters relFilter = new ResultFilters(0, -1, null, relFields);
							// - execute query
							List<SMObject> rels = dataService.readObjects("relationship", relQuery, 1, relFilter);
							if (rels != null && rels.size() == 1) {
								SMObject relObject = rels.get(0);
								// only allow if user is in this relationship and is mutual friend
								SMInt typeOwner = (SMInt)relObject.getValue().get("type_by_owner");
								SMInt typeReceiver = (SMInt)relObject.getValue().get("type_by_receiver");
								SMObject ownerObject = (SMObject)relObject.getValue().get("owner");
								SMString ownerId = (SMString)ownerObject.getValue().get("username");
								SMObject receiverObject = (SMObject)relObject.getValue().get("receiver");
								SMString receiverId = (SMString)receiverObject.getValue().get("username");
								String userRole = "";
								SMString joinedAction = new SMString("");
								SMString joinedPlace = new SMString("");
								if (ownerId.equals(userId)) {
									userRole = "owner";
									joinedAction = (SMString)receiverObject.getValue().get("action");
									joinedPlace = (SMString)receiverObject.getValue().get("place");
								} else if (receiverId.equals(userId)) {
									userRole = "receiver";
									joinedAction = (SMString)ownerObject.getValue().get("action");
									joinedPlace = (SMString)ownerObject.getValue().get("place");
								}
								if (!userRole.isEmpty() && typeOwner.getValue().longValue() == 2L && typeOwner.getValue().longValue() == 2L) {
									action = joinedAction.getValue();
									place = joinedPlace.getValue();
									fixPlace = true;
								} else {
									validAction = false;
								}
							} else {
								validAction = false;
							}
						}
						// change action only if it is valid
						if (validAction) {
							userUpdates.add(new SMSet("action", new SMString(action)));
							returnMap.put("action", action);
							statusChanged = true;
							// change place if it's action-dependent
							if (fixPlace) {
								userUpdates.add(new SMSet("place", new SMString(place)));
								returnMap.put("place", place);
								newPlace = false; // ignore custom place change
							}
						}
					}
				}
				// 6. change place
				if (newPlace) {
					SMString oldPlace = (SMString)userObject.getValue().get("place");
					if (!oldPlace.getValue().equals(place)) {
						userUpdates.add(new SMSet("place", new SMString(place)));
						returnMap.put("place", place);
						statusChanged = true;
					}
				}
				// 7. change status mod date if status is changed
				if (statusChanged) {
					long currentTime = System.currentTimeMillis();
					userUpdates.add(new SMSet("status_mod_date", new SMInt(currentTime)));
					returnMap.put("status_mod_date", new Long(currentTime));
					// remove all status update request events
					List<SMString> removedEventList = new ArrayList<SMString>();
					// - relationships by user
					List<SMObject> relList = new ArrayList<SMObject>();
					if (userObject.getValue().containsKey("relationships_by_user")) {
						SMList<SMObject> relListValue = (SMList<SMObject>)userObject.getValue().get("relationships_by_user");
						relList = relListValue.getValue();
					}
					for (int i = 0; i < relList.size(); i++) {
						SMObject relObject = relList.get(i);
						List<SMObject> eventList = new ArrayList<SMObject>();
						if (relObject.getValue().containsKey("events_by_receiver")) {
							SMList<SMObject> eventListValue = (SMList<SMObject>)relObject.getValue().get("events_by_receiver");
							eventList = eventListValue.getValue();
						}
						List<SMString> statReqList = new ArrayList<SMString>();
						for (int j = 0; j < eventList.size(); j++) {
							SMObject eventObject = eventList.get(i);
							SMInt eventType = (SMInt)eventObject.getValue().get("type");
							if (eventType.getValue().longValue() == 3L) {
								SMString eventId = (SMString)eventObject.getValue().get("event_id");
								statReqList.add(eventId);
								removedEventList.add(eventId);
							}
						}
						SMString relId = (SMString)relObject.getValue().get("relationship_id");
						dataService.removeRelatedObjects("relationship", relId, "events_by_receiver", statReqList, true);
					}
					// - relationships by others
					relList = new ArrayList<SMObject>();
					if (userObject.getValue().containsKey("relationships_by_others")) {
						SMList<SMObject> relListValue = (SMList<SMObject>)userObject.getValue().get("relationships_by_others");
						relList = relListValue.getValue();
					}
					for (int i = 0; i < relList.size(); i++) {
						SMObject relObject = relList.get(i);
						List<SMObject> eventList = new ArrayList<SMObject>();
						if (relObject.getValue().containsKey("events_by_owner")) {
							SMList<SMObject> eventListValue = (SMList<SMObject>)relObject.getValue().get("events_by_owner");
							eventList = eventListValue.getValue();
						}
						List<SMString> statReqList = new ArrayList<SMString>();
						for (int j = 0; j < eventList.size(); j++) {
							SMObject eventObject = eventList.get(i);
							SMInt eventType = (SMInt)eventObject.getValue().get("type");
							if (eventType.getValue().longValue() == 3L) {
								SMString eventId = (SMString)eventObject.getValue().get("event_id");
								statReqList.add(eventId);
								removedEventList.add(eventId);
							}
						}
						SMString relId = (SMString)relObject.getValue().get("relationship_id");
						dataService.removeRelatedObjects("relationship", relId, "events_by_owner", statReqList, true);
					}
					returnMap.put("removed_events", removedEventList);
				}
				// update user (only if there is one)
				if (userUpdates.size() > 0) {
					dataService.updateObject("user", userId, userUpdates);
				}
				// return updated data for local database
				return new ResponseToProcess(HttpURLConnection.HTTP_OK, returnMap);
			} else {
				// TO DO:
				// handle user fetch error
				
				HashMap<String, String> errMap = new HashMap<String, String>();
				errMap.put("error", "invalid user fetch");
				errMap.put("detail", (users == null ? "null fetch result" : ("fetch result count = " + users.size())));
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
