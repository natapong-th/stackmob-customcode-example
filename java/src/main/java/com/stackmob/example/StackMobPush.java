/**
 * Copyright 2012 StackMob
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

import com.stackmob.sdkapi.http.HttpService;
import com.stackmob.sdkapi.http.request.HttpRequest;
import com.stackmob.sdkapi.http.request.GetRequest;
import com.stackmob.sdkapi.http.response.HttpResponse;
import com.stackmob.core.ServiceNotActivatedException;
import com.stackmob.sdkapi.http.exceptions.AccessDeniedException;
import com.stackmob.sdkapi.http.exceptions.TimeoutException;
import com.stackmob.core.InvalidSchemaException;
import com.stackmob.core.DatastoreException;
import com.stackmob.sdkapi.PushService;
import com.stackmob.sdkapi.PushService.TokenAndType;
import com.stackmob.sdkapi.PushService.TokenType;
import com.stackmob.core.PushServiceException;
import com.stackmob.core.ServiceNotActivatedException;

import java.net.MalformedURLException;
import com.stackmob.sdkapi.http.request.PostRequest;
import com.stackmob.sdkapi.http.Header;
import com.stackmob.sdkapi.LoggerService;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;

// Added JSON parsing to handle JSON posted in the body
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.URLEncoder;
import java.io.UnsupportedEncodingException;

public class StackMobPush implements CustomCodeMethod {

  //Create your SendGrid Acct at sendgrid.com
  static String API_USER = "YOUR_SENDGRID_USERNAME";
  static String API_KEY = "YOUR_SENDGRID_PASSWORD";

  @Override
  public String getMethodName() {
    return "register_token";
  }
    
    
  @Override
  public List<String> getParams() {
    return Arrays.asList("device_token");
  }  
    

  @Override
  public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
    int responseCode = 0;
    String responseBody = "";
    
    LoggerService logger = serviceProvider.getLoggerService(StackMobPush.class);
    //Log the JSON object passed to the StackMob Logs
    logger.debug("Start PUSH code");
    
    // TO phonenumber should be YOUR cel phone
    String deviceToken = request.getParams().get("device_token");
    
    if (deviceToken == null || deviceToken.isEmpty()) {
      HashMap<String, String> errParams = new HashMap<String, String>();
      errParams.put("error", "the device token passed was empty or null");
      return new ResponseToProcess(HttpURLConnection.HTTP_BAD_REQUEST, errParams); // http 400 - bad request
    }
    	
    String username = "sidney";
    TokenAndType token = new TokenAndType(deviceToken, TokenType.iOS);
    
    try {
	    PushService service = serviceProvider.getPushService();
	    
	    try {
	    	service.registerTokenForUser(username, token);
	        responseCode = HttpURLConnection.HTTP_OK;
	        responseBody = "token saved";
	    	
	    } catch (Exception e) {
	        //handle exception
	    	logger.error("error registering token " + e.toString());
	    	responseCode = HttpURLConnection.HTTP_BAD_REQUEST;
	    	responseBody = e.toString();	
	    } 
    
    } catch (ServiceNotActivatedException e) {    	
        //handle exception
    	logger.error("error service not active" + e.toString());
    	responseCode = HttpURLConnection.HTTP_BAD_REQUEST;
    	responseBody = e.toString();	
        
    }
    
    logger.debug("End PUSH code");
    
    
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("response_body", responseBody);

    return new ResponseToProcess(responseCode, map);
  }
}
