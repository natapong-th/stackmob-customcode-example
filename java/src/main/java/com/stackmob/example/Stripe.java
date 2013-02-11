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

import com.stackmob.sdkapi.http.HttpService;
import com.stackmob.sdkapi.http.request.HttpRequest;
import com.stackmob.sdkapi.http.request.GetRequest;
import com.stackmob.sdkapi.http.response.HttpResponse;
import com.stackmob.core.ServiceNotActivatedException;
import com.stackmob.sdkapi.http.exceptions.AccessDeniedException;
import com.stackmob.sdkapi.http.exceptions.TimeoutException;
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
import org.apache.commons.codec.binary.Base64;

public class Stripe implements CustomCodeMethod {

  //Create your Stripe Acct at stripe.com and enter 
  //Your secret api key below.
  public static final String secretKey = "sk_test_zK96ItRMZZiKPs31WKaCD6ah";

  @Override
  public String getMethodName() {
    return "stripe";
  }

  @Override
  public List<String> getParams() {
    return Arrays.asList("amount","token","description");
  }  

  @Override
  public ResponseToProcess execute(ProcessedAPIRequest request, SDKServiceProvider serviceProvider) {
    int responseCode = 0;
    String responseBody = "";

    LoggerService logger = serviceProvider.getLoggerService(Stripe.class);
      
    // AMOUNT should be a whole integer - ie 100 is 1 US dollar
    String amount = request.getParams().get("amount");
      
    //  CURRENCY - should be one of the supported currencies, please check STRIPE documentation.
    String currency = "usd";
      
    //  TOKEN - is returned when you submit the credit card information to stripe using
    // the Stripe JavaScript library.
    String token = request.getParams().get("token");
      
    // DESCRIPTION - the description of the transaction
    String description = request.getParams().get("description");
      
    if (token.isEmpty() || token.trim().isEmpty()) {
      logger.error("Token is missing");
    }
      
    if (amount.isEmpty() || amount.trim().isEmpty()) {
      logger.error("Amount is missing");
    }

    StringBuilder body = new StringBuilder();

    body.append("amount=");
    body.append(amount);
    body.append("&currency=");
    body.append(currency);
    body.append("&card=");
    body.append(token);
    body.append("&description=");
    body.append(description);

    //String body = "amount=" + amount + "&currency=" + currency + "&card=" + token + "&description=" + description;
    String url = "https://api.stripe.com/v1/charges";
    String pair = secretKey;
      
    //Base 64 Encode the secretKey
    String encodedString = "";
    try {
      byte[] b =Base64.encodeBase64(pair.getBytes("utf-8"));
      encodedString = new String(b);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      responseCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
      responseBody = e.getMessage();
    }

    Header accept = new Header("Accept-Charset", "utf-8");
    Header auth = new Header("Authorization","Basic " + encodedString );
    Header content = new Header("Content-Type", "application/x-www-form-urlencoded");

    Set<Header> set = new HashSet();
    set.add(accept);
    set.add(content);
    set.add(auth);
      
    try {
      HttpService http = serviceProvider.getHttpService();
      PostRequest req = new PostRequest(url,set,body.toString());
             
      HttpResponse resp = http.post(req);
      responseCode = resp.getCode();
      responseBody = resp.getBody();
    } catch(TimeoutException e) {
      logger.error(e.getMessage(), e);
      responseCode = HttpURLConnection.HTTP_BAD_GATEWAY;
      responseBody = e.getMessage();
    } catch(AccessDeniedException e) {
      logger.error(e.getMessage(), e);
      responseCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
      responseBody = e.getMessage();
    } catch(MalformedURLException e) {
      logger.error(e.getMessage(), e);
      responseCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
      responseBody = e.getMessage();
    } catch(ServiceNotActivatedException e) {
      logger.error(e.getMessage(), e);
      responseCode = HttpURLConnection.HTTP_INTERNAL_ERROR;
      responseBody = e.getMessage();
    }
      
    Map<String, Object> map = new HashMap<String, Object>();
    map.put("response_body", responseBody);
     
    return new ResponseToProcess(responseCode, map);
  }
}
