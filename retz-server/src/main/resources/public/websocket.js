/*
 *    Retz
 *    Copyright (C) 2016 Nautilus Technologies, Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
//Establish the WebSocket connection and set up event handlers
var webSocket = new WebSocket("ws://" + location.hostname + ":" + location.port + "/cui");
  setup();

function setup() {
webSocket.onmessage = function (msg) { updateConsole(msg); };
webSocket.onopen = function () {
  console.log("connected");
  sendMessage("watch");
  append("console", "connected<br/>");
}
webSocket.onclose = function () {
 //alert("WebSocket connection closed")
   append("console", "connection closed<br/>");
  };
  }

id("reconnect").addEventListener("click", function () {
    console.log("reconnect");
    webSocket.close();
    refreshMessage();
    append("console", "reconnecting<br/>");
    webSocket = new WebSocket("ws://" + location.hostname + ":" + location.port + "/cui");
    setup(webSocket);
});



//Send a message if it's not empty, then clear the input field
function sendMessage(message) {
    if (message !== "") {
        webSocket.send(JSON.stringify({"command": message}));
    } else {
        //refreshMessage();
    }
}

function updateConsole(msg) {
    var data = JSON.parse(msg.data);
    append("console", JSON.stringify(data) + "<br/>");
    // id("console").scrollTop = id("console").scrollHeight;
}

//Helper function for inserting HTML as the first child of an element
function insert(targetId, message) {
    id(targetId).insertAdjacentHTML("afterbegin", message);
}

function append(targetId, message) {
    id(targetId).insertAdjacentHTML("beforeEnd", message);
        id("console").scrollTop = id("console").scrollHeight;

}

//Helper function for selecting element by id
function id(id) {
    return document.getElementById(id);
}

function refreshMessage(){
    id("console").value = "";
    //append("console", "<br/>\n&gt; ");
}
