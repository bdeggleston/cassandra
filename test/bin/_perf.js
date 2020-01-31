/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var highlighted_rows = {};
var highlighted_columns = {};

var valueData = {}
var paramData = []

function mapAdd(map, key, incr) {
    var val = map[key];
    if (val === undefined)
        val = 0;

    val = val + incr;
    map[key] = val;
    return val;
}

function dataKey(hmapId, v) {
    return hmapId + ' ' + v;
}

function dataMouseover(hmapId, x, y) {
    mapAdd(highlighted_columns, dataKey(hmapId + x), 1);
    mapAdd(highlighted_rows, dataKey(hmapId + y), 1);
    var setOpacity = function (e) { e.style.opacity = "0.35"; };
    document.querySelectorAll('.col' + x).forEach(setOpacity);
    document.querySelectorAll('.row' + y).forEach(setOpacity);
    hmapShowOutput(x, y);
}

function dataMouseout(hmapId, x, y) {
    var setOpacity = function (e) { e.style.opacity = "0"; }
    if (mapAdd(highlighted_columns, dataKey(hmapId + x), -1) === 0)
        document.querySelectorAll('.col' + x).forEach(setOpacity);
    if (mapAdd(highlighted_rows, dataKey(hmapId + y), -1) === 0)
        document.querySelectorAll('.row' + y).forEach(setOpacity);
}

function hmapSetText(eid, text) {

}

function hmapShowOutput(x, y) {
    // debugger;
    for (var key in valueData) {
        var params = paramData[y][x]
        var output = valueData[key][y][x]
        document.querySelector("#" + key + "-xy-out").textContent = "x[" + x + "] y[" + y + "]";
        document.querySelector("#" + key + "-data-out").textContent = output;
        document.querySelector("#" + key + "-x-out").textContent = params['x_out'];
        document.querySelector("#" + key + "-y-out").textContent = params['y_out'];
    }
}

function hmapMouseout() {
    for (var key in valueData) {
        document.querySelector("#" + key + "-xy-out").textContent = "";
        document.querySelector("#" + key + "-data-out").textContent = "";
        document.querySelector("#" + key + "-x-out").textContent = "";
        document.querySelector("#" + key + "-y-out").textContent = "";
    }
}