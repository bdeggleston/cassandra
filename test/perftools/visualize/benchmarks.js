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

let highlighted_rows = {};
let highlighted_columns = {};

const valueData = ${VALUE_DATA};
const paramData = ${PARAM_DATA};
const paramArgs = ${PARAM_ARGS};

let freezeXY = false;

function mapAdd(map, key, incr) {
    var val = map[key];
    if (val === undefined)
        val = 0;

    val = val + incr;
    if (val < 0)
        val = 0;
    map[key] = val;
    return val;
}

function mapClear(map) {
    for (const key in map)
        map[key] = 0;
}

function dataClick()
{
    if (freezeXY)
    {
        debugger;
        var setOpacity = function (e) { e.style.opacity = "0"; }
        document.querySelectorAll('.data-highlight').forEach(setOpacity);
        mapClear(highlighted_rows);
        mapClear(highlighted_columns);
        freezeXY = false;
    }
    else
    {
        freezeXY = true;
    }
}

function dataKey(hmapId, v) {
    return hmapId + ' ' + v;
}

function dataMouseover(hmapId, x, y) {
    if (freezeXY)
        return;
    mapAdd(highlighted_columns, dataKey(hmapId + x), 1);
    mapAdd(highlighted_rows, dataKey(hmapId + y), 1);
    var setOpacity = function (e) { e.style.opacity = "0.35"; };
    document.querySelectorAll('.col' + x).forEach(setOpacity);
    document.querySelectorAll('.row' + y).forEach(setOpacity);
    hmapShowOutput(x, y);
}

function dataMouseout(hmapId, x, y) {
    if (freezeXY)
        return;
    var setOpacity = function (e) { e.style.opacity = "0"; }
    if (mapAdd(highlighted_columns, dataKey(hmapId + x), -1) === 0)
        document.querySelectorAll('.col' + x).forEach(setOpacity);
    if (mapAdd(highlighted_rows, dataKey(hmapId + y), -1) === 0)
        document.querySelectorAll('.row' + y).forEach(setOpacity);
}

function hmapSetText(eid, text) {

}

function hmapShowOutput(x, y) {
    var params = paramData[y][x]
    document.querySelector("#x-out").textContent = "x[" + x + "] -> " + params['x_out'];
    document.querySelector("#y-out").textContent = "y[" + y + "] -> " + params['y_out'];
    document.querySelector("#params-out").textContent = "[jmh args] -> " + paramArgs[y][x];
    for (var key in valueData) {
        var output = valueData[key][y][x]
        document.querySelector("#" + key + "-data-out").textContent = output;
    }
}

function hmapMouseout() {
    document.querySelector("#x-out").textContent = "";
    document.querySelector("#y-out").textContent = "";
    for (var key in valueData) {
        document.querySelector("#" + key + "-data-out").textContent = "";
    }
}