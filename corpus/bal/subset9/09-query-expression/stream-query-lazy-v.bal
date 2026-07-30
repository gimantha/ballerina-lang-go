// Copyright (c) 2026, WSO2 LLC. (http://www.wso2.com).
//
// WSO2 LLC. licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

import ballerina/io;

int projections = 0;
int innerCollections = 0;
int joinCollections = 0;
int leftJoinKeys = 0;
int rightJoinKeys = 0;
int groupKeys = 0;
int[] limitEvents = [];

function project(int value) returns int {
    projections += 1;
    return value * 10;
}

function inner(int value) returns int[] {
    innerCollections += 1;
    return [value, value + 10];
}

function joinedValues() returns int[] {
    joinCollections += 1;
    return [10, 20, 30];
}

function leftJoinKey(int value) returns int {
    leftJoinKeys += 1;
    return value;
}

function rightJoinKey(int value) returns int {
    rightJoinKeys += 1;
    return value / 10;
}

function groupKey(int value) returns boolean {
    groupKeys += 1;
    return value % 2 == 0;
}

function limitedValues() returns int[] {
    limitEvents.push(2);
    return [1, 2];
}

function queryLimit() returns int {
    limitEvents.push(1);
    return 1;
}

function checkedValue(int value) returns int|error {
    if value == 0 {
        return error("checked completion");
    }
    return value;
}

public function main() {
    stream<int> projected = stream from int value in [1, 2, 3]
        where value > 1
        select project(value);
    io:println(projections); // @output 0
    io:println(projected.next()); // @output {"value":20}
    io:println(projections); // @output 1
    io:println(projected.next()); // @output {"value":30}
    io:println(projections); // @output 2
    io:println(projected.next()); // @output

    stream<int> nested = stream from int value in [1, 2, 3]
        from int nestedValue in inner(value)
        let int total = value + nestedValue
        where total > 3
        limit 2
        select total;
    io:println(innerCollections); // @output 0
    io:println(nested.next()); // @output {"value":12}
    io:println(innerCollections); // @output 1
    io:println(nested.next()); // @output {"value":4}
    io:println(innerCollections); // @output 2
    io:println(nested.next()); // @output
    io:println(innerCollections); // @output 2

    stream<int> joined = stream from int left in [2, 1, 4]
        join int right in joinedValues()
        on leftJoinKey(left) equals rightJoinKey(right)
        order by right descending
        select left + right;
    io:println(joinCollections); // @output 0
    io:println(joined.next()); // @output {"value":22}
    io:println(joinCollections); // @output 1
    io:println(leftJoinKeys); // @output 3
    io:println(rightJoinKeys); // @output 3
    io:println(joined.next()); // @output {"value":11}
    io:println(joined.next()); // @output

    stream<boolean> outerJoined = stream from int left in [1, 2]
        outer join var right in [1]
        on left equals right
        select right is ();
    io:println(outerJoined.next()); // @output {"value":false}
    io:println(outerJoined.next()); // @output {"value":true}
    io:println(outerJoined.next()); // @output

    stream<int[]> grouped = stream from int value in [1, 2, 3, 4]
        group by var _ = groupKey(value)
        select [value];
    io:println(groupKeys); // @output 0
    io:println(grouped.next()); // @output {"value":[1,3]}
    io:println(groupKeys); // @output 4
    io:println(grouped.next()); // @output {"value":[2,4]}
    io:println(grouped.next()); // @output

    stream<int, error?> checked = stream from int value in [0, 1]
        select check checkedValue(value);
    io:println(checked.next()); // @output error("checked completion")
    io:println(checked.next()); // @output {"value":1}
    io:println(checked.next()); // @output

    stream<int> limited = stream from int value in limitedValues()
        limit queryLimit()
        select value;
    io:println(limitEvents); // @output []
    io:println(limited.next()); // @output {"value":1}
    io:println(limitEvents); // @output [1,2]
    io:println(limited.next()); // @output
}
